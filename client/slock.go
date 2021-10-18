package client

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"net"
	"sync"
	"time"
)

type Client struct {
	glock             *sync.Mutex
	replset           *ReplsetClient
	protocol          ClientProtocol
	dbs               []*Database
	requests          map[[16]byte]chan protocol.ICommand
	requestLock       *sync.Mutex
	subscribes        map[uint32]*Subscriber
	subscribeLock     *sync.Mutex
	serverAddress     string
	clientId          [16]byte
	closed            bool
	closedWaiter      chan bool
	reconnectWaiter   chan bool
	unavailableWaiter chan bool
}

func NewClient(host string, port uint) *Client {
	address := fmt.Sprintf("%s:%d", host, port)
	client := &Client{&sync.Mutex{}, nil, nil, make([]*Database, 256),
		make(map[[16]byte]chan protocol.ICommand, 4096), &sync.Mutex{}, make(map[uint32]*Subscriber, 4),
		&sync.Mutex{}, address, protocol.GenClientId(),
		false, make(chan bool, 1), nil, nil}
	return client
}

func (self *Client) Open() error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	err := self.connect(self.serverAddress, self.clientId)
	if err != nil {
		return err
	}
	go self.process()
	return nil
}

func (self *Client) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}
	self.glock.Unlock()

	subscriberClosedWaiters := make([]chan bool, 0)
	self.subscribeLock.Lock()
	for _, subscriber := range self.subscribes {
		go func(subscriber *Subscriber) {
			err := self.CloseSubscribe(subscriber)
			if err != nil {
				self.subscribeLock.Lock()
				if _, ok := self.subscribes[subscriber.subscribeId]; ok {
					delete(self.subscribes, subscriber.subscribeId)
				}
				subscriber.closed = true
				close(subscriber.channel)
				close(subscriber.closedWaiter)
				self.subscribeLock.Unlock()
			}
		}(subscriber)
		subscriberClosedWaiters = append(subscriberClosedWaiters, subscriber.closedWaiter)
	}
	self.subscribeLock.Unlock()
	for _, closedWaiter := range subscriberClosedWaiters {
		<-closedWaiter
	}

	self.requestLock.Lock()
	self.closed = true
	for requestId := range self.requests {
		close(self.requests[requestId])
	}
	self.requests = make(map[[16]byte]chan protocol.ICommand, 0)
	self.requestLock.Unlock()

	for dbId, db := range self.dbs {
		if db == nil {
			continue
		}

		err := db.Close()
		if err != nil {
			return err
		}
		self.dbs[dbId] = nil
	}

	self.glock.Lock()
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	if self.reconnectWaiter != nil {
		close(self.reconnectWaiter)
	}
	self.glock.Unlock()
	<-self.closedWaiter
	if self.unavailableWaiter != nil {
		close(self.unavailableWaiter)
		self.unavailableWaiter = nil
	}
	return nil
}

func (self *Client) connect(host string, clientId [16]byte) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return err
	}

	stream := NewStream(conn)
	clientProtocol := NewBinaryClientProtocol(stream)
	err = self.initProtocol(clientProtocol, clientId)
	if err != nil {
		_ = clientProtocol.Close()
		return err
	}

	self.protocol = clientProtocol
	if self.replset != nil {
		self.replset.addAvailableClient(self)
	}
	return nil
}

func (self *Client) reconnect() ClientProtocol {
	self.glock.Lock()
	if self.reconnectWaiter != nil {
		self.reconnectWaiter = make(chan bool, 1)
	}
	if self.unavailableWaiter != nil {
		close(self.unavailableWaiter)
		self.unavailableWaiter = nil
	}
	self.glock.Unlock()

	for !self.closed {
		select {
		case <-self.reconnectWaiter:
			continue
		case <-time.After(3 * time.Second):
			err := self.connect(self.serverAddress, self.clientId)
			if err != nil {
				continue
			}

			self.glock.Lock()
			self.reconnectWaiter = nil
			self.glock.Unlock()
			self.reconnectUpdateSubcribers()
			return nil
		}
	}

	self.glock.Lock()
	self.reconnectWaiter = nil
	self.glock.Unlock()
	return nil
}

func (self *Client) reconnectUpdateSubcribers() {
	self.subscribeLock.Lock()
	for _, subscriber := range self.subscribes {
		go func(subscriber *Subscriber) {
			err := self.UpdateSubscribe(subscriber)
			if err != nil {
				_ = self.CloseSubscribe(subscriber)
			}
		}(subscriber)
	}
	self.subscribeLock.Unlock()
}

func (self *Client) initProtocol(clientProtocol ClientProtocol, clientId [16]byte) error {
	initCommand := &protocol.InitCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_INIT, RequestId: self.GenRequestId()}, ClientId: clientId}
	if err := clientProtocol.Write(initCommand); err != nil {
		return err
	}
	result, rerr := clientProtocol.Read()
	if rerr != nil {
		return rerr
	}

	if initResultCommand, ok := result.(*protocol.InitResultCommand); ok {
		return self.handleInitCommandResult(initResultCommand)
	}
	return errors.New("init fail")
}

func (self *Client) handleInitCommandResult(initResultCommand *protocol.InitResultCommand) error {
	if initResultCommand.Result != protocol.RESULT_SUCCED {
		return errors.New(fmt.Sprintf("init stream error: %d", initResultCommand.Result))
	}

	if initResultCommand.InitType != 0 && initResultCommand.InitType != 2 {
		self.requestLock.Lock()
		for requestId := range self.requests {
			close(self.requests[requestId])
			delete(self.requests, requestId)
		}
		self.requestLock.Unlock()
	}
	return nil
}

func (self *Client) process() {
	for !self.closed {
		if self.protocol == nil {
			err := self.reconnect()
			if err != nil {
				return
			}
			continue
		}

		command, err := self.protocol.Read()
		if err != nil {
			self.glock.Lock()
			_ = self.protocol.Close()
			if self.replset != nil {
				self.replset.removeAvailableClient(self)
			}
			self.protocol = nil
			self.glock.Unlock()
			err := self.reconnect()
			if err != nil {
				return
			}
			continue
		}

		if command != nil {
			_ = self.handleCommand(command.(protocol.ICommand))
		}
	}

	self.glock.Lock()
	if self.protocol != nil {
		_ = self.protocol.Close()
		self.protocol = nil
	}
	self.glock.Unlock()
	if self.replset != nil {
		self.replset.removeAvailableClient(self)
	}
	close(self.closedWaiter)
}

func (self *Client) getOrNewDB(dbId uint8) *Database {
	self.glock.Lock()
	db := self.dbs[dbId]
	if db == nil {
		db = NewDatabase(dbId, self)
		self.dbs[dbId] = db
	}
	self.glock.Unlock()
	return db
}

func (self *Client) handleCommand(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_INIT:
		initCommand := command.(*protocol.InitResultCommand)
		return self.handleInitCommandResult(initCommand)
	case protocol.COMMAND_PUBLISH:
		lockCommand := command.(*protocol.LockResultCommand)
		subscribeId := uint32(lockCommand.RequestId[12]) | uint32(lockCommand.RequestId[13])<<8 | uint32(lockCommand.RequestId[14])<<16 | uint32(lockCommand.RequestId[15])<<24
		self.subscribeLock.Lock()
		if subscriber, ok := self.subscribes[subscribeId]; ok {
			self.subscribeLock.Unlock()
			return subscriber.Push(lockCommand)
		}
		return nil
	}

	requestId := command.GetRequestId()
	self.requestLock.Lock()
	if request, ok := self.requests[requestId]; ok {
		delete(self.requests, requestId)
		self.requestLock.Unlock()

		request <- command
		return nil
	}
	self.requestLock.Unlock()
	return nil
}

func (self *Client) SelectDB(dbId uint8) *Database {
	db := self.dbs[dbId]
	if db == nil {
		db = self.getOrNewDB(dbId)
	}
	return db
}

func (self *Client) ExecuteCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
	self.requestLock.Lock()
	requestId := command.GetRequestId()
	if _, ok := self.requests[requestId]; ok {
		self.requestLock.Unlock()
		return nil, errors.New("request is used")
	}
	waiter := make(chan protocol.ICommand, 1)
	self.requests[requestId] = waiter
	self.requestLock.Unlock()

	self.glock.Lock()
	if self.protocol == nil {
		self.glock.Unlock()
		return nil, errors.New("client is not opened")
	}
	err := self.protocol.Write(command)
	self.glock.Unlock()
	if err != nil {
		self.requestLock.Lock()
		if _, ok := self.requests[requestId]; ok {
			delete(self.requests, requestId)
		}
		self.requestLock.Unlock()
		return nil, err
	}

	select {
	case r := <-waiter:
		if r == nil {
			return nil, errors.New("wait timeout")
		}
		return r, nil
	case <-time.After(time.Duration(timeout+1) * time.Second):
		self.requestLock.Lock()
		if _, ok := self.requests[requestId]; ok {
			delete(self.requests, requestId)
		}
		self.requestLock.Unlock()
		return nil, errors.New("timeout")
	}
}

func (self *Client) SendCommand(command protocol.ICommand) error {
	self.glock.Lock()
	if self.protocol == nil {
		self.glock.Unlock()
		return errors.New("client is not opened")
	}
	err := self.protocol.Write(command)
	self.glock.Unlock()
	return err
}

func (self *Client) Lock(lockKey [16]byte, timeout uint32, expried uint32) *Lock {
	return self.SelectDB(0).Lock(lockKey, timeout, expried)
}

func (self *Client) Event(eventKey [16]byte, timeout uint32, expried uint32, defaultSeted bool) *Event {
	return self.SelectDB(0).Event(eventKey, timeout, expried, defaultSeted)
}

func (self *Client) Semaphore(semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	return self.SelectDB(0).Semaphore(semaphoreKey, timeout, expried, count)
}

func (self *Client) RWLock(lockKey [16]byte, timeout uint32, expried uint32) *RWLock {
	return self.SelectDB(0).RWLock(lockKey, timeout, expried)
}

func (self *Client) RLock(lockKey [16]byte, timeout uint32, expried uint32) *RLock {
	return self.SelectDB(0).RLock(lockKey, timeout, expried)
}

func (self *Client) MaxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow {
	return self.SelectDB(0).MaxConcurrentFlow(flowKey, count, timeout, expried)
}

func (self *Client) TokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	return self.SelectDB(0).TokenBucketFlow(flowKey, count, timeout, period)
}

func (self *Client) State(dbId uint8) *protocol.StateResultCommand {
	return self.SelectDB(dbId).State()
}

func (self *Client) Subscribe(expried uint32, maxSize uint32) (*Subscriber, error) {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	return self.SubscribeMask(lockKeyMask, expried, maxSize)
}

func (self *Client) SubscribeMask(lockKeyMask [16]byte, expried uint32, maxSize uint32) (*Subscriber, error) {
	self.subscribeLock.Lock()
	subscriberClientIdIndex++
	clientId := subscriberClientIdIndex
	self.subscribeLock.Unlock()
	command := protocol.NewSubscribeCommand(clientId, 0, 0, lockKeyMask, expried, maxSize)
	resultCommand, err := self.ExecuteCommand(command, 5)
	if err != nil {
		return nil, err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return nil, errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCED {
		return nil, errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}

	subscriber := NewSubscriber(self, clientId, subscribeResultCommand.SubscribeId, lockKeyMask, expried, maxSize)
	self.subscribeLock.Lock()
	self.subscribes[subscriber.subscribeId] = subscriber
	self.subscribeLock.Unlock()
	return subscriber, nil
}

func (self *Client) CloseSubscribe(subscriber *Subscriber) error {
	if subscriber.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(subscriber.clientId, subscriber.subscribeId, 1, subscriber.lockKeyMask, subscriber.expried, subscriber.maxSize)
	resultCommand, err := self.ExecuteCommand(command, 5)
	if err != nil {
		return err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCED {
		return errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}

	self.subscribeLock.Lock()
	if _, ok := self.subscribes[subscriber.subscribeId]; ok {
		delete(self.subscribes, subscriber.subscribeId)
	}
	subscriber.closed = true
	close(subscriber.channel)
	close(subscriber.closedWaiter)
	self.subscribeLock.Unlock()
	if subscriber.replset != nil {
		hasAvailable := false
		for _, s := range subscriber.replset.subscribers {
			if !s.closed {
				hasAvailable = true
			}
		}
		if !hasAvailable {
			_ = subscriber.replset.Close()
		}
	}
	return nil
}

func (self *Client) UpdateSubscribe(subscriber *Subscriber) error {
	if subscriber.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(subscriber.clientId, subscriber.subscribeId, 0, subscriber.lockKeyMask, subscriber.expried, subscriber.maxSize)
	resultCommand, err := self.ExecuteCommand(command, 5)
	if err != nil {
		return err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCED {
		return errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}
	subscriber.subscribeId = subscribeResultCommand.SubscribeId
	return nil
}

func (self *Client) GenRequestId() [16]byte {
	return protocol.GenRequestId()
}

func (self *Client) Unavailable() chan bool {
	self.glock.Lock()
	if self.unavailableWaiter == nil {
		self.unavailableWaiter = make(chan bool, 1)
	}
	self.glock.Unlock()
	return self.unavailableWaiter
}

type ReplsetClient struct {
	glock             *sync.Mutex
	clients           []*Client
	availableClients  []*Client
	closed            bool
	closedWaiter      chan bool
	unavailableWaiter chan bool
}

func NewReplsetClient(hosts []string) *ReplsetClient {
	replsetClient := &ReplsetClient{&sync.Mutex{}, make([]*Client, 0),
		make([]*Client, 0), false, make(chan bool, 1), nil}

	for _, host := range hosts {
		client := &Client{&sync.Mutex{}, replsetClient, nil, make([]*Database, 256),
			make(map[[16]byte]chan protocol.ICommand, 64), &sync.Mutex{}, make(map[uint32]*Subscriber, 4),
			&sync.Mutex{}, host, protocol.GenClientId(),
			false, make(chan bool, 1), nil, nil}
		replsetClient.clients = append(replsetClient.clients, client)
	}
	return replsetClient
}

func (self *ReplsetClient) Open() error {
	if len(self.clients) == 0 {
		return errors.New("not client")
	}

	for _, client := range self.clients {
		err := client.Open()
		if err != nil {
			go client.process()
		}
	}
	return nil
}

func (self *ReplsetClient) Close() error {
	for _, client := range self.clients {
		_ = client.Close()
	}
	close(self.closedWaiter)
	if self.unavailableWaiter != nil {
		close(self.unavailableWaiter)
		self.unavailableWaiter = nil
	}
	return nil
}

func (self *ReplsetClient) addAvailableClient(client *Client) {
	self.glock.Lock()
	self.availableClients = append(self.availableClients, client)
	self.glock.Unlock()
}

func (self *ReplsetClient) removeAvailableClient(client *Client) {
	self.glock.Lock()
	availableClients := make([]*Client, 0)
	for _, c := range self.availableClients {
		if c != client {
			availableClients = append(availableClients, c)
		}
	}
	self.availableClients = availableClients
	if len(self.availableClients) == 0 {
		if self.unavailableWaiter != nil {
			close(self.unavailableWaiter)
			self.unavailableWaiter = nil
		}
	}
	self.glock.Unlock()
}

func (self *ReplsetClient) GetClient() *Client {
	self.glock.Lock()
	if len(self.availableClients) > 0 {
		client := self.availableClients[0]
		self.glock.Unlock()
		return client
	}
	self.glock.Unlock()
	return self.clients[0]
}

func (self *ReplsetClient) SelectDB(dbId uint8) *Database {
	client := self.GetClient()
	return client.SelectDB(dbId)
}

func (self *ReplsetClient) Lock(lockKey [16]byte, timeout uint32, expried uint32) *Lock {
	client := self.GetClient()
	return client.SelectDB(0).Lock(lockKey, timeout, expried)
}

func (self *ReplsetClient) Event(eventKey [16]byte, timeout uint32, expried uint32, defaultSeted bool) *Event {
	client := self.GetClient()
	return client.SelectDB(0).Event(eventKey, timeout, expried, defaultSeted)
}

func (self *ReplsetClient) Semaphore(semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	client := self.GetClient()
	return client.SelectDB(0).Semaphore(semaphoreKey, timeout, expried, count)
}

func (self *ReplsetClient) RWLock(lockKey [16]byte, timeout uint32, expried uint32) *RWLock {
	client := self.GetClient()
	return client.SelectDB(0).RWLock(lockKey, timeout, expried)
}

func (self *ReplsetClient) RLock(lockKey [16]byte, timeout uint32, expried uint32) *RLock {
	client := self.GetClient()
	return client.SelectDB(0).RLock(lockKey, timeout, expried)
}

func (self *ReplsetClient) MaxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow {
	client := self.GetClient()
	return client.SelectDB(0).MaxConcurrentFlow(flowKey, count, timeout, expried)
}

func (self *ReplsetClient) TokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	client := self.GetClient()
	return client.SelectDB(0).TokenBucketFlow(flowKey, count, timeout, period)
}

func (self *ReplsetClient) Subscribe(expried uint32, maxSize uint32) (*ReplsetSubscriber, error) {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	return self.SubscribeMask(lockKeyMask, expried, maxSize)
}

func (self *ReplsetClient) SubscribeMask(lockKeyMask [16]byte, expried uint32, maxSize uint32) (*ReplsetSubscriber, error) {
	replsetSubscriber := NewReplsetSubscriber(self, lockKeyMask, expried, maxSize)
	var err error = nil
	for _, client := range self.availableClients {
		subscriber, cerr := client.SubscribeMask(lockKeyMask, expried, maxSize)
		if cerr != nil {
			err = cerr
			continue
		}
		_ = replsetSubscriber.addSubscriber(subscriber)
	}

	if len(replsetSubscriber.subscribers) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("empty clients")
	}
	return replsetSubscriber, nil
}

func (self *ReplsetClient) CloseSubscribe(replsetSubscriber *ReplsetSubscriber) error {
	if replsetSubscriber.closed {
		return nil
	}

	for _, subscriber := range replsetSubscriber.subscribers {
		_ = subscriber.client.CloseSubscribe(subscriber)
	}

	self.glock.Lock()
	replsetSubscriber.closed = true
	close(replsetSubscriber.channel)
	close(replsetSubscriber.closedWaiter)
	self.glock.Unlock()
	return nil
}

func (self *ReplsetClient) UpdateSubscribe(replsetSubscriber *ReplsetSubscriber) error {
	if replsetSubscriber.closed {
		return nil
	}

	for _, subscriber := range replsetSubscriber.subscribers {
		_ = subscriber.client.UpdateSubscribe(subscriber)
	}
	return nil
}

func (self *ReplsetClient) GenRequestId() [16]byte {
	return protocol.GenRequestId()
}

func (self *ReplsetClient) Unavailable() chan bool {
	self.glock.Lock()
	if self.unavailableWaiter == nil {
		self.unavailableWaiter = make(chan bool, 1)
	}
	self.glock.Unlock()
	return self.unavailableWaiter
}
