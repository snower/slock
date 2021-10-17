package client

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"net"
	"sync"
	"time"
)

type CommandRequest struct {
	command        protocol.ICommand
	waiter         chan protocol.ICommand
	clientProtocol ClientProtocol
}

type Client struct {
	glock             *sync.Mutex
	protocols         []ClientProtocol
	dbs               []*Database
	requests          map[[16]byte]*CommandRequest
	requestLock       *sync.Mutex
	subscribes        map[uint64]*Subscriber
	subscribeLock     *sync.Mutex
	hosts             []string
	clientIds         map[string][16]byte
	closed            bool
	closedWaiter      *sync.WaitGroup
	reconnectWaiters  map[string]chan bool
	unavailableWaiter chan bool
}

func NewClient(host string, port uint) *Client {
	address := fmt.Sprintf("%s:%d", host, port)
	client := &Client{&sync.Mutex{}, make([]ClientProtocol, 0), make([]*Database, 256),
		make(map[[16]byte]*CommandRequest, 64), &sync.Mutex{}, make(map[uint64]*Subscriber, 4),
		&sync.Mutex{}, []string{address}, make(map[string][16]byte, 4),
		false, &sync.WaitGroup{}, make(map[string]chan bool, 4), nil}
	return client
}

func NewReplsetClient(hosts []string) *Client {
	client := &Client{&sync.Mutex{}, make([]ClientProtocol, 0),
		make([]*Database, 256), make(map[[16]byte]*CommandRequest, 64), &sync.Mutex{}, make(map[uint64]*Subscriber, 4),
		&sync.Mutex{}, hosts, make(map[string][16]byte, 4), false, &sync.WaitGroup{},
		make(map[string]chan bool, 4), nil}
	return client
}

func (self *Client) Open() error {
	if len(self.protocols) > 0 {
		return errors.New("Client is Opened")
	}

	var err error
	protocols := make(map[string]ClientProtocol, 4)
	for _, host := range self.hosts {
		self.clientIds[host] = self.genClientId()
		clientProtocol, cerr := self.connect(host, self.clientIds[host])
		if cerr != nil {
			err = cerr
		}
		protocols[host] = clientProtocol
	}

	if len(self.protocols) == 0 {
		return err
	}

	for host, clientProtocol := range protocols {
		go self.process(host, self.clientIds[host], clientProtocol)
		self.closedWaiter.Add(1)
	}
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
	self.closed = true
	self.subscribeLock.Unlock()
	for _, closedWaiter := range subscriberClosedWaiters {
		<-closedWaiter
	}

	self.glock.Lock()
	for requestId := range self.requests {
		close(self.requests[requestId].waiter)
	}
	self.requests = make(map[[16]byte]*CommandRequest, 0)
	self.glock.Unlock()

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

	for _, clientProtocol := range self.protocols {
		_ = clientProtocol.Close()
	}

	self.glock.Lock()
	for _, waiter := range self.reconnectWaiters {
		close(waiter)
	}
	self.glock.Unlock()
	self.closedWaiter.Wait()
	if self.unavailableWaiter != nil {
		close(self.unavailableWaiter)
		self.unavailableWaiter = nil
	}
	return nil
}

func (self *Client) connect(host string, clientId [16]byte) (ClientProtocol, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	stream := NewStream(conn)
	clientProtocol := NewBinaryClientProtocol(stream)
	err = self.initProtocol(clientProtocol, clientId)
	if err != nil {
		_ = clientProtocol.Close()
		return nil, err
	}
	self.addProtocol(clientProtocol)
	return clientProtocol, nil
}

func (self *Client) reconnect(host string, clientId [16]byte, _ ClientProtocol) ClientProtocol {
	self.glock.Lock()
	waiter := make(chan bool, 1)
	self.reconnectWaiters[host] = waiter
	if len(self.protocols) == 0 {
		if self.unavailableWaiter != nil {
			close(self.unavailableWaiter)
			self.unavailableWaiter = nil
		}
	}
	self.glock.Unlock()

	for !self.closed {
		select {
		case <-waiter:
			continue
		case <-time.After(3 * time.Second):
			clientProtocol, err := self.connect(host, clientId)
			if err != nil {
				continue
			}

			self.glock.Lock()
			if _, ok := self.reconnectWaiters[host]; ok {
				delete(self.reconnectWaiters, host)
			}
			self.glock.Unlock()
			self.reconnectUpdateSubcribers(host)
			return clientProtocol
		}
	}

	self.glock.Lock()
	if _, ok := self.reconnectWaiters[host]; ok {
		delete(self.reconnectWaiters, host)
	}
	self.glock.Unlock()
	return nil
}

func (self *Client) reconnectUpdateSubcribers(host string) {
	self.subscribeLock.Lock()
	for _, subscriber := range self.subscribes {
		if subscriber.subscribeHost != host {
			continue
		}

		go func(subscriber *Subscriber) {
			err := self.UpdateSubscribe(subscriber)
			if err != nil {
				_ = self.CloseSubscribe(subscriber)
			}
		}(subscriber)
	}
	self.subscribeLock.Unlock()
}

func (self *Client) addProtocol(clientProtocol ClientProtocol) {
	self.glock.Lock()
	self.protocols = append(self.protocols, clientProtocol)
	self.glock.Unlock()
}

func (self *Client) removeProtocol(clientProtocol ClientProtocol) {
	self.glock.Lock()
	protocols := make([]ClientProtocol, 0)
	for _, p := range self.protocols {
		if p != clientProtocol {
			protocols = append(protocols, p)
		}
	}
	self.protocols = protocols
	self.glock.Unlock()
}

func (self *Client) genClientId() [16]byte {
	return protocol.GenClientId()
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
		return self.handleInitCommandResult(clientProtocol, initResultCommand)
	}
	return errors.New("init fail")
}

func (self *Client) handleInitCommandResult(clientProtocol ClientProtocol, initResultCommand *protocol.InitResultCommand) error {
	if initResultCommand.Result != protocol.RESULT_SUCCED {
		return errors.New(fmt.Sprintf("init stream error: %d", initResultCommand.Result))
	}

	if initResultCommand.InitType != 0 && initResultCommand.InitType != 2 {
		self.glock.Lock()
		for requestId := range self.requests {
			if self.requests[requestId].clientProtocol == clientProtocol {
				close(self.requests[requestId].waiter)
				delete(self.requests, requestId)
			}
		}
		self.glock.Unlock()

		for _, db := range self.dbs {
			if db == nil {
				continue
			}

			db.glock.Lock()
			for requestId := range db.requests {
				if db.requests[requestId].clientProtocol == clientProtocol {
					close(db.requests[requestId].waiter)
					delete(db.requests, requestId)
				}
			}
			db.glock.Unlock()
		}
	}
	return nil
}

func (self *Client) process(host string, clientId [16]byte, clientProtocol ClientProtocol) {
	for !self.closed {
		if clientProtocol == nil {
			clientProtocol = self.reconnect(host, clientId, clientProtocol)
			continue
		}

		command, err := clientProtocol.Read()
		if err != nil {
			_ = clientProtocol.Close()
			self.removeProtocol(clientProtocol)
			clientProtocol = self.reconnect(host, clientId, clientProtocol)
			continue
		}
		if command == nil {
			continue
		}
		_ = self.handleCommand(clientProtocol, command.(protocol.ICommand))
	}

	if clientProtocol != nil {
		_ = clientProtocol.Close()
	}
	self.closedWaiter.Done()
}

func (self *Client) getPrococol() ClientProtocol {
	self.glock.Lock()
	if self.closed || len(self.protocols) == 0 {
		self.glock.Unlock()
		return nil
	}

	clientProtocol := self.protocols[0]
	self.glock.Unlock()
	return clientProtocol
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

func (self *Client) handleCommand(clientProtocol ClientProtocol, command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_LOCK:
		lockCommand := command.(*protocol.LockResultCommand)
		db := self.dbs[lockCommand.DbId]
		if db == nil {
			db = self.getOrNewDB(lockCommand.DbId)
		}
		return db.handleCommandResult(lockCommand)

	case protocol.COMMAND_UNLOCK:
		lockCommand := command.(*protocol.LockResultCommand)
		db := self.dbs[lockCommand.DbId]
		if db == nil {
			db = self.getOrNewDB(lockCommand.DbId)
		}
		return db.handleCommandResult(lockCommand)

	case protocol.COMMAND_STATE:
		stateCommand := command.(*protocol.StateResultCommand)
		db := self.dbs[stateCommand.DbId]
		if db == nil {
			db = self.getOrNewDB(stateCommand.DbId)
		}
		return db.handleCommandResult(stateCommand)
	case protocol.COMMAND_INIT:
		initCommand := command.(*protocol.InitResultCommand)
		return self.handleInitCommandResult(clientProtocol, initCommand)
	case protocol.COMMAND_PUBLISH:
		lockCommand := command.(*protocol.LockResultCommand)
		subscribeId := uint64(lockCommand.RequestId[8]) | uint64(lockCommand.RequestId[9])<<8 | uint64(lockCommand.RequestId[10])<<16 | uint64(lockCommand.RequestId[11])<<24 | uint64(lockCommand.RequestId[12])<<32 | uint64(lockCommand.RequestId[13])<<40 | uint64(lockCommand.RequestId[14])<<48 | uint64(lockCommand.RequestId[15])<<56
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

		request.waiter <- command
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
	clientProtocol := self.getPrococol()
	if clientProtocol == nil {
		return nil, errors.New("client is not opened")
	}
	return self.doExecuteCommand(clientProtocol, command, timeout)
}

func (self *Client) doExecuteCommand(clientProtocol ClientProtocol, command protocol.ICommand, timeout int) (protocol.ICommand, error) {
	if self.closed {
		return nil, errors.New("closed")
	}

	requestId := command.GetRequestId()
	self.requestLock.Lock()
	if _, ok := self.requests[requestId]; ok {
		self.requestLock.Unlock()
		return nil, errors.New("request is used")
	}

	waiter := make(chan protocol.ICommand, 1)
	self.requests[requestId] = &CommandRequest{command, waiter, clientProtocol}
	self.requestLock.Unlock()

	err := clientProtocol.Write(command)
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
	clientProtocol := self.getPrococol()
	if clientProtocol == nil {
		return errors.New("client is not opened")
	}

	return clientProtocol.Write(command)
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
	command := protocol.NewSubscribeCommand(0, 0, lockKeyMask, expried, maxSize)
	clientProtocol := self.getPrococol()
	if clientProtocol == nil {
		return nil, errors.New("client is not opened")
	}
	resultCommand, err := self.doExecuteCommand(clientProtocol, command, 5)
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

	subscriber := NewSubscriber(self, clientProtocol.GetStream().RemoteAddr().String(), subscribeResultCommand.SubscribeId,
		lockKeyMask, expried, maxSize)
	self.subscribeLock.Lock()
	self.subscribes[subscriber.subscribeId] = subscriber
	self.subscribeLock.Unlock()
	return subscriber, nil
}

func (self *Client) CloseSubscribe(subscriber *Subscriber) error {
	if subscriber.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(subscriber.subscribeId, 1, subscriber.lockKeyMask, subscriber.expried, subscriber.maxSize)
	var clientProtocol ClientProtocol = nil
	self.glock.Lock()
	for _, cp := range self.protocols {
		stream := cp.GetStream()
		if stream == nil {
			continue
		}
		if stream.RemoteAddr().String() == subscriber.subscribeHost {
			clientProtocol = cp
			break
		}
	}
	self.glock.Unlock()
	if clientProtocol == nil {
		return errors.New("client stream unconnected")
	}

	resultCommand, err := self.doExecuteCommand(clientProtocol, command, 5)
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
	return nil
}

func (self *Client) UpdateSubscribe(subscriber *Subscriber) error {
	if subscriber.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(subscriber.subscribeId, 0, subscriber.lockKeyMask, subscriber.expried, subscriber.maxSize)
	var clientProtocol ClientProtocol = nil
	self.glock.Lock()
	for _, cp := range self.protocols {
		stream := cp.GetStream()
		if stream == nil {
			continue
		}
		if stream.RemoteAddr().String() == subscriber.subscribeHost {
			clientProtocol = cp
			break
		}
	}
	self.glock.Unlock()
	if clientProtocol == nil {
		return nil
	}

	resultCommand, err := self.doExecuteCommand(clientProtocol, command, 5)
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
