package client

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Database struct {
	dbId     uint8
	client   *Client
	requests map[[16]byte]*CommandRequest
	glock    *sync.Mutex
}

func NewDatabase(dbId uint8, client *Client) *Database {
	return &Database{dbId, client, make(map[[16]byte]*CommandRequest, 4096), &sync.Mutex{}}
}

func (self *Database) Close() error {
	self.glock.Lock()

	for requestId := range self.requests {
		close(self.requests[requestId].waiter)
	}
	self.requests = make(map[[16]byte]*CommandRequest, 0)
	self.client = nil
	self.glock.Unlock()
	return nil
}

func (self *Database) reconnect(_ string, clientProtocol ClientProtocol) error {
	self.glock.Lock()
	for requestId := range self.requests {
		if self.requests[requestId].clientProtocol == clientProtocol {
			close(self.requests[requestId].waiter)
			delete(self.requests, requestId)
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *Database) handleCommandResult(command protocol.ICommand) error {
	requestId := command.GetRequestId()
	self.glock.Lock()
	if request, ok := self.requests[requestId]; ok {
		delete(self.requests, requestId)
		self.glock.Unlock()

		request.waiter <- command
		return nil
	}
	self.glock.Unlock()
	return nil
}

func (self *Database) executeCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
	if self.client == nil {
		return nil, errors.New("db is not closed")
	}
	clientProtocol := self.client.getPrococol()
	if clientProtocol == nil {
		return nil, errors.New("client is not opened")
	}

	requestId := command.GetRequestId()
	self.glock.Lock()
	if _, ok := self.requests[requestId]; ok {
		self.glock.Unlock()
		return nil, errors.New("request is used")
	}

	waiter := make(chan protocol.ICommand, 1)
	self.requests[requestId] = &CommandRequest{command, waiter, clientProtocol}
	self.glock.Unlock()

	err := clientProtocol.Write(command)
	if err != nil {
		self.glock.Lock()
		if _, ok := self.requests[requestId]; ok {
			delete(self.requests, requestId)
		}
		self.glock.Unlock()
		return nil, err
	}

	select {
	case r := <-waiter:
		if r == nil {
			return nil, errors.New("wait timeout")
		}
		return r, nil
	case <-time.After(time.Duration(timeout+1) * time.Second):
		self.glock.Lock()
		if _, ok := self.requests[requestId]; ok {
			delete(self.requests, requestId)
		}
		self.glock.Unlock()
		return nil, errors.New("timeout")
	}
}

func (self *Database) sendCommand(command protocol.ICommand) error {
	if self.client == nil {
		return errors.New("db is not closed")
	}
	clientProtocol := self.client.getPrococol()
	if clientProtocol == nil {
		return errors.New("client is not opened")
	}

	return clientProtocol.Write(command)
}

func (self *Database) Lock(lockKey [16]byte, timeout uint32, expried uint32) *Lock {
	return NewLock(self, lockKey, timeout, expried)
}

func (self *Database) Event(eventKey [16]byte, timeout uint32, expried uint32, defaultSeted bool) *Event {
	if defaultSeted {
		return NewDefaultSetEvent(self, eventKey, timeout, expried)
	}
	return NewDefaultClearEvent(self, eventKey, timeout, expried)
}

func (self *Database) Semaphore(semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	return NewSemaphore(self, semaphoreKey, timeout, expried, count)
}

func (self *Database) RWLock(lockKey [16]byte, timeout uint32, expried uint32) *RWLock {
	return NewRWLock(self, lockKey, timeout, expried)
}

func (self *Database) RLock(lockKey [16]byte, timeout uint32, expried uint32) *RLock {
	return NewRLock(self, lockKey, timeout, expried)
}

func (self *Database) MaxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow {
	return NewMaxConcurrentFlow(self, flowKey, count, timeout, expried)
}

func (self *Database) TokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	return NewTokenBucketFlow(self, flowKey, count, timeout, period)
}

func (self *Database) State() *protocol.StateResultCommand {
	requestId := self.client.GenRequestId()
	command := &protocol.StateCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_STATE, RequestId: requestId},
		Flag: 0, DbId: self.dbId, Blank: [43]byte{}}
	resultCommand, err := self.executeCommand(command, 5)
	if err != nil {
		return nil
	}

	if c, ok := resultCommand.(*protocol.StateResultCommand); ok {
		return c
	}
	return nil
}

func (self *Database) ListLocks(timeout int) (*protobuf.LockDBListLockResponse, error) {
	request := protobuf.LockDBListLockRequest{DbId: uint32(self.dbId)}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_LOCK", data)
	resultCommand, err := self.client.ExecuteCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListLockResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (self *Database) ListLockLockeds(lockKey [16]byte, timeout int) (*protobuf.LockDBListLockedResponse, error) {
	request := protobuf.LockDBListLockedRequest{DbId: uint32(self.dbId), LockKey: lockKey[:]}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_LOCKED", data)
	resultCommand, err := self.client.ExecuteCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListLockedResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (self *Database) ListLockWaits(lockKey [16]byte, timeout int) (*protobuf.LockDBListWaitResponse, error) {
	request := protobuf.LockDBListWaitRequest{DbId: uint32(self.dbId), LockKey: lockKey[:]}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_WAIT", data)
	resultCommand, err := self.client.ExecuteCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListWaitResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (self *Database) GenRequestId() [16]byte {
	if self.client == nil {
		return [16]byte{}
	}
	return self.client.GenRequestId()
}

func (self *Database) GenLockId() [16]byte {
	now := uint64(time.Now().UnixNano() / 1e6)
	lid := atomic.AddUint32(&lockIdIndex, 1)
	return [16]byte{
		byte(now >> 40), byte(now >> 32), byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
		LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(lid >> 24), byte(lid >> 16), byte(lid >> 8), byte(lid),
	}
}
