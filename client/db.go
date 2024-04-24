package client

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"google.golang.org/protobuf/proto"
	"sync"
)

type Database struct {
	dbId               uint8
	client             IClient
	glock              *sync.Mutex
	closed             bool
	defaultTimeoutFlag uint16
	defaultExpriedFlag uint16
}

func NewDatabase(dbId uint8, client IClient) *Database {
	return &Database{dbId, client, &sync.Mutex{}, false,
		client.GetDefaultTimeoutFlag(), client.GetDefaultExpriedFlag()}
}

func (self *Database) Close() error {
	self.closed = true
	return nil
}

func (self *Database) executeCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
	if self.client == nil {
		return nil, errors.New("db is not closed")
	}
	return self.client.ExecuteCommand(command, timeout)
}

func (self *Database) sendCommand(command protocol.ICommand) error {
	if self.client == nil {
		return errors.New("db is not closed")
	}
	return self.client.SendCommand(command)
}

func (self *Database) Lock(lockKey [16]byte, timeout uint32, expried uint32) *Lock {
	lock := NewLock(self, lockKey, timeout, expried)
	if self.defaultTimeoutFlag > 0 {
		lock.SetTimeoutFlag(self.defaultTimeoutFlag)
	}
	if self.defaultExpriedFlag > 0 {
		lock.SetExpriedFlag(self.defaultExpriedFlag)
	}
	return lock
}

func (self *Database) Event(eventKey [16]byte, timeout uint32, expried uint32, defaultSeted bool) *Event {
	if defaultSeted {
		return NewDefaultSetEvent(self, eventKey, timeout, expried)
	}
	return NewDefaultClearEvent(self, eventKey, timeout, expried)
}

func (self *Database) GroupEvent(groupKey [16]byte, clientId uint64, versionId uint64, timeout uint32, expried uint32) *GroupEvent {
	return NewGroupEvent(self, groupKey, clientId, versionId, timeout, expried)
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

func (self *Database) TreeLock(lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	return NewTreeLock(self, lockKey, parentKey, timeout, expried)
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
	data, err := proto.Marshal(&request)
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
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (self *Database) ListLockLockeds(lockKey [16]byte, timeout int) (*protobuf.LockDBListLockedResponse, error) {
	request := protobuf.LockDBListLockedRequest{DbId: uint32(self.dbId), LockKey: lockKey[:]}
	data, err := proto.Marshal(&request)
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
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (self *Database) ListLockWaits(lockKey [16]byte, timeout int) (*protobuf.LockDBListWaitResponse, error) {
	request := protobuf.LockDBListWaitRequest{DbId: uint32(self.dbId), LockKey: lockKey[:]}
	data, err := proto.Marshal(&request)
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
	err = proto.Unmarshal(callResultCommand.Data, &response)
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
	return protocol.GenLockId()
}

func (self *Database) SetDefaultTimeoutFlag(timeoutFlag uint16) {
	self.defaultTimeoutFlag = timeoutFlag
}

func (self *Database) SetDefaultExpriedFlag(expriedFlag uint16) {
	self.defaultExpriedFlag = expriedFlag
}

func (self *Database) GetDefaultTimeoutFlag() uint16 {
	return self.defaultTimeoutFlag
}

func (self *Database) GetDefaultExpriedFlag() uint16 {
	return self.defaultExpriedFlag
}
