package client

import (
    "errors"
    "github.com/snower/slock/protocol"
    "math/rand"
    "sync"
    "sync/atomic"
    "time"
)

type Database struct {
    db_id uint8
    client *Client
    requests map[[2]uint64]chan protocol.ICommand
    glock *sync.Mutex
}

func NewDatabase(db_id uint8, client *Client) *Database {
    return &Database{db_id, client, make(map[[2]uint64]chan protocol.ICommand, 4096), &sync.Mutex{}}
}

func (self *Database) Close() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    for request_id := range self.requests {
        self.requests[request_id] <- nil
    }

    self.requests = make(map[[2]uint64]chan protocol.ICommand, 0)

    self.client = nil
    return nil
}

func (self *Database) HandleLockCommandResult (command *protocol.LockResultCommand) error {
    self.glock.Lock()

    if request, ok := self.requests[command.RequestId]; ok {
        delete(self.requests, command.RequestId)
        self.glock.Unlock()

        request <- command
        return nil
    }

    self.glock.Unlock()
    return nil
}

func (self *Database) HandleUnLockCommandResult (command *protocol.LockResultCommand) error {
    self.glock.Lock()

    if request, ok := self.requests[command.RequestId]; ok {
        delete(self.requests, command.RequestId)
        self.glock.Unlock()

        request <- command
        return nil
    }

    self.glock.Unlock()
    return nil
}

func (self *Database) HandleStateCommandResult (command *protocol.ResultStateCommand) error {
    self.glock.Lock()

    if request, ok := self.requests[command.RequestId]; ok {
        delete(self.requests, command.RequestId)
        self.glock.Unlock()

        request <- command
        return nil
    }

    self.glock.Unlock()
    return nil
}

func (self *Database) SendLockCommand(command *protocol.LockCommand) (*protocol.LockResultCommand, error) {
    if self.client.protocol == nil {
        return nil, errors.New("client is not opened")
    }

    self.glock.Lock()
    if _, ok := self.requests[command.RequestId]; ok {
        self.glock.Unlock()
        return nil, errors.New("request is used")
    }

    waiter := make(chan protocol.ICommand, 1)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.client.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        if _, ok := self.requests[command.RequestId]; ok {
            delete(self.requests, command.RequestId)
        }
        self.glock.Unlock()
        return nil, err
    }

    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*protocol.LockResultCommand), nil
}

func (self *Database) SendUnLockCommand(command *protocol.LockCommand) (*protocol.LockResultCommand, error) {
    if self.client.protocol == nil {
        return nil, errors.New("client is not opened")
    }

    self.glock.Lock()
    if _, ok := self.requests[command.RequestId]; ok {
        self.glock.Unlock()
        return nil, errors.New("request is used")
    }

    waiter := make(chan protocol.ICommand, 1)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.client.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        if _, ok := self.requests[command.RequestId]; ok {
            delete(self.requests, command.RequestId)
        }
        self.glock.Unlock()
        return nil, err
    }

    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*protocol.LockResultCommand), nil
}

func (self *Database) SendStateCommand(command *protocol.StateCommand) (*protocol.ResultStateCommand, error) {
    if self.client.protocol == nil {
        return nil, errors.New("client not opened")
    }

    self.glock.Lock()
    if _, ok := self.requests[command.RequestId]; ok {
        self.glock.Unlock()
        return nil, errors.New("request used")
    }

    waiter := make(chan protocol.ICommand, 1)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.client.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        if _, ok := self.requests[command.RequestId]; ok {
            delete(self.requests, command.RequestId)
        }
        self.glock.Unlock()
        return nil, err
    }

    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*protocol.ResultStateCommand), nil
}

func (self *Database) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *Lock {
    return NewLock(self, lock_key, timeout, expried, 0, 0)
}

func (self *Database) Event(event_key [2]uint64, timeout uint32, expried uint32) *Event {
    return NewEvent(self, event_key, timeout, expried)
}

func (self *Database) CycleEvent(event_key [2]uint64, timeout uint32, expried uint32) *CycleEvent {
    return NewCycleEvent(self, event_key, timeout, expried)
}

func (self *Database) Semaphore(semaphore_key [2]uint64, timeout uint32, expried uint32, count uint16) *Semaphore {
    return NewSemaphore(self, semaphore_key, timeout, expried, count)
}

func (self *Database) RWLock(lock_key [2]uint64, timeout uint32, expried uint32) *RWLock {
    return NewRWLock(self, lock_key, timeout, expried)
}

func (self *Database) RLock(lock_key [2]uint64, timeout uint32, expried uint32) *RLock {
    return NewRLock(self, lock_key, timeout, expried)
}

func (self *Database) State() *protocol.ResultStateCommand {
    request_id := self.GetRequestId()
    command := &protocol.StateCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_STATE, RequestId: request_id},
        Flag: 0, DbId: self.db_id, Blank: [43]byte{}}
    result_command, err := self.SendStateCommand(command)
    if err != nil {
        return nil
    }
    return result_command
}

func (self *Database) GetRequestId() [2]uint64 {
    request_id := [2]uint64{}
    request_id[0] = (uint64(time.Now().Unix()) & 0xffffffff)<<32 | uint64(LETTERS[rand.Intn(52)])<<24 | uint64(LETTERS[rand.Intn(52)])<<16 | uint64(LETTERS[rand.Intn(52)])<<8 | uint64(LETTERS[rand.Intn(52)])
    request_id[1] = atomic.AddUint64(&request_id_index, 1)
    return request_id
}

func (self *Database) GenLockId() ([2]uint64) {
    lock_id := [2]uint64{}
    lock_id[0] = (uint64(time.Now().Unix()) & 0xffffffff)<<32 | uint64(LETTERS[rand.Intn(52)])<<24 | uint64(LETTERS[rand.Intn(52)])<<16 | uint64(LETTERS[rand.Intn(52)])<<8 | uint64(LETTERS[rand.Intn(52)])
    lock_id[1] = atomic.AddUint64(&lock_id_index, 1)
    return lock_id
}