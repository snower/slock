package client

import (
    "errors"
    "fmt"
    "math/rand"
    "sync"
    "time"
    "github.com/snower/slock/protocol"
)

type Database struct {
    db_id uint8
    client *Client
    protocol *ClientProtocol
    requests map[[2]uint64]chan protocol.ICommand
    glock sync.Mutex
}

func NewDatabase(db_id uint8, client *Client, client_protocol *ClientProtocol) *Database {
    return &Database{db_id, client, client_protocol, make(map[[2]uint64]chan protocol.ICommand, 0), sync.Mutex{}}
}

func (self *Database) HandleClose() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    for request_id := range self.requests {
        self.requests[request_id] <- nil
    }

    self.requests = make(map[[2]uint64]chan protocol.ICommand, 0)
    return nil
}

func (self *Database) HandleLockCommandResult (command *protocol.LockResultCommand) error {
    self.glock.Lock()

    request, ok := self.requests[command.RequestId]
    if ok {
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

    request, ok := self.requests[command.RequestId]
    if ok {
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

    request, ok := self.requests[command.RequestId]
    if ok {
        delete(self.requests, command.RequestId)
        self.glock.Unlock()

        request <- command
        return nil
    }

    self.glock.Unlock()
    return nil
}

func (self *Database) SendLockCommand(command *protocol.LockCommand) (*protocol.LockResultCommand, error) {
    waiter := make(chan protocol.ICommand)

    self.glock.Lock()
    _, ok := self.requests[command.RequestId]
    if ok {
        self.glock.Unlock()
        return nil, errors.New("request used")
    }

    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        _, ok := self.requests[command.RequestId]
        if ok {
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
    waiter := make(chan protocol.ICommand)

    self.glock.Lock()
    _, ok := self.requests[command.RequestId]
    if ok {
        self.glock.Unlock()
        return nil, errors.New("request used")
    }

    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        _, ok := self.requests[command.RequestId]
        if ok {
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
    waiter := make(chan protocol.ICommand)

    self.glock.Lock()
    _, ok := self.requests[command.RequestId]
    if ok {
        self.glock.Unlock()
        return nil, errors.New("request used")
    }

    self.requests[command.RequestId] = waiter
    self.glock.Unlock()

    err := self.protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        _, ok := self.requests[command.RequestId]
        if ok {
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
    return NewLock(self, lock_key, timeout, expried, 0)
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

func (self *Database) State() *protocol.ResultStateCommand {
    request_id := self.GetRequestId()
    command := &protocol.StateCommand{protocol.Command{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_STATE, request_id},
        0, self.db_id, [43]byte{}}
    result_command, err := self.SendStateCommand(command)
    if err != nil {
        return nil
    }
    return result_command
}

func (self *Database) GetRequestId() [2]uint64 {
    request_id := [2]uint64{}
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    request_id[0] = uint64(now[0]) | uint64(now[1])<<8 | uint64(now[2])<<16 | uint64(now[3])<<24 | uint64(now[4])<<32 | uint64(now[5])<<40 | uint64(now[6])<<48 | uint64(now[7])<<56

    randstr := [16]byte{}
    for i := 0; i < 8; i++{
        randstr[i] = LETTERS[rand.Intn(letters_count)]
    }
    request_id[1] = uint64(randstr[0]) | uint64(randstr[1])<<8 | uint64(randstr[2])<<16 | uint64(randstr[3])<<24 | uint64(randstr[4])<<32 | uint64(randstr[5])<<40 | uint64(randstr[6])<<48 | uint64(randstr[7])<<56

    return request_id
}

func (self *Database) GenLockId() ([2]uint64) {
    lock_id := [2]uint64{}
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    lock_id[0] = uint64(now[0]) | uint64(now[1])<<8 | uint64(now[2])<<16 | uint64(now[3])<<24 | uint64(now[4])<<32 | uint64(now[5])<<40 | uint64(now[6])<<48 | uint64(now[7])<<56

    randstr := [16]byte{}
    for i := 0; i < 8; i++{
        randstr[i] = LETTERS[rand.Intn(letters_count)]
    }
    lock_id[1] = uint64(randstr[0]) | uint64(randstr[1])<<8 | uint64(randstr[2])<<16 | uint64(randstr[3])<<24 | uint64(randstr[4])<<32 | uint64(randstr[5])<<40 | uint64(randstr[6])<<48 | uint64(randstr[7])<<56

    return lock_id
}