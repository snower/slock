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
    db_id       uint8
    client      *Client
    requests    map[[16]byte]chan protocol.ICommand
    glock       *sync.Mutex
}

func NewDatabase(db_id uint8, client *Client) *Database {
    return &Database{db_id, client, make(map[[16]byte]chan protocol.ICommand, 4096), &sync.Mutex{}}
}

func (self *Database) Close() error {
    self.glock.Lock()

    for request_id := range self.requests {
        close(self.requests[request_id])
    }
    self.requests = make(map[[16]byte]chan protocol.ICommand, 0)
    self.client = nil
    self.glock.Unlock()
    return nil
}

func (self *Database) handleCommandResult (command protocol.ICommand) error {
    request_id := command.GetRequestId()
    self.glock.Lock()
    if request, ok := self.requests[request_id]; ok {
        delete(self.requests, request_id)
        self.glock.Unlock()

        request <- command
        return nil
    }
    self.glock.Unlock()
    return nil
}

func (self *Database) executeCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
    if self.client == nil {
        return nil, errors.New("db is not closed")
    }
    client_protocol := self.client.getPrococol()
    if client_protocol == nil {
        return nil, errors.New("client is not opened")
    }

    request_id := command.GetRequestId()
    self.glock.Lock()
    if _, ok := self.requests[request_id]; ok {
        self.glock.Unlock()
        return nil, errors.New("request is used")
    }

    waiter := make(chan protocol.ICommand, 1)
    self.requests[request_id] = waiter
    self.glock.Unlock()

    err := client_protocol.Write(command)
    if err != nil {
        self.glock.Lock()
        if _, ok := self.requests[request_id]; ok {
            delete(self.requests, request_id)
        }
        self.glock.Unlock()
        return nil, err
    }

    select {
    case r := <- waiter:
        if r == nil {
            return nil, errors.New("wait timeout")
        }
        return r, nil
    case <- time.After(time.Duration(timeout + 1) * time.Second):
        self.glock.Lock()
        if _, ok := self.requests[request_id]; ok {
            delete(self.requests, request_id)
        }
        self.glock.Unlock()
        return nil, errors.New("timeout")
    }
}

func (self *Database) Lock(lock_key [16]byte, timeout uint32, expried uint32) *Lock {
    return NewLock(self, lock_key, timeout, expried, 0, 0)
}

func (self *Database) Event(event_key [16]byte, timeout uint32, expried uint32, default_seted bool) *Event {
    if default_seted {
        return NewDefaultSetEvent(self, event_key, timeout, expried)
    }
    return NewDefaultClearEvent(self, event_key, timeout, expried)
}

func (self *Database) Semaphore(semaphore_key [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
    return NewSemaphore(self, semaphore_key, timeout, expried, count)
}

func (self *Database) RWLock(lock_key [16]byte, timeout uint32, expried uint32) *RWLock {
    return NewRWLock(self, lock_key, timeout, expried)
}

func (self *Database) RLock(lock_key [16]byte, timeout uint32, expried uint32) *RLock {
    return NewRLock(self, lock_key, timeout, expried)
}

func (self *Database) State() *protocol.StateResultCommand {
    request_id := self.client.GenRequestId()
    command := &protocol.StateCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_STATE, RequestId: request_id},
        Flag: 0, DbId: self.db_id, Blank: [43]byte{}}
    result_command, err := self.executeCommand(command, 5)
    if err != nil {
        return nil
    }

    if c, ok := result_command.(*protocol.StateResultCommand); ok {
        return c
    }
    return nil
}

func (self *Database) GenRequestId() [16]byte {
    if self.client == nil {
        return [16]byte{}
    }
    return self.client.GenRequestId()
}

func (self *Database) GenLockId() [16]byte {
    now := uint64(time.Now().Nanosecond() / 1e6)
    lid := atomic.AddUint32(&lock_id_index, 1)
    return [16]byte{
        byte(now >> 40), byte(now >> 32), byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(lid >> 24), byte(lid >> 16), byte(lid >> 8), byte(lid),
    }
}