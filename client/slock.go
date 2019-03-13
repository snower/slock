package client

import (
    "net"
    "fmt"
    "sync"
    "errors"
    "time"
    "math/rand"
    "github.com/snower/slock/protocol"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type Client struct {
    host string
    port int
    stream *Stream
    protocol *ClientProtocol
    dbs []*ClientDB
    glock sync.Mutex
    is_stop bool
}

func NewClient(host string, port int) *Client{
    return &Client{host, port, nil, nil,make([]*ClientDB, 256), sync.Mutex{}, false}
}

func (self *Client) Open() error {
    addr := fmt.Sprintf("%s:%d", self.host, self.port)
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return err
    }
    self.stream = NewStream(self, conn)
    self.protocol = NewClientProtocol(self.stream)
    go self.Handle(self.stream)
    return nil
}

func (self *Client) Handle(stream *Stream) (err error) {
    client_protocol := self.protocol

    defer func() {
        defer self.glock.Unlock()
        self.glock.Lock()

        client_protocol.Close()

        for _, db := range self.dbs {
            if db != nil {
                db.HandleClose()
            }
        }
    }()

    for {
        command, err := client_protocol.Read()
        if err != nil {
            break
        }
        if command == nil {
            break
        }

        go self.HandleCommand(command.(protocol.ICommand))
    }

    return nil
}

func (self *Client) Close() error {
    self.protocol.Close()
    self.is_stop = true
    return nil
}

func (self *Client) GetDb(db_id uint8) *ClientDB{
    defer self.glock.Unlock()
    self.glock.Lock()

    db := self.dbs[db_id]
    if db == nil {
        db = NewClientDB(db_id, self, self.protocol)
        self.dbs[db_id] = db
    }
    return db
}

func (self *Client) HandleCommand(command protocol.ICommand) error{
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_command := command.(*protocol.LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetDb(lock_command.DbId)
        }
        db.HandleLockCommandResult(lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetDb(lock_command.DbId)
        }
        db.HandleUnLockCommandResult(lock_command)

    case protocol.COMMAND_STATE:
        state_command := command.(*protocol.ResultStateCommand)
        db := self.dbs[state_command.DbId]
        if db == nil {
            db = self.GetDb(state_command.DbId)
        }
        db.HandleStateCommandResult(state_command)
    }
    return nil
}

func (self *Client) SelectDB(db_id uint8) *ClientDB {
    db := self.dbs[db_id]
    if db == nil {
        db = self.GetDb(db_id)
    }
    return db
}

func (self *Client) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *ClientLock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [2]uint64, timeout uint32, expried uint32) *ClientEvent {
    return self.SelectDB(0).Event(event_key, timeout, expried)
}

func (self *Client) State(db_id uint8) *protocol.ResultStateCommand {
    return self.SelectDB(db_id).State()
}

type ClientDB struct {
    db_id uint8
    client *Client
    protocol *ClientProtocol
    requests map[[2]uint64]chan protocol.ICommand
    glock sync.Mutex
}

func NewClientDB(db_id uint8, client *Client, client_protocol *ClientProtocol) *ClientDB {
    return &ClientDB{db_id, client, client_protocol, make(map[[2]uint64]chan protocol.ICommand, 0), sync.Mutex{}}
}

func (self *ClientDB) HandleClose() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    for request_id := range self.requests {
        self.requests[request_id] <- nil
    }

    self.requests = make(map[[2]uint64]chan protocol.ICommand, 0)
    return nil
}

func (self *ClientDB) HandleLockCommandResult (command *protocol.LockResultCommand) error {
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

func (self *ClientDB) HandleUnLockCommandResult (command *protocol.LockResultCommand) error {
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

func (self *ClientDB) HandleStateCommandResult (command *protocol.ResultStateCommand) error {
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

func (self *ClientDB) SendLockCommand(command *protocol.LockCommand) (*protocol.LockResultCommand, error) {
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

func (self *ClientDB) SendUnLockCommand(command *protocol.LockCommand) (*protocol.LockResultCommand, error) {
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

func (self *ClientDB) SendStateCommand(command *protocol.StateCommand) (*protocol.ResultStateCommand, error) {
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

func (self *ClientDB) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *ClientLock {
    return NewClientLock(self, lock_key, timeout, expried)
}

func (self *ClientDB) Event(event_key [2]uint64, timeout uint32, expried uint32) *ClientEvent {
    return NewClientEvent(self, event_key, timeout, expried)
}

func (self *ClientDB) State() *protocol.ResultStateCommand {
    request_id := self.GetRequestId()
    command := &protocol.StateCommand{protocol.Command{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_STATE, request_id},
        0, self.db_id, [43]byte{}}
    result_command, err := self.SendStateCommand(command)
    if err != nil {
        return nil
    }
    return result_command
}

func (self *ClientDB) GetRequestId() [2]uint64 {
    request_id := [2]uint64{}
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    request_id[0] = uint64(now[0]) | uint64(now[1])<<8 | uint64(now[2])<<16 | uint64(now[3])<<24 | uint64(now[4])<<32 | uint64(now[5])<<40 | uint64(now[6])<<48 | uint64(now[7])<<56

    randstr := [16]byte{}
    for i := 0; i < 8; i++{
        randstr[i] = LETTERS[rand.Intn(letters_count)]
    }
    request_id[0] = uint64(randstr[0]) | uint64(randstr[1])<<8 | uint64(randstr[2])<<16 | uint64(randstr[3])<<24 | uint64(randstr[4])<<32 | uint64(randstr[5])<<40 | uint64(randstr[6])<<48 | uint64(randstr[7])<<56

    return request_id
}

func (self *ClientDB) GenLockId() ([2]uint64) {
    lock_id := [2]uint64{}
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    lock_id[0] = uint64(now[0]) | uint64(now[1])<<8 | uint64(now[2])<<16 | uint64(now[3])<<24 | uint64(now[4])<<32 | uint64(now[5])<<40 | uint64(now[6])<<48 | uint64(now[7])<<56

    randstr := [16]byte{}
    for i := 0; i < 8; i++{
        randstr[i] = LETTERS[rand.Intn(letters_count)]
    }
    lock_id[0] = uint64(randstr[0]) | uint64(randstr[1])<<8 | uint64(randstr[2])<<16 | uint64(randstr[3])<<24 | uint64(randstr[4])<<32 | uint64(randstr[5])<<40 | uint64(randstr[6])<<48 | uint64(randstr[7])<<56

    return lock_id
}

type ClientLockError struct {
    Result uint8
    Err   error
}

func (self ClientLockError) Error() string {
    return fmt.Sprintf("%d %s", self.Result, self.Err.Error())
}

type ClientLock struct {
    db *ClientDB
    request_id [2]uint64
    lock_id [2]uint64
    lock_key [2]uint64
    timeout uint32
    expried uint32
}

func NewClientLock(db *ClientDB, lock_key [2]uint64, timeout uint32, expried uint32) *ClientLock {
    return &ClientLock{db, db.GetRequestId(), db.GenLockId(), lock_key, timeout, expried}
}

func (self *ClientLock) Lock() *ClientLockError{
    request_id := self.db.GetRequestId()
    command := &protocol.LockCommand{protocol.Command{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_LOCK, request_id},
        0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, 0,[1]byte{}}
    result_command, err := self.db.SendLockCommand(command)
    if err != nil {
        return &ClientLockError{protocol.RESULT_ERROR, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return &ClientLockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

func (self *ClientLock) Unlock() *ClientLockError{
    request_id := self.db.GetRequestId()
    command := &protocol.LockCommand{protocol.Command{ protocol.MAGIC, protocol.VERSION, protocol.COMMAND_UNLOCK, request_id},
        0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, 0,[1]byte{}}
    result_command, err := self.db.SendUnLockCommand(command)
    if err != nil {
        return &ClientLockError{protocol.RESULT_ERROR, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return &ClientLockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

type ClientEvent struct {
    db *ClientDB
    event_key [2]uint64
    timeout uint32
    expried uint32
    event_lock *ClientLock
    check_lock *ClientLock
    wait_lock *ClientLock
    glock sync.Mutex
}

func NewClientEvent(db *ClientDB, event_key [2]uint64, timeout uint32, expried uint32) *ClientEvent {
    return &ClientEvent{db, event_key, timeout, expried, nil, nil, nil, sync.Mutex{}}
}

func (self *ClientEvent) Clear() error{
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.event_lock == nil {
        self.event_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried}
    }
    err := self.event_lock.Lock()
    if err != nil && err.Result != protocol.RESULT_LOCKED_ERROR {
        return err
    }

    return nil
}

func (self *ClientEvent) Set() error{
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.event_lock == nil {
        self.event_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried}
    }
    err := self.event_lock.Unlock()
    if err != nil && err.Result != protocol.RESULT_UNLOCK_ERROR {
        return err
    }

    return nil
}

func (self *ClientEvent) IsSet() (bool, error){
    defer self.glock.Unlock()
    self.glock.Lock()

    self.check_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, 0, 0}

    err := self.check_lock.Lock()

    if err != nil && err.Result != protocol.RESULT_TIMEOUT {
        return true, nil
    }

    return false, err
}

func (self *ClientEvent) Wait(timeout uint32) (bool, error) {
    defer self.glock.Unlock()
    self.glock.Lock()

    self.wait_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, timeout, 0}

    err := self.wait_lock.Lock()

    if err == nil {
        return true, nil
    }

    return false, err
}