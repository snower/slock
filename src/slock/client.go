package slock

import (
    "net"
    "fmt"
    "sync"
    "errors"
    "time"
    "math/rand"
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
    self.stream = NewStream(nil, self, conn)
    self.protocol = NewClientProtocol(self.stream)
    go self.Handle(self.stream)
    return nil
}

func (self *Client) Handle(stream *Stream) (err error) {
    protocol := NewClientProtocol(stream)
    defer protocol.Close()
    for {
        command, err := protocol.Read()
        if err != nil {
            break
        }
        if command == nil {
            break
        }

        go self.HandleCommand(command.(ICommand))
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

func (self *Client) HandleCommand(command ICommand) error{
    switch command.GetCommandType() {
    case COMMAND_LOCK:
        lock_command := command.(*LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetDb(lock_command.DbId)
        }
        db.HandleLockCommandResult(lock_command)

    case COMMAND_UNLOCK:
        lock_command := command.(*LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetDb(lock_command.DbId)
        }
        db.HandleUnLockCommandResult(lock_command)

    case COMMAND_STATE:
        state_command := command.(*ResultStateCommand)
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

func (self *Client) Lock(lock_key [16]byte, timeout uint32, expried uint32) *ClientLock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [16]byte, timeout uint32, expried uint32) *ClientEvent {
    return self.SelectDB(0).Event(event_key, timeout, expried)
}

func (self *Client) State(db_id uint8) *ResultStateCommand {
    return self.SelectDB(db_id).State()
}

type ClientDB struct {
    db_id uint8
    client *Client
    protocol *ClientProtocol
    requests map[[16]byte]chan ICommand
    glock sync.Mutex
}

func NewClientDB(db_id uint8, client *Client, protocol *ClientProtocol) *ClientDB {
    return &ClientDB{db_id, client, protocol, make(map[[16]byte]chan ICommand, 0), sync.Mutex{}}
}

func (self *ClientDB) HandleLockCommandResult (command *LockResultCommand) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    request, ok := self.requests[command.RequestId]
    if ok {
        request <- command
        delete(self.requests, command.RequestId)
    }
    return nil
}

func (self *ClientDB) HandleUnLockCommandResult (command *LockResultCommand) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    request, ok := self.requests[command.RequestId]
    if ok {
        request <- command
        delete(self.requests, command.RequestId)
    }
    return nil
}

func (self *ClientDB) HandleStateCommandResult (command *ResultStateCommand) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    request, ok := self.requests[command.RequestId]
    if ok {
        request <- command
        delete(self.requests, command.RequestId)
    }
    return nil
}

func (self *ClientDB) SendLockCommand(command *LockCommand) (*LockResultCommand, error) {
    err := self.protocol.Write(command)
    if err != nil {
        return nil, err
    }
    self.glock.Lock()
    waiter := make(chan ICommand)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()
    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*LockResultCommand), nil
}

func (self *ClientDB) SendUnLockCommand(command *LockCommand) (*LockResultCommand, error) {
    err := self.protocol.Write(command)
    if err != nil {
        return nil, err
    }
    self.glock.Lock()
    waiter := make(chan ICommand)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()
    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*LockResultCommand), nil
}

func (self *ClientDB) SendStateCommand(command *StateCommand) (*ResultStateCommand, error) {
    err := self.protocol.Write(command)
    if err != nil {
        return nil, err
    }
    self.glock.Lock()
    waiter := make(chan ICommand)
    self.requests[command.RequestId] = waiter
    self.glock.Unlock()
    result_command := <-waiter
    if result_command == nil {
        return nil, errors.New("wait timeout")
    }
    return result_command.(*ResultStateCommand), nil
}

func (self *ClientDB) Lock(lock_key [16]byte, timeout uint32, expried uint32) *ClientLock {
    return NewClientLock(self, lock_key, timeout, expried)
}

func (self *ClientDB) Event(event_key [16]byte, timeout uint32, expried uint32) *ClientEvent {
    return NewClientEvent(self, event_key, timeout, expried)
}

func (self *ClientDB) State() *ResultStateCommand {
    request_id := self.GetRequestId()
    command := &StateCommand{Command{MAGIC, VERSION, COMMAND_STATE, request_id}, 0, self.db_id, [43]byte{}}
    result_command, err := self.SendStateCommand(command)
    if err != nil {
        return nil
    }
    return result_command
}

func (self *ClientDB) GetRequestId() [16]byte {
    request_id := [16]byte{}
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    for i := 0; i< 8; i++ {
        request_id[i] = now[i]
        request_id[8 + i] = LETTERS[rand.Intn(letters_count)]
    }
    return request_id
}

func (self *ClientDB) GenLockId() (request_id [16]byte) {
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    for i := 0; i< 8; i++ {
        request_id[i] = now[i]
        request_id[8 + i] = LETTERS[rand.Intn(letters_count)]
    }
    return request_id
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
    request_id [16]byte
    lock_id [16]byte
    lock_key [16]byte
    timeout uint32
    expried uint32
}

func NewClientLock(db *ClientDB, lock_key [16]byte, timeout uint32, expried uint32) *ClientLock {
    return &ClientLock{db, db.GetRequestId(), db.GenLockId(), lock_key, timeout, expried}
}

func (self *ClientLock) Lock() *ClientLockError{
    request_id := self.db.GetRequestId()
    command := &LockCommand{Command{MAGIC, VERSION, COMMAND_LOCK, request_id}, 0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, [3]byte{}}
    result_command, err := self.db.SendLockCommand(command)
    if err != nil {
        return &ClientLockError{RESULT_ERROR, err}
    }
    if result_command.Result != RESULT_SUCCED {
        return &ClientLockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

func (self *ClientLock) Unlock() *ClientLockError{
    request_id := self.db.GetRequestId()
    command := &LockCommand{Command{ MAGIC, VERSION, COMMAND_UNLOCK, request_id}, 0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, [3]byte{}}
    result_command, err := self.db.SendUnLockCommand(command)
    if err != nil {
        return &ClientLockError{RESULT_ERROR, err}
    }
    if result_command.Result != RESULT_SUCCED {
        return &ClientLockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

type ClientEvent struct {
    db *ClientDB
    event_key [16]byte
    timeout uint32
    expried uint32
    event_lock *ClientLock
    check_lock *ClientLock
    wait_lock *ClientLock
    glock sync.Mutex
}

func NewClientEvent(db *ClientDB, event_key [16]byte, timeout uint32, expried uint32) *ClientEvent {
    return &ClientEvent{db, event_key, timeout, expried, nil, nil, nil, sync.Mutex{}}
}

func (self *ClientEvent) Clear() error{
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.event_lock == nil {
        self.event_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried}
    }
    err := self.event_lock.Lock()
    if err != nil && err.Result != RESULT_LOCKED_ERROR {
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
    if err != nil && err.Result != RESULT_UNLOCK_ERROR {
        return err
    }

    return nil
}

func (self *ClientEvent) IsSet() (bool, error){
    defer self.glock.Unlock()
    self.glock.Lock()

    self.check_lock = &ClientLock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, 0, 0}

    err := self.check_lock.Lock()

    if err != nil && err.Result != RESULT_TIMEOUT {
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