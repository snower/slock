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
    protocol := self.protocol

    defer func() {
        defer self.glock.Unlock()
        self.glock.Lock()

        protocol.Close()

        for _, db := range self.dbs {
            if db != nil {
                db.HandleClose()
            }
        }
    }()

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

func (self *Client) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *ClientLock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [2]uint64, timeout uint32, expried uint32) *ClientEvent {
    return self.SelectDB(0).Event(event_key, timeout, expried)
}

func (self *Client) State(db_id uint8) *ResultStateCommand {
    return self.SelectDB(db_id).State()
}

type ClientDB struct {
    db_id uint8
    client *Client
    protocol *ClientProtocol
    requests map[[2]uint64]chan ICommand
    glock sync.Mutex
}

func NewClientDB(db_id uint8, client *Client, protocol *ClientProtocol) *ClientDB {
    return &ClientDB{db_id, client, protocol, make(map[[2]uint64]chan ICommand, 0), sync.Mutex{}}
}

func (self *ClientDB) HandleClose() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    for request_id := range self.requests {
        self.requests[request_id] <- nil
    }

    self.requests = make(map[[2]uint64]chan ICommand, 0)
    return nil
}

func (self *ClientDB) HandleLockCommandResult (command *LockResultCommand) error {
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

func (self *ClientDB) HandleUnLockCommandResult (command *LockResultCommand) error {
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

func (self *ClientDB) HandleStateCommandResult (command *ResultStateCommand) error {
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

func (self *ClientDB) SendLockCommand(command *LockCommand) (*LockResultCommand, error) {
    waiter := make(chan ICommand)

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
    return result_command.(*LockResultCommand), nil
}

func (self *ClientDB) SendUnLockCommand(command *LockCommand) (*LockResultCommand, error) {
    waiter := make(chan ICommand)

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
    return result_command.(*LockResultCommand), nil
}

func (self *ClientDB) SendStateCommand(command *StateCommand) (*ResultStateCommand, error) {
    waiter := make(chan ICommand)

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
    return result_command.(*ResultStateCommand), nil
}

func (self *ClientDB) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *ClientLock {
    return NewClientLock(self, lock_key, timeout, expried)
}

func (self *ClientDB) Event(event_key [2]uint64, timeout uint32, expried uint32) *ClientEvent {
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
    command := &LockCommand{Command{MAGIC, VERSION, COMMAND_LOCK, request_id}, 0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, 0,[1]byte{}}
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
    command := &LockCommand{Command{ MAGIC, VERSION, COMMAND_UNLOCK, request_id}, 0, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, 0,[1]byte{}}
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