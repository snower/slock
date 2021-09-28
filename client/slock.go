package client

import (
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
    "math/rand"
    "net"
    "sync"
    "sync/atomic"
    "time"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var request_id_index uint32 = 0
var lock_id_index uint32 = 0

type Client struct {
    glock                   *sync.Mutex
    protocols               []ClientProtocol
    dbs                     []*Database
    requests                map[[16]byte]chan protocol.ICommand
    request_lock            *sync.Mutex
    hosts                   []string
    client_id               [16]byte
    closed                  bool
    closed_waiter           *sync.WaitGroup
    reconnect_waiters       map[string]chan bool
}

func NewClient(host string, port uint) *Client{
    address := fmt.Sprintf("%s:%d", host, port)
    client := &Client{&sync.Mutex{}, make([]ClientProtocol, 0), make([]*Database, 256),
        make(map[[16]byte]chan protocol.ICommand, 64), &sync.Mutex{}, []string{address},
        [16]byte{}, false, &sync.WaitGroup{}, make(map[string]chan bool, 4)}
    return client
}

func NewReplsetClient(hosts []string) *Client{
    client := &Client{&sync.Mutex{}, make([]ClientProtocol, 0),
        make([]*Database, 256), make(map[[16]byte]chan protocol.ICommand, 64), &sync.Mutex{},
        hosts, [16]byte{}, false, &sync.WaitGroup{}, make(map[string]chan bool, 4)}
    return client
}

func (self *Client) Open() error {
    if len(self.protocols) > 0 {
        return errors.New("Client is Opened")
    }

    var err error
    if len(self.hosts) == 1 {
        self.initClientId()
    }

    protocols := make(map[string]ClientProtocol, 4)
    for _, host := range self.hosts {
        client_protocol, cerr := self.connect(host)
        if cerr != nil {
            err = cerr
        }
        protocols[host] = client_protocol
    }

    if len(self.protocols) == 0 {
        return err
    }

    for host, client_protocol := range protocols {
        go self.process(host, client_protocol)
        self.closed_waiter.Add(1)
    }
    return nil
}


func (self *Client) Close() error {
    self.glock.Lock()
    if self.closed {
        self.glock.Unlock()
        return nil
    }

    self.closed = true
    for request_id := range self.requests {
        close(self.requests[request_id])
    }
    self.requests = make(map[[16]byte]chan protocol.ICommand, 0)
    self.glock.Unlock()

    for db_id, db := range self.dbs {
        if db == nil {
            continue
        }

        err := db.Close()
        if err != nil {
            return err
        }
        self.dbs[db_id] = nil
    }

    for _, client_protocol := range self.protocols {
        _ = client_protocol.Close()
    }

    self.glock.Lock()
    for _, waiter := range self.reconnect_waiters {
        close(waiter)
    }
    self.glock.Unlock()
    self.closed_waiter.Wait()
    return nil
}

func (self *Client) connect(host string) (ClientProtocol, error) {
    conn, err := net.Dial("tcp", host)
    if err != nil {
        return nil, err
    }

    stream := NewStream(conn)
    client_protocol := NewBinaryClientProtocol(stream)
    if len(self.hosts) == 1 {
        err = self.initProtocol(client_protocol)
        if err != nil {
            _  = client_protocol.Close()
            return nil, err
        }
    }
    self.addProtocol(client_protocol)
    return client_protocol, nil
}

func (self *Client) reconnect(host string) ClientProtocol {
    self.glock.Lock()
    waiter := make(chan bool, 1)
    self.reconnect_waiters[host] = waiter
    self.glock.Unlock()

    for !self.closed {
        select {
        case <- waiter:
            continue
        case <- time.After(3 * time.Second):
            client_protocol, err := self.connect(host)
            if err != nil {
                continue
            }

            self.glock.Lock()
            if _, ok := self.reconnect_waiters[host]; ok {
                delete(self.reconnect_waiters, host)
            }
            self.glock.Unlock()
            return client_protocol
        }
    }

    self.glock.Lock()
    if _, ok := self.reconnect_waiters[host]; ok {
        delete(self.reconnect_waiters, host)
    }
    self.glock.Unlock()
    return nil
}

func (self *Client) addProtocol(client_protocol ClientProtocol) {
    self.glock.Lock()
    self.protocols = append(self.protocols, client_protocol)
    self.glock.Unlock()
}

func (self *Client) removeProtocol(client_protocol ClientProtocol) {
    self.glock.Lock()
    protocols := make([]ClientProtocol, 0)
    for _, p := range self.protocols {
        if p != client_protocol {
            protocols = append(protocols, p)
        }
    }
    self.protocols = protocols
    self.glock.Unlock()
}


func (self *Client) initClientId() {
    now := uint32(time.Now().Unix())
    self.client_id = [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
    }
}

func (self *Client) initProtocol(client_protocol ClientProtocol) error {
    init_command := &protocol.InitCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_INIT, RequestId: self.client_id}, ClientId: self.client_id}
    if err := client_protocol.Write(init_command); err != nil {
        return err
    }
    result, rerr := client_protocol.Read()
    if rerr != nil {
        return rerr
    }

    if init_result_command, ok := result.(*protocol.InitResultCommand); ok {
        if init_result_command.Result != protocol.RESULT_SUCCED {
            return errors.New(fmt.Sprintf("init stream error: %d", init_result_command.Result))
        }

        if init_result_command.InitType != 1 {
            for _, db := range self.dbs {
                if db == nil {
                    continue
                }

                db.glock.Lock()
                for request_id := range db.requests {
                    close(db.requests[request_id])
                }
                db.requests = make(map[[16]byte]chan protocol.ICommand, 0)
                db.glock.Unlock()
            }
        }
        return  nil
    }
    return errors.New("init fail")
}

func (self *Client) process(host string, client_protocol ClientProtocol) {
    for ; !self.closed; {
        if client_protocol == nil {
            client_protocol = self.reconnect(host)
            continue
        }

        command, err := client_protocol.Read()
        if err != nil {
            _ = client_protocol.Close()
            self.removeProtocol(client_protocol)
            self.reconnect(host)
            continue
        }
        if command == nil {
            continue
        }
        _ = self.handleCommand(command.(protocol.ICommand))
    }

    if client_protocol != nil {
        _ =client_protocol.Close()
    }
    self.closed_waiter.Done()
}

func (self *Client) getPrococol() ClientProtocol {
    self.glock.Lock()
    if self.closed || len(self.protocols) == 0 {
        self.glock.Unlock()
        return nil
    }

    client_protocol := self.protocols[0]
    self.glock.Unlock()
    return client_protocol
}

func (self *Client) getOrNewDB(db_id uint8) *Database{
    self.glock.Lock()
    db := self.dbs[db_id]
    if db == nil {
        db = NewDatabase(db_id, self)
        self.dbs[db_id] = db
    }
    self.glock.Unlock()
    return db
}

func (self *Client) handleCommand(command protocol.ICommand) error{
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_command := command.(*protocol.LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.getOrNewDB(lock_command.DbId)
        }
        return db.handleCommandResult(lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.getOrNewDB(lock_command.DbId)
        }
        return db.handleCommandResult(lock_command)

    case protocol.COMMAND_STATE:
        state_command := command.(*protocol.StateResultCommand)
        db := self.dbs[state_command.DbId]
        if db == nil {
            db = self.getOrNewDB(state_command.DbId)
        }
        return db.handleCommandResult(state_command)
    }

    request_id := command.GetRequestId()
    self.request_lock.Lock()
    if request, ok := self.requests[request_id]; ok {
        delete(self.requests, request_id)
        self.request_lock.Unlock()

        request <- command
        return nil
    }
    self.request_lock.Unlock()
    return nil
}

func (self *Client) SelectDB(db_id uint8) *Database {
    db := self.dbs[db_id]
    if db == nil {
        db = self.getOrNewDB(db_id)
    }
    return db
}

func (self *Client) ExecuteCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
    client_protocol := self.getPrococol()
    if client_protocol == nil {
        return nil, errors.New("client is not opened")
    }

    request_id := command.GetRequestId()
    self.request_lock.Lock()
    if _, ok := self.requests[request_id]; ok {
        self.request_lock.Unlock()
        return nil, errors.New("request is used")
    }

    waiter := make(chan protocol.ICommand, 1)
    self.requests[request_id] = waiter
    self.request_lock.Unlock()

    err := client_protocol.Write(command)
    if err != nil {
        self.request_lock.Lock()
        if _, ok := self.requests[request_id]; ok {
            delete(self.requests, request_id)
        }
        self.request_lock.Unlock()
        return nil, err
    }

    select {
    case r := <- waiter:
        if r == nil {
            return nil, errors.New("wait timeout")
        }
        return r.(*protocol.LockResultCommand), nil
    case <- time.After(time.Duration(timeout + 1) * time.Second):
        self.request_lock.Lock()
        if _, ok := self.requests[request_id]; ok {
            delete(self.requests, request_id)
        }
        self.request_lock.Unlock()
        return nil, errors.New("timeout")
    }
}

func (self *Client) Lock(lock_key [16]byte, timeout uint32, expried uint32) *Lock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [16]byte, timeout uint32, expried uint32, default_seted bool) *Event {
    return self.SelectDB(0).Event(event_key, timeout, expried, default_seted)
}

func (self *Client) Semaphore(semaphore_key [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
    return self.SelectDB(0).Semaphore(semaphore_key, timeout, expried, count)
}

func (self *Client) RWLock(lock_key [16]byte, timeout uint32, expried uint32) *RWLock {
    return self.SelectDB(0).RWLock(lock_key, timeout, expried)
}

func (self *Client) RLock(lock_key [16]byte, timeout uint32, expried uint32) *RLock {
    return self.SelectDB(0).RLock(lock_key, timeout, expried)
}

func (self *Client) State(db_id uint8) *protocol.StateResultCommand {
    return self.SelectDB(db_id).State()
}

func (self *Client) GenRequestId() [16]byte {
    now := uint64(time.Now().Nanosecond() / 1e6)
    rid := atomic.AddUint32(&request_id_index, 1)
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(rid >> 24), byte(rid >> 16), byte(rid >> 8), byte(rid),
    }
}