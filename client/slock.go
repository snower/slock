package client

import (
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
    "math/rand"
    "net"
    "sync"
    "time"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var request_id_index uint64 = 0
var lock_id_index uint64 = 0

type Client struct {
    host string
    port uint
    stream *Stream
    protocol ClientProtocol
    dbs []*Database
    glock *sync.Mutex
    client_id [16]byte
    is_stop bool
    reconnect_count int
}

func NewClient(host string, port uint) *Client{
    client := &Client{host, port, nil, nil,make([]*Database, 256), &sync.Mutex{}, [16]byte{}, false, 0}
    client.InitClientId()
    return client
}

func (self *Client) Open() error {
    if self.protocol != nil {
        return errors.New("Client is Opened")
    }

    addr := fmt.Sprintf("%s:%d", self.host, self.port)
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return err
    }
    stream := NewStream(conn)
    client_protocol := NewBinaryClientProtocol(stream)
    if err := self.InitProtocol(client_protocol); err != nil {
        client_protocol.Close()
        return err
    }
    self.stream = stream
    self.protocol = client_protocol
    go self.Handle(self.stream)
    return nil
}

func (self *Client) Reopen() {
    self.reconnect_count = 1
    time.Sleep(time.Duration(self.reconnect_count) * time.Second)

    for !self.is_stop {
        err := self.Open()
        if err == nil {
            break
        }

        if self.reconnect_count >= 64 {
            go func() {
                self.Close()
            }()
            break
        }

        self.reconnect_count++
        time.Sleep(time.Duration(self.reconnect_count) * time.Second)
    }
}

func (self *Client) InitClientId() {
    now := uint32(time.Now().Unix())
    self.client_id = [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
    }
}

func (self *Client) InitProtocol(client_protocol ClientProtocol) error {
    init_command := &protocol.InitCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_INIT, RequestId: self.client_id}, ClientId: self.client_id}
    if err := client_protocol.Write(init_command); err != nil {
        return err
    }

    result, rerr := client_protocol.Read()
    if rerr != nil {
        return rerr
    }

    init_result_command := result.(*protocol.InitResultCommand)
    if init_result_command.Result != protocol.RESULT_SUCCED {
        return errors.New(fmt.Sprintf("init stream error: %d", init_result_command.Result))
    }

    if init_result_command.InitType == 0 {
        for _, db := range self.dbs {
            if db == nil {
                continue
            }

            db.glock.Lock()
            for request_id := range db.requests {
                db.requests[request_id] <- nil
                delete(db.requests, request_id)
            }
            db.glock.Unlock()
        }
    }
    return nil
}

func (self *Client) Handle(stream *Stream) {
    client_protocol := self.protocol

    defer func() {
        defer self.glock.Unlock()
        self.glock.Lock()
        client_protocol.Close()
        self.stream = nil
        self.protocol = nil
        if !self.is_stop {
            self.Reopen()
        }
    }()

    for ; !self.is_stop; {
        command, err := client_protocol.Read()
        if err != nil {
            break
        }
        if command == nil {
            break
        }

        self.HandleCommand(command.(protocol.ICommand))
    }
}

func (self *Client) Close() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.is_stop {
        return nil
    }

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

    if self.protocol != nil {
        err := self.protocol.Close()
        if err != nil {
            return err
        }
    }

    self.stream = nil
    self.protocol = nil
    self.is_stop = true
    return nil
}

func (self *Client) GetDb(db_id uint8) *Database{
    defer self.glock.Unlock()
    self.glock.Lock()

    db := self.dbs[db_id]
    if db == nil {
        db = NewDatabase(db_id, self)
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
        return db.HandleLockCommandResult(lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockResultCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetDb(lock_command.DbId)
        }
        return db.HandleUnLockCommandResult(lock_command)

    case protocol.COMMAND_STATE:
        state_command := command.(*protocol.StateResultCommand)
        db := self.dbs[state_command.DbId]
        if db == nil {
            db = self.GetDb(state_command.DbId)
        }
        return db.HandleStateCommandResult(state_command)
    }
    return nil
}

func (self *Client) SelectDB(db_id uint8) *Database {
    db := self.dbs[db_id]
    if db == nil {
        db = self.GetDb(db_id)
    }
    return db
}

func (self *Client) Lock(lock_key [16]byte, timeout uint32, expried uint32) *Lock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [16]byte, timeout uint32, expried uint32) *Event {
    return self.SelectDB(0).Event(event_key, timeout, expried)
}

func (self *Client) CycleEvent(event_key [16]byte, timeout uint32, expried uint32) *CycleEvent {
    return self.SelectDB(0).CycleEvent(event_key, timeout, expried)
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