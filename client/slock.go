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

type Client struct {
    host string
    port int
    stream *Stream
    protocol *ClientProtocol
    dbs []*Database
    glock sync.Mutex
    client_id [2]uint64
    is_stop bool
}

func NewClient(host string, port int) *Client{
    client := &Client{host, port, nil, nil,make([]*Database, 256), sync.Mutex{}, [2]uint64{0, 0}, false}
    client.InitClientId()
    return client
}

func (self *Client) Open() error {
    addr := fmt.Sprintf("%s:%d", self.host, self.port)
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return err
    }
    stream := NewStream(self, conn)
    client_protocol := NewClientProtocol(stream)
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
    for !self.is_stop {
        time.Sleep(time.Second)
        addr := fmt.Sprintf("%s:%d", self.host, self.port)
        conn, err := net.Dial("tcp", addr)
        if err != nil {
            continue
        }
        stream := NewStream(self, conn)
        client_protocol := NewClientProtocol(stream)
        if err := self.InitProtocol(client_protocol); err != nil {
            client_protocol.Close()
            continue
        }
        self.stream = stream
        self.protocol = client_protocol
        go self.Handle(self.stream)
        break
    }
}

func (self *Client) InitClientId() {
    now := fmt.Sprintf("%x", int32(time.Now().Unix()))
    letters_count := len(LETTERS)
    self.client_id[0] = uint64(now[0]) | uint64(now[1])<<8 | uint64(now[2])<<16 | uint64(now[3])<<24 | uint64(now[4])<<32 | uint64(now[5])<<40 | uint64(now[6])<<48 | uint64(now[7])<<56

    randstr := [16]byte{}
    for i := 0; i < 8; i++{
        randstr[i] = LETTERS[rand.Intn(letters_count)]
    }
    self.client_id[1] = uint64(randstr[0]) | uint64(randstr[1])<<8 | uint64(randstr[2])<<16 | uint64(randstr[3])<<24 | uint64(randstr[4])<<32 | uint64(randstr[5])<<40 | uint64(randstr[6])<<48 | uint64(randstr[7])<<56
}

func (self *Client) InitProtocol(client_protocol *ClientProtocol) error {
    init_command := &protocol.InitCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.client_id}, ClientId: self.client_id}
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
}

func (self *Client) Close() error {
    if self.is_stop {
        return nil
    }

    err := self.protocol.Close()
    if err != nil {
        return err
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
        state_command := command.(*protocol.ResultStateCommand)
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

func (self *Client) Lock(lock_key [2]uint64, timeout uint32, expried uint32) *Lock {
    return self.SelectDB(0).Lock(lock_key, timeout, expried)
}

func (self *Client) Event(event_key [2]uint64, timeout uint32, expried uint32) *Event {
    return self.SelectDB(0).Event(event_key, timeout, expried)
}

func (self *Client) CycleEvent(event_key [2]uint64, timeout uint32, expried uint32) *CycleEvent {
    return self.SelectDB(0).CycleEvent(event_key, timeout, expried)
}

func (self *Client) Semaphore(semaphore_key [2]uint64, timeout uint32, expried uint32, count uint16) *Semaphore {
    return self.SelectDB(0).Semaphore(semaphore_key, timeout, expried, count)
}

func (self *Client) RWLock(lock_key [2]uint64, timeout uint32, expried uint32) *RWLock {
    return self.SelectDB(0).RWLock(lock_key, timeout, expried)
}

func (self *Client) RLock(lock_key [2]uint64, timeout uint32, expried uint32) *RLock {
    return self.SelectDB(0).RLock(lock_key, timeout, expried)
}


func (self *Client) State(db_id uint8) *protocol.ResultStateCommand {
    return self.SelectDB(db_id).State()
}