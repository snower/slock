package client

import (
    "net"
    "fmt"
    "sync"
    "github.com/snower/slock/protocol"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type Client struct {
    host string
    port int
    stream *Stream
    protocol *ClientProtocol
    dbs []*Database
    glock sync.Mutex
    is_stop bool
}

func NewClient(host string, port int) *Client{
    return &Client{host, port, nil, nil,make([]*Database, 256), sync.Mutex{}, false}
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

func (self *Client) Handle(stream *Stream) {
    client_protocol := self.protocol

    defer func() {
        defer self.glock.Unlock()
        self.glock.Lock()

        for _, db := range self.dbs {
            if db != nil {
                db.HandleClose()
            }
        }

        client_protocol.Close()
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
    err := self.protocol.Close()
    if err != nil {
        return err
    }

    self.is_stop = true
    return nil
}

func (self *Client) GetDb(db_id uint8) *Database{
    defer self.glock.Unlock()
    self.glock.Lock()

    db := self.dbs[db_id]
    if db == nil {
        db = NewDatabase(db_id, self, self.protocol)
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

func (self *Client) State(db_id uint8) *protocol.ResultStateCommand {
    return self.SelectDB(db_id).State()
}