package slock

import (
    "fmt"
    "net"
    "sync"
    "io"
)

type Server struct {
    host    string
    port    int
    server  net.Listener
    streams []*Stream
    slock   *SLock
    glock   sync.Mutex
}

func NewServer(port int, host string, slock *SLock) *Server {
    return &Server{host, port, nil, make([]*Stream, 0), slock, sync.Mutex{}}
}

func (self *Server) Listen() error {
    addr := fmt.Sprintf("%s:%d", self.host, self.port)
    server, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    self.server = server
    return nil
}

func (self *Server) AddStream(stream *Stream) (err error) {
    defer self.glock.Unlock()
    self.glock.Lock()
    self.streams = append(self.streams, stream)
    return nil
}

func (self *Server) RemoveStream(stream *Stream) (err error) {
    defer self.glock.Unlock()
    self.glock.Lock()
    streams := self.streams
    self.streams = make([]*Stream, len(streams))
    for i, v := range streams {
        if stream != v {
            self.streams[i] = v
        }
    }
    return nil
}

func (self *Server) Loop() {
    addr := fmt.Sprintf("%s:%d", self.host, self.port)
    self.slock.Log().Infof("start server %s", addr)
    for {
        conn, err := self.server.Accept()
        if err != nil {
            continue
        }
        stream := NewStream(self, nil, conn)
        self.AddStream(stream)
        go self.Handle(stream)
    }
}

func (self *Server) Handle(stream *Stream) (err error) {
    protocol := NewServerProtocol(self.slock, stream, self.slock.free_lock_commands[self.slock.free_index], &self.slock.free_lock_command_count[self.slock.free_index], self.slock.free_lock_result_commands[self.slock.free_index], &self.slock.free_lock_result_command_count[self.slock.free_index])

    self.slock.glock.Lock()
    self.slock.free_index++
    if self.slock.free_index >= FREE_COMMANDS_SLOT_COUNT {
        self.slock.free_index = 0
    }
    self.slock.glock.Unlock()

    defer protocol.Close()
    for {
        command, err := protocol.Read()
        if err != nil {
            if err != io.EOF {
                self.slock.Log().Infof("read command error: %v", err)
            }
            break
        }
        if command == nil {
            self.slock.Log().Infof("read command decode error", err)
            break
        }
        self.slock.Handle(protocol, command.(ICommand))
    }
    return nil
}
