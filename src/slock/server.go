package slock

import (
    "fmt"
    "net"
    "sync"
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
    protocol := NewServerProtocol(self.slock, stream)
    defer protocol.Close()
    for {
        command, err := protocol.Read()
        if err != nil {
            break
        }
        if command == nil {
            break
        }
        self.slock.Handle(command.(ICommand))
    }
    return nil
}
