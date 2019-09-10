package server

import (
    "fmt"
    "net"
    "os"
    "os/signal"
    "sync"
    "io"
    "github.com/snower/slock/protocol"
    "syscall"
)

type Server struct {
    server  net.Listener
    streams []*Stream
    slock   *SLock
    glock   sync.Mutex
    is_stop bool
}

func NewServer(slock *SLock) *Server {
    return &Server{nil, make([]*Stream, 0), slock, sync.Mutex{}, false}
}

func (self *Server) Listen() error {
    addr := fmt.Sprintf("%s:%d", Config.Bind, Config.Port)
    server, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    self.server = server
    return nil
}

func (self *Server) Close() {
    defer self.glock.Unlock()
    self.glock.Lock()

    self.is_stop = true
    self.server.Close()

    self.slock.Close()
    for _, stream := range self.streams {
        stream.Close()
    }
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
    stop_signal := make(chan os.Signal, 1)
    signal.Notify(stop_signal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-stop_signal
        self.slock.Log().Infof("Server is stopping")
        self.Close()
    }()

    addr := fmt.Sprintf("%s:%d", Config.Bind, Config.Port)
    self.slock.Log().Infof("start server %s", addr)
    for ; !self.is_stop; {
        conn, err := self.server.Accept()
        if err != nil {
            continue
        }
        stream := NewStream(self, conn)
        if self.AddStream(stream) == nil {
            go self.Handle(stream)
        }
    }
    self.slock.Log().Infof("Server has stopped")
}

func (self *Server) Handle(stream *Stream) {
    server_protocol := NewServerProtocol(self.slock, stream)
    defer func() {
        err := server_protocol.Close()
        if err != nil {
            self.slock.Log().Infof("server protocol close error: %v", err)
        }
    }()

    for ; !self.is_stop; {
        command, err := server_protocol.Read()
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

        err = self.slock.Handle(server_protocol, command.(protocol.ICommand))
        if err != nil {
            self.slock.Log().Infof("slock handle command error", err)
            break
        }
    }
}
