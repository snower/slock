package server

import (
    "fmt"
    "io"
    "net"
    "os"
    "os/signal"
    "sync"
    "syscall"
)

type Server struct {
    slock                   *SLock
    server                  net.Listener
    streams                 []*Stream
    glock                   *sync.Mutex
    connected_count         uint32
    connecting_count        uint32
    is_stop                 bool
}

func NewServer(slock *SLock) *Server {
    server := &Server{slock, nil, make([]*Stream, 0), &sync.Mutex{}, 0, 0,false}
    admin := slock.GetAdmin()
    admin.server = server
    return server
}

func (self *Server) Listen() error {
    server, err := net.Listen("tcp", fmt.Sprintf("%s:%d", Config.Bind, Config.Port))
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
    err := self.server.Close()
    if err != nil {
        self.slock.Log().Errorf("Server Close Error: %v", err)
    }

    self.slock.Close()
    for _, stream := range self.streams {
        if stream == nil {
            continue
        }
        err := stream.Close()
        if err != nil {
            self.slock.Log().Errorf("Stream Close Error: %v", err)
        }
    }
}

func (self *Server) AddStream(stream *Stream) error {
    defer self.glock.Unlock()
    self.glock.Lock()
    self.streams = append(self.streams, stream)
    self.connecting_count++
    self.connected_count++
    return nil
}

func (self *Server) RemoveStream(stream *Stream) error {
    defer self.glock.Unlock()
    self.glock.Lock()
    streams := self.streams
    self.streams = make([]*Stream, len(streams))
    for i, v := range streams {
        if stream != v {
            self.streams[i] = v
        } else {
            self.connecting_count--
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

    self.slock.Log().Infof("Start Server %s", fmt.Sprintf("%s:%d", Config.Bind, Config.Port))
    for ; !self.is_stop; {
        conn, err := self.server.Accept()
        if err != nil {
            continue
        }
        stream := NewStream(self, conn)
        if self.AddStream(stream) != nil {
            err := stream.Close()
            if err != nil {
                self.slock.Log().Errorf("Stream Close Error: %v", err)
            }
            continue
        }
        go self.Handle(stream)
    }
    self.slock.Log().Infof("Server has stopped")
}

func (self *Server) CheckProtocol(stream *Stream) (ServerProtocol, error) {
    buf := make([]byte, 64)
    n, err := stream.Read(buf)
    if err != nil {
        return nil, err
    }

    mv := uint16(buf[0]) | uint16(buf[1])<<8
    if n == 64 && mv == 0x0156 {
        server_protocol := NewBinaryServerProtocol(self.slock, stream)
        err := server_protocol.ProcessParse(buf)
        if err != nil {
            cerr := server_protocol.Close()
            if cerr != nil {
                self.slock.Log().Errorf("Protocol Close error: %v", cerr)
            }
            return nil, err
        }
        self.slock.Log().Infof("New Binary Protocol Connection %s", server_protocol.RemoteAddr().String())
        return server_protocol, nil
    }

    server_protocol := NewTextServerProtocol(self.slock, stream)
    err = server_protocol.ProcessParse(buf[:n])
    if err != nil {
        cerr := server_protocol.Close()
        if cerr != nil {
            self.slock.Log().Errorf("Protocol Close error: %v", err)
        }
        return nil, err
    }
    self.slock.Log().Infof("New Text Protocol Connection %s", server_protocol.RemoteAddr().String())
    return server_protocol, nil
}

func (self *Server) Handle(stream *Stream) {
    server_protocol, err := self.CheckProtocol(stream)
    if err != nil {
        cerr := stream.Close()
        if cerr != nil {
            self.slock.Log().Errorf("Stream Error: %v", cerr)
        }

        if err != io.EOF {
            self.slock.Log().Errorf("Protocol Error: %v", err)
        }
        return
    }

    err = server_protocol.Process()
    if err != nil {
        if err != io.EOF {
            self.slock.Log().Errorf("Protocol Process Error: %v", err)
        }
    }

    err = server_protocol.Close()
    if err != nil {
        self.slock.Log().Errorf("Protocol Close error: %v", err)
    }
    self.slock.Log().Infof("Protocol Close %s", server_protocol.RemoteAddr().String())
}
