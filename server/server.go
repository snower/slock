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
    stoped                  bool
    stoped_waiter           chan bool
}

func NewServer(slock *SLock) *Server {
    server := &Server{slock, nil, make([]*Stream, 0), &sync.Mutex{}, 0, 0,false, make(chan bool, 1)}
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
    self.glock.Lock()
    self.stoped = true
    err := self.server.Close()
    if err != nil {
        self.slock.Log().Errorf("Server Close Error: %v", err)
    }
    self.glock.Unlock()

    for _, stream := range self.streams {
        if stream.stream_type != STREAM_TYPE_NORMAL {
            continue
        }

        stream.closed_waiter = make(chan bool, 1)
        err := stream.Close()
        if err != nil {
            self.slock.Log().Errorf("Stream Close Error: %v", err)
        }
        <- stream.closed_waiter
        stream.closed_waiter = nil
    }
    self.slock.Close()
    self.stoped_waiter <- true
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
    self.streams = make([]*Stream, 0)

    for _, v := range streams {
        if stream != v {
            self.streams = append(self.streams, v)
        } else {
            self.connecting_count--
        }
    }
    return nil
}

func (self *Server) CloseStreams() error {
    for _, stream := range self.streams {
        if stream.stream_type != STREAM_TYPE_NORMAL {
            continue
        }
        stream.Close()
    }
    return nil
}

func (self *Server) Loop() {
    stop_signal := make(chan os.Signal, 1)
    signal.Notify(stop_signal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-stop_signal
        self.slock.Log().Infof("Server is shutdown start")
        self.Close()
    }()

    self.slock.Log().Infof("Start Server %s", fmt.Sprintf("%s:%d", Config.Bind, Config.Port))
    for ; !self.stoped; {
        conn, err := self.server.Accept()
        if err != nil {
            continue
        }

        stream := NewStream(conn)
        err = self.AddStream(stream)
        if err != nil {
            err := stream.Close()
            if err != nil {
                self.slock.Log().Errorf("Stream Close Error: %v", err)
            }
            continue
        }
        go self.Handle(stream)
    }
    <- self.stoped_waiter
    self.slock.Log().Infof("Server has shutdown")
}

func (self *Server) CheckProtocol(stream *Stream) (ServerProtocol, error) {
    var server_protocol ServerProtocol
    buf := make([]byte, 64)
    n, err := stream.Read(buf)
    if err != nil {
        return nil, err
    }

    mv := uint16(buf[0]) | uint16(buf[1])<<8
    if n == 64 && mv == 0x0156 {
        if self.slock.state == STATE_LEADER {
            server_protocol = NewBinaryServerProtocol(self.slock, stream)
        } else {
            server_protocol = NewTransparencyBinaryServerProtocol(self.slock, stream, NewBinaryServerProtocol(self.slock, stream))
        }
        self.slock.Log().Infof("New Binary Protocol Connection %s", server_protocol.RemoteAddr().String())

        err := server_protocol.ProcessParse(buf)
        if err != nil {
            cerr := server_protocol.Close()
            if cerr != nil {
                self.slock.Log().Errorf("Protocol Close error: %v", cerr)
            }
            return nil, err
        }
        return server_protocol, nil
    }


    if self.slock.state == STATE_LEADER {
        server_protocol = NewTextServerProtocol(self.slock, stream)
    } else {
        server_protocol = NewTransparencyTextServerProtocol(self.slock, stream, NewTextServerProtocol(self.slock, stream))
    }
    self.slock.Log().Infof("New Text Protocol Connection %s", server_protocol.RemoteAddr().String())

    err = server_protocol.ProcessParse(buf[:n])
    if err != nil {
        cerr := server_protocol.Close()
        if cerr != nil {
            self.slock.Log().Errorf("Protocol Close error: %v", err)
        }
        return nil, err
    }
    return server_protocol, nil
}

func (self *Server) Handle(stream *Stream) {
    defer func() {
        err := self.RemoveStream(stream)
        if err != nil {
            self.slock.Log().Errorf("Stream Close Remove Stream Error %v", err)
        }
        if stream.closed_waiter != nil {
            stream.closed_waiter <- true
        }
    }()

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

    for {
        switch server_protocol.(type) {
        case *BinaryServerProtocol:
            if self.slock.state != STATE_LEADER {
                if err == AGAIN {
                    binary_server_protocol := server_protocol.(*BinaryServerProtocol)
                    server_protocol = NewTransparencyBinaryServerProtocol(self.slock, stream, binary_server_protocol)
                    err = server_protocol.ProcessParse(binary_server_protocol.rbuf)
                    if err == nil {
                        err = server_protocol.Process()
                    }
                } else {
                    server_protocol = NewTransparencyBinaryServerProtocol(self.slock, stream, server_protocol.(*BinaryServerProtocol))
                    err = server_protocol.Process()
                }
            } else {
                err = server_protocol.Process()
            }
        case *TextServerProtocol:
            if self.slock.state != STATE_LEADER {
                if err == AGAIN {
                    text_server_protocol := server_protocol.(*TextServerProtocol)
                    transparency_server_protocol := NewTransparencyTextServerProtocol(self.slock, stream, text_server_protocol)
                    server_protocol = transparency_server_protocol
                    err = transparency_server_protocol.RunCommand()
                    if err == nil {
                        err = server_protocol.Process()
                    }
                } else {
                    server_protocol = NewTransparencyTextServerProtocol(self.slock, stream, server_protocol.(*TextServerProtocol))
                    if err == nil {
                        err = server_protocol.Process()
                    }
                }
            } else {
                err = server_protocol.Process()
            }
        case *TransparencyBinaryServerProtocol:
            if self.slock.state == STATE_LEADER {
                transparency_server_protocol := server_protocol.(*TransparencyBinaryServerProtocol)
                err = transparency_server_protocol.server_protocol.Process()
            } else {
                err = server_protocol.Process()
            }
        case *TransparencyTextServerProtocol:
            if self.slock.state == STATE_LEADER {
                transparency_server_protocol := server_protocol.(*TransparencyTextServerProtocol)
                err = transparency_server_protocol.server_protocol.Process()
            } else {
                err = server_protocol.Process()
            }
        default:
            err = server_protocol.Process()
        }

        if err != nil {
            if err == AGAIN {
                continue
            }

            if err != io.EOF && !self.stoped {
                self.slock.Log().Errorf("Protocol Process Error: %v", err)
            }
        }
        break
    }

    err = server_protocol.Close()
    if err != nil {
        self.slock.Log().Errorf("Protocol Close error: %v", err)
    }
    self.slock.Log().Infof("Protocol Close %s", server_protocol.RemoteAddr().String())
}
