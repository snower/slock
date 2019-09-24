package server

import (
    "errors"
    "net"
    "time"
)

type Stream struct {
    server *Server
    conn   net.Conn
    closed bool
}

func NewStream(server *Server, conn net.Conn) *Stream {
    stream := &Stream{server, conn, false}
    tcp_conn, ok := conn.(*net.TCPConn)
    if ok {
        if tcp_conn.SetNoDelay(true) != nil {
            return nil
        }
    }
    return stream
}

func (self *Stream) ReadBytes(b []byte) (int, error) {
    if self.closed {
        return 0, errors.New("stream closed")
    }

    return self.conn.Read(b)
}

func (self *Stream) Read(b []byte) (int, error) {
    if self.closed {
        return 0, errors.New("stream closed")
    }

    return self.conn.Read(b)
}

func (self *Stream) WriteBytes(b []byte) error {
    if self.closed {
        return errors.New("stream closed")
    }

    _, err := self.conn.Write(b)
    return err
}

func (self *Stream) WriteAllBytes(b []byte) error {
    if self.closed {
        return errors.New("stream closed")
    }

    cn := len(b)
    for ; cn > 0; {
        n, err := self.conn.Write(b)
        if err != nil {
            return err
        }
        cn -= n
    }
    return nil
}

func (self *Stream) Write(b []byte) (int, error) {
    if self.closed {
        return 0, errors.New("stream closed")
    }

    return self.conn.Write(b)
}

func (self *Stream) Close() error {
    if self.closed {
        return nil
    }

    self.closed = true
    if self.server != nil {
        err := self.server.RemoveStream(self)
        if err != nil {
            self.server.slock.Log().Errorf("Stream Close Remove Stream Error %v", err)
        }
        self.server = nil
    }
    return self.conn.Close()
}

func (self *Stream) LocalAddr() net.Addr {
    return self.conn.LocalAddr()
}

func (self *Stream) RemoteAddr() net.Addr {
    return self.conn.RemoteAddr()
}

func (self *Stream) SetDeadline(t time.Time) error {
    return self.conn.SetDeadline(t)
}

func (self *Stream) SetReadDeadline(t time.Time) error {
    return self.conn.SetReadDeadline(t)
}

func (self *Stream) SetWriteDeadline(t time.Time) error {
    return self.conn.SetWriteDeadline(t)
}
