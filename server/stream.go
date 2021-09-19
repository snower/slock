package server

import (
    "io"
    "net"
    "sync/atomic"
    "time"
)

var client_id uint64 = 0

const (
    STREAM_TYPE_NORMAL uint8 = 0
    STREAM_TYPE_AOF    uint8 = 1
    STREAM_TYPE_ARBITER uint8 = 2
)

type Stream struct {
    conn            net.Conn
    protocol        ServerProtocol
    start_time      *time.Time
    stream_id       uint64
    stream_type     uint8
    closed          bool
    closed_waiter   chan bool
}

func NewStream(conn net.Conn) *Stream {
    now := time.Now()
    stream := &Stream{conn, nil,&now, atomic.AddUint64(&client_id, 1),
        STREAM_TYPE_NORMAL, false, nil}
    if tcp_conn, ok := conn.(*net.TCPConn); ok {
        if tcp_conn.SetNoDelay(true) != nil {
            return nil
        }
    }
    return stream
}

func (self *Stream) ReadBytes(b []byte) (int, error) {
    if self.closed {
        return 0, io.EOF
    }

    cn := len(b)
    n, err := self.conn.Read(b)
    if err != nil {
        return n, err
    }

    if n < cn {
        for ; n < cn; {
            nn, nerr := self.conn.Read(b[n:])
            if nerr != nil {
                return n + nn, nerr
            }
            n += nn
        }
    }
    return n, nil
}

func (self *Stream) Read(b []byte) (int, error) {
    if self.closed {
        return 0, io.EOF
    }

    return self.conn.Read(b)
}

func (self *Stream) WriteBytes(b []byte) error {
    if self.closed {
        return io.EOF
    }

    cn := len(b)
    n, err := self.conn.Write(b)
    if err != nil {
        return err
    }

    if n < cn {
        for ; n < cn; {
            nn, nerr := self.conn.Write(b[n:])
            if nerr != nil {
                return nerr
            }
            n += nn
        }
    }
    return nil
}

func (self *Stream) Write(b []byte) (int, error) {
    if self.closed {
        return 0, io.EOF
    }

    return self.conn.Write(b)
}

func (self *Stream) Close() error {
    if self.closed {
        return nil
    }

    self.closed = true
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
