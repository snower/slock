package slock

import (
	"errors"
	"io"
	"net"
	"time"
)

type Stream struct {
	server *Server
	client *Client
	conn   net.Conn
	closed bool
}

func NewStream(server *Server, client *Client, conn net.Conn) *Stream {
	stream := &Stream{server, client, conn, false}
	tcp_conn, ok := conn.(*net.TCPConn)
	if ok {
		tcp_conn.SetNoDelay(true)
	}
	return stream
}

func (self *Stream) ReadBytes(n int) (b []byte, err error) {
	if self.closed {
		return nil, errors.New("stream closed")
	}

	b = make([]byte, n)
	n, err = self.conn.Read(b)
	if err == io.EOF {
		return nil, io.EOF
	}
	return b, err
}

func (self *Stream) Read(b []byte) (n int, err error) {
	if self.closed {
		return 0, errors.New("stream closed")
	}

	return self.conn.Read(b)
}

func (self *Stream) WriteBytes(b []byte) (err error) {
	if self.closed {
		return errors.New("stream closed")
	}

	_, err = self.conn.Write(b)
	return err
}

func (self *Stream) Write(b []byte) (n int, err error) {
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
		self.server.RemoveStream(self)
		self.server = nil
	}

	if self.client != nil {
		self.client = nil
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
