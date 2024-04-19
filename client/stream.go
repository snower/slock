package client

import (
	"errors"
	"io"
	"net"
	"time"
)

type StreamReaderBuffer struct {
	buf   []byte
	index int
	len   int
	cap   int
}

func NewStreamReaderBuffer(size int) *StreamReaderBuffer {
	return &StreamReaderBuffer{make([]byte, size), 0, 0, size}
}

func (self *StreamReaderBuffer) GetSize() int {
	return self.len - self.index
}

func (self *StreamReaderBuffer) GetCapSize() int {
	return self.cap
}

func (self *StreamReaderBuffer) Read(buf []byte) int {
	if self.index >= self.len {
		return 0
	}

	bufSize, size := self.len-self.index, len(buf)
	if bufSize > size {
		copy(buf, self.buf[self.index:self.index+size])
		self.index += size
		return size
	}

	copy(buf[:bufSize], self.buf[self.index:self.len])
	self.index, self.len = 0, 0
	return bufSize
}

func (self *StreamReaderBuffer) ReadBytesSize(size int) []byte {
	if self.index >= self.len {
		return nil
	}
	bufSize := self.len - self.index
	if bufSize > size {
		buf := self.buf[self.index : self.index+size]
		self.index += size
		return buf
	}

	buf := self.buf[self.index:self.len]
	self.index, self.len = 0, 0
	return buf
}

func (self *StreamReaderBuffer) ReadFromConn(conn net.Conn, size int) (int, error) {
	bufSize := self.len - self.index
	freeSize := self.cap - bufSize
	if freeSize <= 0 {
		return bufSize, nil
	}
	readConnSize := size - bufSize
	if freeSize < readConnSize {
		index := 0
		for ; self.index < self.len; self.index++ {
			self.buf[index] = self.buf[self.index]
			index++
		}
		self.index, self.len = 0, index
	}
	n, err := conn.Read(self.buf[self.len:self.cap])
	if err != nil {
		return bufSize, err
	}
	self.len += n
	readConnSize -= n
	if readConnSize > 0 {
		for readConnSize > 0 {
			n, err = conn.Read(self.buf[self.len:self.cap])
			if err != nil {
				return self.len - self.index, err
			}
			self.len += n
			readConnSize -= n
		}
	}
	return self.len - self.index, nil
}

type Stream struct {
	conn         net.Conn
	readerBuffer *StreamReaderBuffer
	closed       bool
	closedWait   chan bool
}

func NewStream(conn net.Conn) *Stream {
	stream := &Stream{conn, NewStreamReaderBuffer(4096), false, make(chan bool, 1)}
	return stream
}

func (self *Stream) ReadBytes(buf []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}
	bufLen := len(buf)
	if bufLen <= self.readerBuffer.GetSize() {
		n := self.readerBuffer.Read(buf)
		return n, nil
	} else if bufLen <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, bufLen)
		if err != nil {
			return 0, err
		}
		n := self.readerBuffer.Read(buf)
		return n, nil
	}

	n := self.readerBuffer.Read(buf)
	if n >= bufLen {
		return n, nil
	}
	cn, err := self.conn.Read(buf[n:])
	if err != nil {
		return n + cn, err
	}
	cn += n
	if cn < bufLen {
		for cn < bufLen {
			nn, nerr := self.conn.Read(buf[cn:])
			if nerr != nil {
				return cn + nn, nerr
			}
			cn += nn
		}
	}
	return cn, nil
}

func (self *Stream) ReadBytesSize(size int) ([]byte, error) {
	if self.closed {
		return nil, io.EOF
	}
	if size <= self.readerBuffer.GetSize() {
		buf := self.readerBuffer.ReadBytesSize(size)
		return buf, nil
	} else if size <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, size)
		if err != nil {
			return nil, err
		}
		buf := self.readerBuffer.ReadBytesSize(size)
		return buf, nil
	}

	buf := make([]byte, size)
	n := self.readerBuffer.Read(buf)
	if n >= size {
		return buf, nil
	}
	cn, err := self.conn.Read(buf[n:])
	if err != nil {
		return nil, err
	}
	cn += n
	if cn < size {
		for cn < size {
			nn, nerr := self.conn.Read(buf[cn:])
			if nerr != nil {
				return nil, nerr
			}
			cn += nn
		}
	}
	return buf, nil
}

func (self *Stream) ReadBytesFrame() ([]byte, error) {
	frameLen := 0
	if 4 <= self.readerBuffer.GetSize() {
		buf := self.readerBuffer.ReadBytesSize(4)
		if len(buf) != 4 {
			return nil, errors.New("read buf error")
		}
		frameLen = int(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
	} else if 4 <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, 4)
		if err != nil {
			return nil, err
		}
		buf := self.readerBuffer.ReadBytesSize(4)
		if len(buf) != 4 {
			return nil, errors.New("read buf error")
		}
		frameLen = int(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
	}
	buf := make([]byte, frameLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(frameLen), byte(frameLen>>8), byte(frameLen>>16), byte(frameLen>>24)
	if frameLen <= 0 {
		return buf, nil
	}
	_, err := self.ReadBytes(buf[4:])
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (self *Stream) ReadSize(size int) ([]byte, error) {
	if self.closed {
		return nil, io.EOF
	}
	if size <= self.readerBuffer.GetSize() {
		buf := self.readerBuffer.ReadBytesSize(size)
		return buf, nil
	} else if size <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, -1)
		if err != nil {
			return nil, err
		}
		bsize := self.readerBuffer.GetSize()
		if size > bsize {
			buf := self.readerBuffer.ReadBytesSize(bsize)
			return buf, nil
		}
		buf := self.readerBuffer.ReadBytesSize(size)
		return buf, nil
	}

	buf := make([]byte, size)
	n := self.readerBuffer.Read(buf)
	if n >= size {
		return buf, nil
	}
	cn, err := self.conn.Read(buf[n:])
	if err != nil {
		return nil, err
	}
	return buf[:n+cn], nil
}

func (self *Stream) Read(buf []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}
	bufLen := len(buf)
	if bufLen <= self.readerBuffer.GetSize() {
		n := self.readerBuffer.Read(buf)
		return n, nil
	} else if bufLen <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, -1)
		if err != nil {
			return 0, err
		}
		n := self.readerBuffer.Read(buf)
		return n, nil
	}

	n := self.readerBuffer.Read(buf)
	if n >= bufLen {
		return n, nil
	}
	cn, err := self.conn.Read(buf[n:])
	return n + cn, err
}

func (self *Stream) ReadFromConn(buf []byte) (int, error) {
	n := self.readerBuffer.Read(buf)
	if n >= len(buf) {
		return n, nil
	}
	cn, err := self.conn.Read(buf[n:])
	return n + cn, err
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
		for n < cn {
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
	close(self.closedWait)
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
