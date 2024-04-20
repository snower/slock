package server

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"
)

var clientId uint64 = 0

const (
	STREAM_TYPE_NORMAL  uint8 = 0
	STREAM_TYPE_AOF     uint8 = 1
	STREAM_TYPE_ARBITER uint8 = 2
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
		index := self.index + size
		copy(buf, self.buf[self.index:index])
		self.index = index
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
		index := self.index + size
		buf := self.buf[self.index:index]
		self.index = index
		return buf
	}
	buf := self.buf[self.index:self.len]
	self.index, self.len = 0, 0
	return buf
}

func (self *StreamReaderBuffer) ReadFromConn(conn net.Conn, size int) (int, error) {
	bufSize := self.len - self.index
	freeSize := self.cap - bufSize
	if bufSize <= 0 {
		if size < 0 {
			n, err := conn.Read(self.buf)
			if err != nil {
				return bufSize, err
			}
			self.index, self.len = 0, n
			return n, nil
		}
		self.index, self.len = 0, 0
	} else if freeSize <= 0 || size < 0 {
		return bufSize, nil
	}
	readConnSize := size - bufSize
	if freeSize < readConnSize && self.index > 0 {
		index := 0
		for ; self.index < self.len; self.index++ {
			self.buf[index] = self.buf[self.index]
			index++
		}
		self.index, self.len = 0, index
	}
	n, err := conn.Read(self.buf[self.len:])
	if err != nil {
		return bufSize, err
	}
	self.len += n
	readConnSize -= n
	for readConnSize > 0 {
		n, err = conn.Read(self.buf[self.len:])
		if err != nil {
			return self.len - self.index, err
		}
		self.len += n
		readConnSize -= n
	}
	return self.len - self.index, nil
}

type Stream struct {
	conn         net.Conn
	protocol     ServerProtocol
	readerBuffer *StreamReaderBuffer
	startTime    *time.Time
	streamId     uint64
	streamType   uint8
	closed       bool
	closedWaiter chan bool
	nextStream   *Stream
	lastStream   *Stream
}

func NewStream(conn net.Conn) *Stream {
	now := time.Now()
	stream := &Stream{conn, nil, NewStreamReaderBuffer(4096),
		&now, atomic.AddUint64(&clientId, 1), STREAM_TYPE_NORMAL, false,
		make(chan bool, 1), nil, nil}
	return stream
}

func (self *Stream) ReadBytes(buf []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}
	bufLen := len(buf)
	if bufLen <= self.readerBuffer.GetSize() {
		index := self.readerBuffer.index + bufLen
		copy(buf, self.readerBuffer.buf[self.readerBuffer.index:index])
		self.readerBuffer.index = index
		return bufLen, nil
	} else if bufLen <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, bufLen)
		if err != nil {
			return 0, err
		}
		index := self.readerBuffer.index + bufLen
		copy(buf, self.readerBuffer.buf[self.readerBuffer.index:index])
		self.readerBuffer.index = index
		return bufLen, nil
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
	for cn < bufLen {
		nn, nerr := self.conn.Read(buf[cn:])
		if nerr != nil {
			return cn + nn, nerr
		}
		cn += nn
	}
	return cn, nil
}

func (self *Stream) ReadBytesSize(size int) ([]byte, error) {
	if self.closed {
		return nil, io.EOF
	}
	if size <= self.readerBuffer.GetSize() {
		index := self.readerBuffer.index + size
		buf := self.readerBuffer.buf[self.readerBuffer.index:index]
		self.readerBuffer.index = index
		return buf, nil
	} else if size <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, size)
		if err != nil {
			return nil, err
		}
		index := self.readerBuffer.index + size
		buf := self.readerBuffer.buf[self.readerBuffer.index:index]
		self.readerBuffer.index = index
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
	for cn < size {
		nn, nerr := self.conn.Read(buf[cn:])
		if nerr != nil {
			return nil, nerr
		}
		cn += nn
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
	if self.readerBuffer.GetSize() == 0 {
		cn, err := self.conn.Read(buf[4:])
		if err != nil {
			return nil, err
		}
		cn += 4
		for cn < frameLen {
			nn, nerr := self.conn.Read(buf[cn:])
			if nerr != nil {
				return nil, nerr
			}
			cn += nn
		}
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
		index := self.readerBuffer.index + size
		buf := self.readerBuffer.buf[self.readerBuffer.index:index]
		self.readerBuffer.index = index
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

	bsize := self.readerBuffer.GetSize()
	if bsize > 0 {
		buf := self.readerBuffer.ReadBytesSize(bsize)
		return buf, nil
	}
	buf := make([]byte, size)
	n, err := self.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (self *Stream) Read(buf []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}
	bufLen := len(buf)
	if bufLen <= self.readerBuffer.GetSize() {
		index := self.readerBuffer.index + bufLen
		copy(buf, self.readerBuffer.buf[self.readerBuffer.index:index])
		self.readerBuffer.index = index
		return bufLen, nil
	} else if bufLen <= self.readerBuffer.GetCapSize() {
		_, err := self.readerBuffer.ReadFromConn(self.conn, -1)
		if err != nil {
			return 0, err
		}
		n := self.readerBuffer.Read(buf)
		return n, nil
	}

	n := self.readerBuffer.Read(buf)
	if n > 0 {
		return n, nil
	}
	return self.conn.Read(buf[n:])
}

func (self *Stream) ReadFromConn(buf []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}
	n := self.readerBuffer.Read(buf)
	if n > 0 {
		return n, nil
	}
	return self.conn.Read(buf)
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
	for n < cn {
		nn, nerr := self.conn.Write(b[n:])
		if nerr != nil {
			return nerr
		}
		n += nn
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
	self.protocol = nil
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
