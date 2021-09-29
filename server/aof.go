package server

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var request_id_index uint64 = 0

const AOF_LOCK_TYPE_FILE = 0
const AOF_LOCK_TYPE_LOAD = 1
const AOF_LOCK_TYPE_ACK_FILE = 2
const AOF_LOCK_TYPE_ACK_ACKED = 3

type AofLock struct {
	HandleType  uint8
	CommandType uint8
	AofIndex    uint32
	AofId       uint32
	CommandTime uint64
	Flag        uint8
	DbId        uint8
	LockId      [16]byte
	LockKey     [16]byte
	AofFlag     uint16
	StartTime   uint16
	ExpriedFlag uint16
	ExpriedTime uint16
	Count       uint16
	Rcount      uint8
	Result      uint8
	Lcount      uint16
	Lrcount     uint8
	buf         []byte
	lock        *Lock
}

func NewAofLock() *AofLock {
	return &AofLock{0, 0, 0, 0, 0, 0, 0, [16]byte{},
		[16]byte{}, 0, 0, 0, 0, 0, 0, 0,
		0, 0, make([]byte, 64), nil}

}

func (self *AofLock) GetBuf() []byte {
	return self.buf
}

func (self *AofLock) Decode() error {
	buf := self.buf
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	self.CommandType = buf[2]

	self.AofId, self.AofIndex = uint32(buf[3])|uint32(buf[4])<<8|uint32(buf[5])<<16|uint32(buf[6])<<24, uint32(buf[7])|uint32(buf[8])<<8|uint32(buf[9])<<16|uint32(buf[10])<<24
	self.CommandTime = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

	self.Flag, self.DbId = buf[19], buf[20]

	self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
		self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15] =
		buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
		buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

	self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
		self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15] =
		buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
		buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

	self.StartTime, self.AofFlag, self.ExpriedTime, self.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8

	self.Count = uint16(buf[61]) | uint16(buf[62])<<8
	self.Rcount = buf[63]

	return nil
}

func (self *AofLock) Encode() error {
	buf := self.buf
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	buf[2] = self.CommandType

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.AofId), byte(self.AofId>>8), byte(self.AofId>>16), byte(self.AofId>>24), byte(self.AofIndex), byte(self.AofIndex>>8), byte(self.AofIndex>>16), byte(self.AofIndex>>24)
	buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.CommandTime), byte(self.CommandTime>>8), byte(self.CommandTime>>16), byte(self.CommandTime>>24), byte(self.CommandTime>>32), byte(self.CommandTime>>40), byte(self.CommandTime>>48), byte(self.CommandTime>>56)

	buf[19], buf[20] = self.Flag, self.DbId

	buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
		buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36] =
		self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
		self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15]

	buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
		buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52] =
		self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
		self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15]

	buf[53], buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60] = byte(self.StartTime), byte(self.StartTime>>8), byte(self.AofFlag), byte(self.AofFlag>>8), byte(self.ExpriedTime), byte(self.ExpriedTime>>8), byte(self.ExpriedFlag), byte(self.ExpriedFlag>>8)

	buf[61], buf[62] = byte(self.Count), byte(self.Count>>8)
	buf[63] = self.Rcount

	return nil
}

func (self *AofLock) UpdateAofIndexId(aof_index uint32, aof_id uint32) error {
	self.AofIndex = aof_index
	self.AofId = aof_id

	buf := self.buf
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(aof_id), byte(aof_id>>8), byte(aof_id>>16), byte(aof_id>>24), byte(aof_index), byte(aof_index>>8), byte(aof_index>>16), byte(aof_index>>24)
	return nil
}

func (self *AofLock) GetRequestId() [16]byte {
	request_id := [16]byte{}
	request_id[0], request_id[1], request_id[2], request_id[3], request_id[4], request_id[5], request_id[6], request_id[7] = byte(self.AofId), byte(self.AofId>>8), byte(self.AofId>>16), byte(self.AofId>>24), byte(self.AofIndex), byte(self.AofIndex>>8), byte(self.AofIndex>>16), byte(self.AofIndex>>24)
	request_id[8], request_id[9], request_id[10], request_id[11], request_id[12], request_id[13], request_id[14], request_id[15] = byte(self.CommandTime), byte(self.CommandTime>>8), byte(self.CommandTime>>16), byte(self.CommandTime>>24), byte(self.CommandTime>>32), byte(self.CommandTime>>40), byte(self.CommandTime>>48), byte(self.CommandTime>>56)
	return request_id
}

func (self *AofLock) SetRequestId(buf [16]byte) {
	self.AofId, self.AofIndex = uint32(buf[0])|uint32(buf[1])<<8|uint32(buf[2])<<16|uint32(buf[3])<<24, uint32(buf[4])|uint32(buf[5])<<8|uint32(buf[6])<<16|uint32(buf[7])<<24
	self.CommandTime = uint64(buf[8]) | uint64(buf[9])<<8 | uint64(buf[10])<<16 | uint64(buf[11])<<24 | uint64(buf[12])<<32 | uint64(buf[13])<<40 | uint64(buf[14])<<48 | uint64(buf[15])<<56
}

type AofFile struct {
	slock        *SLock
	aof          *Aof
	filename     string
	file         *os.File
	mode         int
	buf_size     int
	buf          []byte
	rbuf         *bufio.Reader
	wbuf         []byte
	windex       int
	size         int
	ack_requests [][]byte
	ack_index    int
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile {
	buf_size = buf_size - buf_size%64
	ack_requests := make([][]byte, buf_size/64)
	return &AofFile{aof.slock, aof, filename, nil, mode, buf_size,
		make([]byte, 64), nil, nil, 0, 0, ack_requests, 0}
}

func (self *AofFile) Open() error {
	mode := self.mode
	if mode == os.O_WRONLY {
		mode |= os.O_CREATE
		mode |= os.O_TRUNC
	}
	file, err := os.OpenFile(self.filename, mode, 0644)
	if err != nil {
		return err
	}

	self.file = file
	if self.mode == os.O_WRONLY {
		self.wbuf = make([]byte, self.buf_size)
		err = self.WriteHeader()
		if err != nil {
			_ = self.file.Close()
			return err
		}
	} else {
		self.rbuf = bufio.NewReaderSize(self.file, self.buf_size)
		err = self.ReadHeader()
		if err != nil {
			_ = self.file.Close()
			return err
		}
	}
	return nil
}

func (self *AofFile) ReadHeader() error {
	n, err := self.rbuf.Read(self.buf[:12])
	if err != nil {
		return err
	}

	if n != 12 {
		return errors.New("File is not AOF FIle")
	}

	if string(self.buf[:8]) != "SLOCKAOF" {
		return errors.New("File is not AOF File")
	}

	version := uint16(self.buf[8]) | uint16(self.buf[9])<<8
	if version != 0x0001 {
		return errors.New("AOF File Unknown Version")
	}

	header_len := uint16(self.buf[10]) | uint16(self.buf[11])<<8
	if header_len != 0x0000 {
		return errors.New("AOF File Header Len Error")
	}

	if header_len > 0 {
		n, err := self.rbuf.Read(make([]byte, header_len))
		if err != nil {
			return err
		}

		if n != int(header_len) {
			return errors.New("File is not AOF FIle")
		}
	}

	self.size += 12 + int(header_len)
	return nil
}

func (self *AofFile) WriteHeader() error {
	self.buf[0], self.buf[1], self.buf[2], self.buf[3], self.buf[4], self.buf[5], self.buf[6], self.buf[7] = 'S', 'L', 'O', 'C', 'K', 'A', 'O', 'F'
	self.buf[8], self.buf[9], self.buf[10], self.buf[11] = 0x01, 0x00, 0x00, 0x00
	n, err := self.file.Write(self.buf[:12])
	if n != 12 || err != nil {
		return errors.New("write header error")
	}

	self.size += 12
	return nil
}

func (self *AofFile) ReadLock(lock *AofLock) error {
	if self.file == nil {
		return errors.New("File Unopen")
	}

	buf := lock.GetBuf()
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	n, err := self.rbuf.Read(buf)
	if err != nil {
		return err
	}

	lock_len := uint16(buf[0]) | uint16(buf[1])<<8
	if n != int(lock_len)+2 {
		nn, nerr := self.rbuf.Read(buf[n:64])
		if nerr != nil {
			return err
		}
		n += nn
		if n != int(lock_len)+2 {
			return errors.New("Lock Len error")
		}
	}

	self.size += 2 + int(lock_len)
	return nil
}

func (self *AofFile) ReadTail(lock *AofLock) error {
	if self.file == nil {
		return errors.New("File Unopen")
	}

	buf := lock.GetBuf()
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	stat, err := self.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() < 76 {
		return io.EOF
	}

	_, _ = self.file.Seek(64, os.SEEK_END)
	n, err := self.rbuf.Read(buf)
	if err != nil {
		_, _ = self.file.Seek(12, os.SEEK_SET)
		return err
	}

	lock_len := uint16(buf[0]) | uint16(buf[1])<<8
	if n != int(lock_len)+2 {
		_, _ = self.file.Seek(12, os.SEEK_SET)
		return errors.New("Lock Len error")
	}
	_, _ = self.file.Seek(12, os.SEEK_SET)
	return nil
}

func (self *AofFile) WriteLock(lock *AofLock) error {
	if self.file == nil {
		return errors.New("File Unopen")
	}

	buf := lock.GetBuf()
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}
	buf[0], buf[1] = 62, 0

	copy(self.wbuf[self.windex:], buf)
	if lock.AofFlag&0x1000 != 0 {
		self.ack_requests[self.ack_index] = self.wbuf[self.windex : self.windex+64]
		self.ack_index++
	}
	self.windex += 64
	if self.windex >= len(self.wbuf) {
		err := self.Flush()
		if err != nil {
			return err
		}
	}
	self.size += 64
	return nil
}

func (self *AofFile) Flush() error {
	if self.windex == 0 {
		return nil
	}
	if self.file == nil {
		return errors.New("File Unopen")
	}

	tn := 0
	for tn < self.windex {
		n, err := self.file.Write(self.wbuf[tn:self.windex])
		if err != nil {
			self.windex = 0
			self.ack_index = 0
			return err
		}
		tn += n
	}
	self.windex = 0

	err := self.file.Sync()
	if err != nil {
		self.ack_index = 0
		return err
	}

	for i := 0; i < self.ack_index; i++ {
		_ = self.aof.LockAcked(self.ack_requests[i], true)
	}
	self.ack_index = 0
	return nil
}

func (self *AofFile) Close() error {
	if self.ack_index > 0 {
		for i := 0; i < self.ack_index; i++ {
			_ = self.aof.LockAcked(self.ack_requests[i], false)
		}
		self.ack_index = 0
	}

	if self.file == nil {
		return errors.New("File Unopen")
	}

	err := self.file.Close()
	if err == nil {
		self.file = nil
		self.wbuf = nil
		self.rbuf = nil
	}
	return err
}

func (self *AofFile) GetSize() int {
	return self.size
}

type AofChannel struct {
	slock           *SLock
	glock           *sync.Mutex
	aof             *Aof
	lock_db         *LockDB
	channel         chan *AofLock
	server_protocol ServerProtocol
	free_locks      []*AofLock
	free_lock_index int32
	free_lock_max   int32
	closed          bool
	closed_waiter   chan bool
}

func (self *AofChannel) Push(lock *Lock, command_type uint8, command *protocol.LockCommand) error {
	if self.closed {
		return io.EOF
	}

	var aof_lock *AofLock = nil
	self.glock.Lock()
	if self.free_lock_index > 0 {
		self.free_lock_index--
		aof_lock = self.free_locks[self.free_lock_index]
	}
	self.glock.Unlock()

	if aof_lock == nil {
		aof_lock = NewAofLock()
	}
	aof_lock.CommandType = command_type
	aof_lock.AofIndex = 0
	aof_lock.AofId = 0
	if lock.expried_time > self.lock_db.current_time {
		aof_lock.CommandTime = uint64(self.lock_db.current_time)
	} else {
		aof_lock.CommandTime = uint64(lock.expried_time)
	}
	aof_lock.Flag = lock.command.Flag
	aof_lock.DbId = lock.manager.db_id
	aof_lock.LockId = lock.command.LockId
	aof_lock.LockKey = lock.command.LockKey
	aof_lock.AofFlag = 0
	if aof_lock.CommandTime-uint64(lock.start_time) > 0xffff {
		aof_lock.StartTime = 0xffff
	} else {
		aof_lock.StartTime = uint16(aof_lock.CommandTime - uint64(lock.start_time))
	}
	aof_lock.ExpriedFlag = lock.command.ExpriedFlag
	if lock.command.ExpriedFlag&0x4000 == 0 {
		aof_lock.ExpriedTime = uint16(uint64(lock.expried_time) - aof_lock.CommandTime)
	} else {
		aof_lock.ExpriedTime = 0
	}
	if command == nil {
		aof_lock.Count = lock.command.Count
		if command_type == protocol.COMMAND_UNLOCK {
			aof_lock.Rcount = 0
		} else {
			aof_lock.Rcount = lock.command.Rcount
		}
	} else {
		aof_lock.Count = command.Count
		aof_lock.Rcount = command.Rcount
	}
	if lock.command.TimeoutFlag&0x1000 != 0 {
		aof_lock.AofFlag |= 0x1000
		aof_lock.lock = lock
	} else {
		aof_lock.lock = nil
	}

	aof_lock.HandleType = AOF_LOCK_TYPE_FILE
	self.channel <- aof_lock
	return nil
}

func (self *AofChannel) Load(lock *AofLock) error {
	if self.closed {
		return io.EOF
	}

	var aof_lock *AofLock = nil
	self.glock.Lock()
	if self.free_lock_index > 0 {
		self.free_lock_index--
		aof_lock = self.free_locks[self.free_lock_index]
	}
	self.glock.Unlock()

	if aof_lock == nil {
		aof_lock = NewAofLock()
	}
	copy(aof_lock.buf, lock.buf)
	aof_lock.HandleType = AOF_LOCK_TYPE_LOAD
	self.channel <- aof_lock
	return nil
}

func (self *AofChannel) AofAcked(buf []byte, succed bool) error {
	if self.closed {
		return io.EOF
	}

	var aof_lock *AofLock = nil
	self.glock.Lock()
	if self.free_lock_index > 0 {
		self.free_lock_index--
		aof_lock = self.free_locks[self.free_lock_index]
	}
	self.glock.Unlock()

	if aof_lock == nil {
		aof_lock = NewAofLock()
	}
	copy(aof_lock.buf, buf)
	if succed {
		aof_lock.Result = protocol.RESULT_SUCCED
	} else {
		aof_lock.Result = protocol.RESULT_ERROR
	}
	aof_lock.HandleType = AOF_LOCK_TYPE_ACK_FILE
	self.channel <- aof_lock
	return nil
}

func (self *AofChannel) Acked(command_result *protocol.LockResultCommand) error {
	if self.closed {
		return io.EOF
	}

	var aof_lock *AofLock = nil
	self.glock.Lock()
	if self.free_lock_index > 0 {
		self.free_lock_index--
		aof_lock = self.free_locks[self.free_lock_index]
	}
	self.glock.Unlock()

	if aof_lock == nil {
		aof_lock = NewAofLock()
	}
	aof_lock.CommandType = command_result.CommandType
	aof_lock.SetRequestId(command_result.RequestId)
	aof_lock.Flag = command_result.Flag
	aof_lock.DbId = command_result.DbId
	aof_lock.LockId = command_result.LockId
	aof_lock.LockKey = command_result.LockKey
	aof_lock.AofFlag = 0
	aof_lock.StartTime = 0
	aof_lock.ExpriedFlag = 0
	aof_lock.ExpriedTime = 0
	aof_lock.Count = command_result.Count
	aof_lock.Rcount = command_result.Rcount
	aof_lock.Result = command_result.Result
	aof_lock.Lcount = command_result.Lcount
	aof_lock.Lrcount = command_result.Lrcount
	aof_lock.HandleType = AOF_LOCK_TYPE_ACK_ACKED
	self.channel <- aof_lock
	return nil
}

func (self *AofChannel) Run() {
	exited := false
	self.aof.ActiveAofChannel(self)
	for {
		select {
		case aof_lock := <-self.channel:
			if aof_lock != nil {
				self.Handle(aof_lock)
				continue
			}
			exited = self.closed
		default:
			self.aof.UnActiveAofChannel(self)
			if exited {
				_ = self.server_protocol.Close()
				self.aof.RemoveAofChannel(self)
				close(self.closed_waiter)
				return
			}

			aof_lock := <-self.channel
			self.aof.ActiveAofChannel(self)
			if aof_lock != nil {
				self.Handle(aof_lock)
				continue
			}
			exited = self.closed
		}
	}
}

func (self *AofChannel) Handle(aof_lock *AofLock) {
	switch aof_lock.HandleType {
	case AOF_LOCK_TYPE_FILE:
		self.HandleLock(aof_lock)
	case AOF_LOCK_TYPE_LOAD:
		self.HandleLoad(aof_lock)
	case AOF_LOCK_TYPE_ACK_FILE:
		self.HandleAofAcked(aof_lock)
	case AOF_LOCK_TYPE_ACK_ACKED:
		self.HandleAcked(aof_lock)
	}

	self.glock.Lock()
	if self.free_lock_index < self.free_lock_max {
		self.free_locks[self.free_lock_index] = aof_lock
		self.free_lock_index++
	}
	self.glock.Unlock()
}

func (self *AofChannel) HandleLock(aof_lock *AofLock) {
	err := aof_lock.Encode()
	if err != nil {
		self.slock.Log().Errorf("Aof push lock encode error %v", err)
		if aof_lock.AofFlag&0x1000 != 0 && aof_lock.CommandType == protocol.COMMAND_LOCK && aof_lock.lock != nil {
			lock_manager := aof_lock.lock.manager
			lock_manager.lock_db.DoAckLock(aof_lock.lock, false)
		}
		return
	}
	self.aof.PushLock(aof_lock)
}

func (self *AofChannel) HandleLoad(aof_lock *AofLock) {
	err := aof_lock.Decode()
	if err != nil {
		return
	}

	expried_time := uint16(0)
	if aof_lock.ExpriedFlag&0x4000 == 0 {
		expried_time = uint16(int64(aof_lock.CommandTime+uint64(aof_lock.ExpriedTime)) - self.lock_db.current_time)
	}

	lock_command := self.server_protocol.GetLockCommand()
	lock_command.CommandType = aof_lock.CommandType
	lock_command.RequestId = aof_lock.GetRequestId()
	lock_command.Flag = aof_lock.Flag | 0x04
	lock_command.DbId = aof_lock.DbId
	lock_command.LockId = aof_lock.LockId
	lock_command.LockKey = aof_lock.LockKey
	if aof_lock.AofFlag&0x1000 != 0 {
		lock_command.TimeoutFlag = 0x1000
	} else {
		lock_command.TimeoutFlag = 0
	}
	lock_command.Timeout = 3
	lock_command.ExpriedFlag = aof_lock.ExpriedFlag
	lock_command.Expried = expried_time + 1
	lock_command.Count = aof_lock.Count
	lock_command.Rcount = aof_lock.Rcount

	err = self.server_protocol.ProcessLockCommand(lock_command)
	if err == nil {
		return
	}
	self.slock.Log().Errorf("Aof load lock Processlockcommand error %v", err)
	if aof_lock.AofFlag&0x1000 != 0 {
		_ = self.aof.LockLoaded(self.server_protocol.(*MemWaiterServerProtocol), lock_command, protocol.RESULT_ERROR, 0, 0)
	}
}

func (self *AofChannel) HandleAofAcked(aof_lock *AofLock) {
	err := aof_lock.Decode()
	if err != nil {
		return
	}

	if self.slock.state == STATE_LEADER {
		db := self.slock.replication_manager.GetAckDB(aof_lock.DbId)
		if db != nil {
			_ = db.ProcessAofed(aof_lock)
		}
		return
	}

	db := self.slock.replication_manager.GetOrNewAckDB(aof_lock.DbId)
	if db != nil {
		_ = db.ProcessAckAofed(aof_lock)
	}
}

func (self *AofChannel) HandleAcked(aof_lock *AofLock) {
	db := self.slock.replication_manager.GetAckDB(aof_lock.DbId)
	if db != nil {
		_ = db.Process(aof_lock)
	}
}

type Aof struct {
	slock                 *SLock
	glock                 *sync.Mutex
	data_dir              string
	aof_file_index        uint32
	aof_file              *AofFile
	aof_glock             *sync.Mutex
	repl_glock            *sync.Mutex
	channels              []*AofChannel
	channel_count         uint32
	actived_channel_count uint32
	channel_flush_waiter  chan bool
	rewrited_waiter       chan bool
	rewrite_size          uint32
	aof_lock_count        uint64
	aof_id                uint32
	is_rewriting          bool
	inited                bool
	closed                bool
}

func NewAof() *Aof {
	return &Aof{nil, &sync.Mutex{}, "", 0, nil, &sync.Mutex{}, &sync.Mutex{},
		make([]*AofChannel, 0), 0, 0, nil, nil, 0, 0, 0,
		false, false, false}
}

func (self *Aof) Init() error {
	self.rewrite_size = uint32(Config.AofFileRewriteSize)
	data_dir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return err
	}

	self.data_dir = data_dir
	if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
		return err
	}
	self.slock.Log().Infof("Aof config data dir %s", self.data_dir)

	_ = self.WaitFlushAofChannel()
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	return nil
}

func (self *Aof) LoadAndInit() error {
	self.rewrite_size = uint32(Config.AofFileRewriteSize)
	data_dir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return err
	}

	self.data_dir = data_dir
	if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
		return err
	}
	self.slock.Log().Infof("Aof config data dir %s", self.data_dir)

	append_files, rewrite_file, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	if len(append_files) > 0 {
		aof_file_index, err := strconv.Atoi(append_files[len(append_files)-1][11:])
		if err != nil {
			return err
		}
		self.aof_file_index = uint32(aof_file_index)
	}

	self.aof_file = NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index+1)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aof_file.Open()
	if err != nil {
		return err
	}
	self.aof_file_index++
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aof_file_index)

	aof_filenames := make([]string, 0)
	if rewrite_file != "" {
		aof_filenames = append(aof_filenames, rewrite_file)
	}
	aof_filenames = append(aof_filenames, append_files...)
	err = self.LoadAofFiles(aof_filenames, func(filename string, aof_file *AofFile, lock *AofLock, first_lock bool) (bool, error) {
		err := self.LoadLock(lock)
		if err != nil {
			return true, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	self.slock.Log().Infof("Aof loaded files %v", aof_filenames)

	_ = self.WaitFlushAofChannel()
	if len(append_files) > 0 {
		go self.RewriteAofFiles()
	}
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	return nil
}

func (self *Aof) LoadMaxId() ([16]byte, error) {
	data_dir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return [16]byte{}, err
	}

	self.data_dir = data_dir
	if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
		return [16]byte{}, err
	}

	append_files, rewrite_file, err := self.FindAofFiles()
	if err != nil {
		return [16]byte{}, err
	}

	aof_lock := NewAofLock()
	aof_lock.AofIndex = 1
	file_aof_id := aof_lock.GetRequestId()
	if len(append_files) > 0 {
		aof_file_index, err := strconv.Atoi(append_files[len(append_files)-1][11:])
		if err == nil {
			aof_lock.AofIndex = uint32(aof_file_index)
			aof_lock.AofId = 0
			file_aof_id = aof_lock.GetRequestId()
		}
	}

	aof_filenames := make([]string, 0)
	if rewrite_file != "" {
		aof_filenames = append(aof_filenames, rewrite_file)
	}
	aof_filenames = append(aof_filenames, append_files...)
	for i := len(aof_filenames) - 1; i >= 0; i-- {
		aof_file := NewAofFile(self, filepath.Join(self.data_dir, aof_filenames[i]), os.O_RDONLY, int(Config.AofFileBufferSize))
		err := aof_file.Open()
		if err != nil {
			return file_aof_id, err
		}
		err = aof_file.ReadTail(aof_lock)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return file_aof_id, err
		}

		err = aof_lock.Decode()
		if err != nil {
			return file_aof_id, err
		}
		return aof_lock.GetRequestId(), nil
	}
	return file_aof_id, nil
}

func (self *Aof) GetCurrentAofID() [16]byte {
	if !self.inited {
		return [16]byte{}
	}

	aof_lock := NewAofLock()
	aof_lock.AofIndex = self.aof_file_index
	aof_lock.AofId = self.aof_id
	return aof_lock.GetRequestId()
}

func (self *Aof) FindAofFiles() ([]string, string, error) {
	append_files := make([]string, 0)
	rewrite_file := ""

	err := filepath.Walk(self.data_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file_name := info.Name()
		if len(file_name) >= 11 && file_name[:10] == "append.aof" {
			_, err := strconv.Atoi(file_name[11:])
			if err == nil {
				append_files = append(append_files, file_name)
			}
		} else if file_name == "rewrite.aof" {
			rewrite_file = file_name
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	sort.Strings(append_files)
	return append_files, rewrite_file, nil
}

func (self *Aof) LoadAofFiles(filenames []string, iter_func func(string, *AofFile, *AofLock, bool) (bool, error)) error {
	lock := NewAofLock()
	now := time.Now().Unix()

	for _, filename := range filenames {
		err := self.LoadAofFile(filename, lock, now, iter_func)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func (self *Aof) LoadAofFile(filename string, lock *AofLock, now int64, iter_func func(string, *AofFile, *AofLock, bool) (bool, error)) error {
	aof_file := NewAofFile(self, filepath.Join(self.data_dir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
	err := aof_file.Open()
	if err != nil {
		return err
	}

	first_lock := true
	for {
		err := aof_file.ReadLock(lock)
		if err == io.EOF {
			err := aof_file.Close()
			if err != nil {
				return err
			}
			return nil
		}

		if err != nil {
			return err
		}

		err = lock.Decode()
		if err != nil {
			return err
		}

		if lock.ExpriedFlag&0x4000 == 0 {
			if int64(lock.CommandTime+uint64(lock.ExpriedTime)) <= now {
				continue
			}
		}

		is_stop, iter_err := iter_func(filename, aof_file, lock, first_lock)
		if iter_err != nil {
			return iter_err
		}

		if !is_stop {
			return io.EOF
		}
		first_lock = false
	}
}

func (self *Aof) Close() {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return
	}
	self.closed = true
	self.glock.Unlock()

	_ = self.WaitFlushAofChannel()
	_ = self.WaitRewriteAofFiles()

	if self.aof_file != nil {
		self.aof_glock.Lock()
		self.aof_file.Close()
		self.aof_file = nil
		self.aof_glock.Unlock()
	}
	self.slock.logger.Infof("Aof closed")
}

func (self *Aof) NewAofChannel(lock_db *LockDB) *AofChannel {
	self.glock.Lock()
	server_protocol := NewMemWaiterServerProtocol(self.slock)
	aof_channel := &AofChannel{self.slock, &sync.Mutex{}, self, lock_db, make(chan *AofLock, Config.AofQueueSize),
		server_protocol, make([]*AofLock, Config.AofQueueSize+4), 0, int32(Config.AofQueueSize + 4),
		false, make(chan bool, 1)}
	_ = server_protocol.SetResultCallback(self.LockLoaded)
	self.channels = append(self.channels, aof_channel)
	self.channel_count++
	self.glock.Unlock()
	go aof_channel.Run()
	return aof_channel
}

func (self *Aof) CloseAofChannel(aof_channel *AofChannel) *AofChannel {
	self.glock.Lock()
	aof_channel.channel <- nil
	aof_channel.closed = true
	self.glock.Unlock()
	return aof_channel
}

func (self *Aof) RemoveAofChannel(aof_channel *AofChannel) *AofChannel {
	self.glock.Lock()
	channels := make([]*AofChannel, 0)
	for _, c := range self.channels {
		if c != aof_channel {
			channels = append(channels, c)
		}
	}
	self.channels = channels
	self.channel_count = uint32(len(channels))
	self.glock.Unlock()
	return aof_channel
}

func (self *Aof) ActiveAofChannel(channel *AofChannel) {
	atomic.AddUint32(&self.actived_channel_count, 1)
}

func (self *Aof) UnActiveAofChannel(channel *AofChannel) {
	atomic.AddUint32(&self.actived_channel_count, 0xffffffff)
	if !atomic.CompareAndSwapUint32(&self.actived_channel_count, 0, 0) {
		return
	}

	self.aof_glock.Lock()
	self.Flush()
	if self.channel_flush_waiter != nil {
		close(self.channel_flush_waiter)
		self.channel_flush_waiter = nil
	}
	self.aof_glock.Unlock()
}

func (self *Aof) WaitFlushAofChannel() error {
	self.aof_glock.Lock()
	self.channel_flush_waiter = make(chan bool, 1)
	if atomic.CompareAndSwapUint32(&self.actived_channel_count, 0, 0) {
		self.channel_flush_waiter = nil
		self.aof_glock.Unlock()
		return nil
	}
	self.aof_glock.Unlock()
	<-self.channel_flush_waiter
	return nil
}

func (self *Aof) LoadLock(lock *AofLock) error {
	db := self.slock.dbs[lock.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lock.DbId)
	}

	fash_hash := (uint32(lock.LockKey[0])<<24 | uint32(lock.LockKey[1])<<16 | uint32(lock.LockKey[2])<<8 | uint32(lock.LockKey[3])) ^ (uint32(lock.LockKey[4])<<24 | uint32(lock.LockKey[5])<<16 | uint32(lock.LockKey[6])<<8 | uint32(lock.LockKey[7])) ^ (uint32(lock.LockKey[8])<<24 | uint32(lock.LockKey[9])<<16 | uint32(lock.LockKey[10])<<8 | uint32(lock.LockKey[11])) ^ (uint32(lock.LockKey[12])<<24 | uint32(lock.LockKey[13])<<16 | uint32(lock.LockKey[14])<<8 | uint32(lock.LockKey[15]))
	aof_channel := db.aof_channels[fash_hash%uint32(db.manager_max_glocks)]
	return aof_channel.Load(lock)
}

func (self *Aof) LoadLockAck(lock_result *protocol.LockResultCommand) error {
	db := self.slock.dbs[lock_result.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lock_result.DbId)
	}

	fash_hash := (uint32(lock_result.LockKey[0])<<24 | uint32(lock_result.LockKey[1])<<16 | uint32(lock_result.LockKey[2])<<8 | uint32(lock_result.LockKey[3])) ^ (uint32(lock_result.LockKey[4])<<24 | uint32(lock_result.LockKey[5])<<16 | uint32(lock_result.LockKey[6])<<8 | uint32(lock_result.LockKey[7])) ^ (uint32(lock_result.LockKey[8])<<24 | uint32(lock_result.LockKey[9])<<16 | uint32(lock_result.LockKey[10])<<8 | uint32(lock_result.LockKey[11])) ^ (uint32(lock_result.LockKey[12])<<24 | uint32(lock_result.LockKey[13])<<16 | uint32(lock_result.LockKey[14])<<8 | uint32(lock_result.LockKey[15]))
	aof_channel := db.aof_channels[fash_hash%uint32(db.manager_max_glocks)]
	return aof_channel.Acked(lock_result)
}

func (self *Aof) PushLock(lock *AofLock) {
	self.aof_glock.Lock()
	self.aof_id++
	_ = lock.UpdateAofIndexId(self.aof_file_index, self.aof_id)

	werr := self.aof_file.WriteLock(lock)
	if werr != nil {
		self.slock.Log().Errorf("Aof append file write error %v", werr)
	}
	if uint32(self.aof_file.GetSize()) >= self.rewrite_size {
		_ = self.RewriteAofFile()
	}
	self.repl_glock.Lock()
	self.aof_glock.Unlock()
	perr := self.slock.replication_manager.PushLock(lock)
	if perr != nil {
		self.slock.Log().Errorf("Aof push ring buffer queue error %v", perr)
	}
	self.repl_glock.Unlock()

	if werr != nil || perr != nil {
		if lock.AofFlag&0x1000 != 0 && lock.CommandType == protocol.COMMAND_LOCK && lock.lock != nil {
			lock_manager := lock.lock.manager
			lock_manager.lock_db.DoAckLock(lock.lock, false)
		}
	}
	atomic.AddUint64(&self.aof_lock_count, 1)
}

func (self *Aof) AppendLock(lock *AofLock) {
	self.aof_glock.Lock()
	if lock.AofIndex != self.aof_file_index || self.aof_file == nil {
		self.aof_file_index = lock.AofIndex - 1
		self.aof_id = lock.AofId
		_ = self.RewriteAofFile()
	}

	err := self.aof_file.WriteLock(lock)
	if err != nil {
		self.slock.Log().Errorf("Aof append file write error %v", err)
	}
	self.aof_id = lock.AofId
	self.aof_glock.Unlock()
	atomic.AddUint64(&self.aof_lock_count, 1)
}

func (self *Aof) LockAcked(buf []byte, succed bool) error {
	aof_id := uint32(buf[3]) | uint32(buf[4])<<8 | uint32(buf[5])<<16 | uint32(buf[6])<<24
	db := self.slock.dbs[buf[20]]
	if db == nil {
		return nil
	}
	aof_channel := db.aof_channels[aof_id%uint32(db.manager_max_glocks)]
	if aof_channel.closed {
		return nil
	}
	return aof_channel.AofAcked(buf, succed)
}

func (self *Aof) LockLoaded(server_protocol *MemWaiterServerProtocol, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.slock.state == STATE_FOLLOWER {
		if command.TimeoutFlag&0x1000 == 0 {
			return nil
		}

		db := self.slock.replication_manager.GetOrNewAckDB(command.DbId)
		if db != nil {
			return db.ProcessAcked(command, result, lcount, lrcount)
		}
		return nil
	}
	return nil
}

func (self *Aof) Flush() {
	for !self.closed {
		err := self.aof_file.Flush()
		if err != nil {
			self.slock.Log().Errorf("Aof flush file error %v", err)
			time.Sleep(1e10)
		}
		break
	}
}

func (self *Aof) OpenAofFile(aof_index uint32) (*AofFile, error) {
	if aof_index == 0 {
		aof_file := NewAofFile(self, filepath.Join(self.data_dir, "rewrite.aof"), os.O_WRONLY, int(Config.AofFileBufferSize))
		err := aof_file.Open()
		if err != nil {
			return nil, err
		}
		return aof_file, nil
	}

	aof_file := NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", aof_index)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := aof_file.Open()
	if err != nil {
		return nil, err
	}
	return aof_file, nil
}

func (self *Aof) Reset(aof_file_index uint32) error {
	defer self.aof_glock.Unlock()
	self.aof_glock.Lock()
	if self.is_rewriting {
		return errors.New("Aof Rewriting")
	}

	if self.aof_file != nil {
		self.Flush()

		err := self.aof_file.Close()
		if err != nil {
			self.slock.Log().Errorf("Aof close file %s.%d error %v", "append.aof", self.aof_file_index, err)
			return err
		}
		self.aof_file = nil
	}

	append_files, rewrite_file, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	if rewrite_file != "" {
		err := os.Remove(filepath.Join(self.data_dir, rewrite_file))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", rewrite_file, err)
			return err
		}
	}

	for _, append_file := range append_files {
		err := os.Remove(filepath.Join(self.data_dir, append_file))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", append_file, err)
			return err
		}
	}

	self.aof_file_index = aof_file_index
	self.aof_id = 0
	self.aof_file = NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index+1)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aof_file.Open()
	if err != nil {
		return err
	}
	self.aof_file_index++
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aof_file_index)
	return nil
}

func (self *Aof) RewriteAofFile() error {
	if self.aof_file != nil {
		self.Flush()

		err := self.aof_file.Close()
		if err != nil {
			self.slock.Log().Errorf("Aof close file %s.%d error %v", "append.aof", self.aof_file_index, err)
		}
		self.aof_file = nil
	}

	aof_filename := "rewrite.aof"
	if self.aof_file_index > 0 {
		aof_filename = fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index+1)
	}
	aof_file := NewAofFile(self, filepath.Join(self.data_dir, aof_filename), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := aof_file.Open()
	if err != nil {
		self.slock.Log().Infof("Aof open current file %s.%d error %v", "append.aof", self.aof_file_index, err)
		return err
	}
	self.aof_file = aof_file
	self.aof_file_index++
	self.aof_id = 0
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aof_file_index)

	go self.RewriteAofFiles()
	return nil
}

func (self *Aof) WaitRewriteAofFiles() error {
	self.glock.Lock()
	if !self.is_rewriting {
		self.glock.Unlock()
		return nil
	}

	rewrited_waiter := self.rewrited_waiter
	if rewrited_waiter == nil {
		rewrited_waiter = make(chan bool, 1)
		self.rewrited_waiter = rewrited_waiter
	}
	self.glock.Unlock()
	<-rewrited_waiter
	return nil
}

func (self *Aof) RewriteAofFiles() {
	self.glock.Lock()
	if self.is_rewriting {
		self.glock.Unlock()
		return
	}
	self.is_rewriting = true
	self.glock.Unlock()

	defer func() {
		self.glock.Lock()
		self.is_rewriting = false
		if self.rewrited_waiter != nil {
			close(self.rewrited_waiter)
			self.rewrited_waiter = nil
		}
		self.glock.Unlock()
	}()

	aof_filenames, err := self.FindRewriteAofFiles()
	if err != nil || len(aof_filenames) == 0 {
		return
	}

	rewrite_aof_file, aof_files, err := self.LoadRewriteAofFiles(aof_filenames)
	if err != nil {
		return
	}

	self.ClearRewriteAofFiles(aof_filenames)
	total_aof_size := len(aof_filenames)*12 - len(aof_files)*12
	for _, aof_file := range aof_files {
		total_aof_size += aof_file.GetSize()
	}
	self.slock.Log().Infof("Aof rewrite file size %d to %d", total_aof_size, rewrite_aof_file.GetSize())
}

func (self *Aof) FindRewriteAofFiles() ([]string, error) {
	append_files, rewrite_file, err := self.FindAofFiles()
	if err != nil {
		return nil, err
	}

	aof_filenames := make([]string, 0)
	if rewrite_file != "" {
		aof_filenames = append(aof_filenames, rewrite_file)
	}
	for _, append_file := range append_files {
		aof_file_index, err := strconv.Atoi(append_file[11:])
		if err != nil {
			continue
		}

		if uint32(aof_file_index) >= self.aof_file_index {
			continue
		}
		aof_filenames = append(aof_filenames, append_file)
	}
	return aof_filenames, nil
}

func (self *Aof) LoadRewriteAofFiles(aof_filenames []string) (*AofFile, []*AofFile, error) {
	rewrite_aof_file := NewAofFile(self, filepath.Join(self.data_dir, "rewrite.aof.tmp"), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := rewrite_aof_file.Open()
	if err != nil {
		self.slock.Log().Infof("Aof open current file rewrite.aof.tmp error %v", err)
		return nil, nil, err
	}

	now := uint64(time.Now().Unix())
	lock_command := &protocol.LockCommand{}
	aof_files := make([]*AofFile, 0)
	aof_id := uint32(0)

	lerr := self.LoadAofFiles(aof_filenames, func(filename string, aof_file *AofFile, lock *AofLock, first_lock bool) (bool, error) {
		db := self.slock.GetDB(lock.DbId)
		if db == nil {
			return true, nil
		}

		lock_command.CommandType = lock.CommandType
		lock_command.DbId = lock.DbId
		lock_command.LockId = lock.LockId
		lock_command.LockKey = lock.LockKey
		if now-lock.CommandTime > 300 && !db.HasLock(lock_command) {
			return true, nil
		}

		aof_id++
		_ = lock.UpdateAofIndexId(0, aof_id)
		err = rewrite_aof_file.WriteLock(lock)
		if err != nil {
			return true, err
		}

		if first_lock {
			aof_files = append(aof_files, aof_file)
		}
		return true, nil
	})
	if lerr != nil {
		self.slock.Log().Errorf("Aof load and rewrite file error %v", err)
	}

	err = rewrite_aof_file.Flush()
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite flush file error %v", err)
	}

	err = rewrite_aof_file.Close()
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite close file error %v", err)
	}
	return rewrite_aof_file, aof_files, lerr
}

func (self *Aof) ClearRewriteAofFiles(aof_filenames []string) {
	for _, aof_filename := range aof_filenames {
		err := os.Remove(filepath.Join(self.data_dir, aof_filename))
		if err != nil {
			self.slock.Log().Errorf("Aof rewrite remove file error %s %v", aof_filename, err)
			continue
		}
		self.slock.Log().Infof("Aof rewrite remove file %s", aof_filename)
	}
	err := os.Rename(filepath.Join(self.data_dir, "rewrite.aof.tmp"), filepath.Join(self.data_dir, "rewrite.aof"))
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite rename rewrite.aof.tmp to rewrite.aof error %v", err)
	}
}

func (self *Aof) ClearAofFiles() error {
	append_files, rewrite_file, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	err = filepath.Walk(self.data_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file_name := info.Name()
		if file_name == rewrite_file {
			return nil
		}

		for _, append_file := range append_files {
			if append_file == file_name {
				return nil
			}
		}

		err = os.Remove(filepath.Join(self.data_dir, file_name))
		if err != nil {
			self.slock.Log().Errorf("Aof clear remove file error %s %v", file_name, err)
		}
		return nil
	})
	return err
}

func (self *Aof) GetRequestId() [16]byte {
	now := uint32(time.Now().Unix())
	request_id_index := atomic.AddUint64(&request_id_index, 1)
	return [16]byte{
		byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
		LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
	}
}
