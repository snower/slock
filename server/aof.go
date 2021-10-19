package server

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const AOF_LOCK_TYPE_FILE = 0
const AOF_LOCK_TYPE_LOAD = 1
const AOF_LOCK_TYPE_ACK_FILE = 2
const AOF_LOCK_TYPE_ACK_ACKED = 3

const AOF_FLAG_REWRITEd = 0x0001
const AOF_FLAG_TIMEOUTED = 0x0002
const AOF_FLAG_EXPRIED = 0x0004
const AOF_FLAG_REQUIRE_ACKED = 0x1000

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
	requestId := [16]byte{}
	requestId[0], requestId[1], requestId[2], requestId[3], requestId[4], requestId[5], requestId[6], requestId[7] = byte(self.AofId), byte(self.AofId>>8), byte(self.AofId>>16), byte(self.AofId>>24), byte(self.AofIndex), byte(self.AofIndex>>8), byte(self.AofIndex>>16), byte(self.AofIndex>>24)
	requestId[8], requestId[9], requestId[10], requestId[11], requestId[12], requestId[13], requestId[14], requestId[15] = byte(self.CommandTime), byte(self.CommandTime>>8), byte(self.CommandTime>>16), byte(self.CommandTime>>24), byte(self.CommandTime>>32), byte(self.CommandTime>>40), byte(self.CommandTime>>48), byte(self.CommandTime>>56)
	return requestId
}

func (self *AofLock) SetRequestId(buf [16]byte) {
	self.AofId, self.AofIndex = uint32(buf[0])|uint32(buf[1])<<8|uint32(buf[2])<<16|uint32(buf[3])<<24, uint32(buf[4])|uint32(buf[5])<<8|uint32(buf[6])<<16|uint32(buf[7])<<24
	self.CommandTime = uint64(buf[8]) | uint64(buf[9])<<8 | uint64(buf[10])<<16 | uint64(buf[11])<<24 | uint64(buf[12])<<32 | uint64(buf[13])<<40 | uint64(buf[14])<<48 | uint64(buf[15])<<56
}

type AofFile struct {
	slock       *SLock
	aof         *Aof
	filename    string
	file        *os.File
	mode        int
	bufSize     int
	buf         []byte
	rbuf        *bufio.Reader
	wbuf        []byte
	windex      int
	size        int
	ackRequests [][]byte
	dirtied     bool
	ackIndex    int
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile {
	buf_size = buf_size - buf_size%64
	ackRequests := make([][]byte, buf_size/64)
	return &AofFile{aof.slock, aof, filename, nil, mode, buf_size,
		make([]byte, 64), nil, nil, 0, 0, ackRequests, false, 0}
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
		self.wbuf = make([]byte, self.bufSize)
		err = self.WriteHeader()
		if err != nil {
			_ = self.file.Close()
			return err
		}
	} else {
		self.rbuf = bufio.NewReaderSize(self.file, self.bufSize)
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

	headerLen := uint16(self.buf[10]) | uint16(self.buf[11])<<8
	if headerLen != 0x0000 {
		return errors.New("AOF File Header Len Error")
	}

	if headerLen > 0 {
		n, err := self.rbuf.Read(make([]byte, headerLen))
		if err != nil {
			return err
		}

		if n != int(headerLen) {
			return errors.New("File is not AOF FIle")
		}
	}

	self.size += 12 + int(headerLen)
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

	lockLen := uint16(buf[0]) | uint16(buf[1])<<8
	if n != int(lockLen)+2 {
		nn, nerr := self.rbuf.Read(buf[n:64])
		if nerr != nil {
			return err
		}
		n += nn
		if n != int(lockLen)+2 {
			return errors.New("Lock Len error")
		}
	}

	self.size += 2 + int(lockLen)
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

	lockLen := uint16(buf[0]) | uint16(buf[1])<<8
	if n != int(lockLen)+2 {
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
	if lock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		self.ackRequests[self.ackIndex] = self.wbuf[self.windex : self.windex+64]
		self.ackIndex++
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
			self.ackIndex = 0
			return err
		}
		tn += n
	}
	self.windex = 0
	self.dirtied = true

	for i := 0; i < self.ackIndex; i++ {
		_ = self.aof.lockAcked(self.ackRequests[i], true)
	}
	self.ackIndex = 0
	return nil
}

func (self *AofFile) Sync() error {
	if !self.dirtied {
		return nil
	}
	if self.file == nil {
		return errors.New("File Unopen")
	}

	err := self.file.Sync()
	if err != nil {
		return err
	}
	self.dirtied = false
	return nil
}

func (self *AofFile) Close() error {
	if self.windex > 0 {
		_ = self.Flush()
	}
	if self.dirtied {
		_ = self.Sync()
	}

	if self.ackIndex > 0 {
		for i := 0; i < self.ackIndex; i++ {
			_ = self.aof.lockAcked(self.ackRequests[i], false)
		}
		self.ackIndex = 0
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

type AofLockQueue struct {
	buffer []*AofLock
	rindex int
	windex int
	blen   int
	next   *AofLockQueue
}

type AofChannel struct {
	aof                     *Aof
	glock                   *sync.Mutex
	lockDb                  *LockDB
	lockDbGlock             *PriorityMutex
	queueHead               *AofLockQueue
	queueTail               *AofLockQueue
	queueCount              int
	queueWaiter             chan bool
	queueGlock              *sync.Mutex
	serverProtocol          ServerProtocol
	freeLocks               []*AofLock
	freeLockIndex           int
	freeLockMax             int
	lockDbGlockAcquiredSize int
	lockDbGlockAcquired     bool
	queuePulled             bool
	closed                  bool
	closedWaiter            chan bool
}

func NewAofChannel(aof *Aof, lockDb *LockDB, lockDbGlock *PriorityMutex) *AofChannel {
	freeLockMax := int(Config.AofQueueSize) / 128
	return &AofChannel{aof, &sync.Mutex{}, lockDb, lockDbGlock, nil, nil,
		0, make(chan bool, 1), &sync.Mutex{}, nil, make([]*AofLock, freeLockMax),
		0, freeLockMax, int(Config.AofQueueSize), false, false,
		false, make(chan bool, 1)}
}

func (self *AofChannel) pushAofLock(aofLock *AofLock) {
	if self.queueTail == nil {
		self.queueTail = self.aof.getLockQueue()
		self.queueHead = self.queueTail
	} else if self.queueTail.windex >= self.queueTail.blen {
		self.queueTail.next = self.aof.getLockQueue()
		self.queueTail = self.queueTail.next
	}
	self.queueTail.buffer[self.queueTail.windex] = aofLock
	self.queueTail.windex++
	self.queueCount++
	if !self.lockDbGlockAcquired && self.queueCount > self.lockDbGlockAcquiredSize {
		self.lockDbGlock.LowSetPriority()
		self.lockDbGlockAcquired = true
	}
	if self.queuePulled {
		self.queueWaiter <- true
		self.queuePulled = false
	}
}

func (self *AofChannel) pullAofLock() *AofLock {
	if self.queueHead == nil {
		return nil
	}
	if self.queueHead == self.queueTail && self.queueHead.rindex == self.queueHead.windex {
		return nil
	}
	aofLock := self.queueHead.buffer[self.queueHead.rindex]
	self.queueHead.buffer[self.queueHead.rindex] = nil
	self.queueHead.rindex++
	self.queueCount--
	if self.queueHead.rindex == self.queueHead.windex {
		if self.queueHead == self.queueTail {
			self.queueHead.rindex, self.queueHead.windex = 0, 0
		} else {
			queue := self.queueHead
			self.queueHead = queue.next
			self.aof.freeLockQueue(queue)
			if self.queueHead == nil {
				self.queueTail = nil
			}
		}
	}

	if self.lockDbGlockAcquired && self.queueCount < self.lockDbGlockAcquiredSize {
		self.lockDbGlock.LowUnSetPriority()
		self.lockDbGlockAcquired = false
	}
	return aofLock
}

func (self *AofChannel) Push(lock *Lock, commandType uint8, command *protocol.LockCommand, aofFlag uint16) error {
	if self.closed {
		return io.EOF
	}

	var aofLock *AofLock
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		aofLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
		aofLock = NewAofLock()
	}

	aofLock.CommandType = commandType
	aofLock.AofIndex = 0
	aofLock.AofId = 0
	if lock.expriedTime > self.lockDb.currentTime {
		aofLock.CommandTime = uint64(self.lockDb.currentTime)
	} else {
		aofLock.CommandTime = uint64(lock.expriedTime)
	}
	aofLock.Flag = lock.command.Flag
	aofLock.DbId = lock.manager.dbId
	aofLock.LockId = lock.command.LockId
	aofLock.LockKey = lock.command.LockKey
	aofLock.AofFlag = 0
	if aofLock.CommandTime-uint64(lock.startTime) > 0xffff {
		aofLock.StartTime = 0xffff
	} else {
		aofLock.StartTime = uint16(aofLock.CommandTime - uint64(lock.startTime))
	}
	aofLock.ExpriedFlag = lock.command.ExpriedFlag
	if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME == 0 {
		aofLock.ExpriedTime = uint16(uint64(lock.expriedTime) - aofLock.CommandTime)
	} else {
		aofLock.ExpriedTime = 0
	}
	if command == nil {
		aofLock.Count = lock.command.Count
		if commandType == protocol.COMMAND_UNLOCK {
			aofLock.Rcount = 0
		} else {
			aofLock.Rcount = lock.command.Rcount
		}
	} else {
		aofLock.Count = command.Count
		aofLock.Rcount = command.Rcount
	}
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 {
		aofLock.AofFlag |= AOF_FLAG_REQUIRE_ACKED
		aofLock.lock = lock
	} else {
		aofLock.lock = nil
	}
	aofLock.AofFlag |= aofFlag

	aofLock.HandleType = AOF_LOCK_TYPE_FILE

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) Load(lock *AofLock) error {
	if self.closed {
		return io.EOF
	}

	if self.lockDbGlock.lowPriority == 1 {
		self.lockDbGlock.LowPriorityLock()
		self.lockDbGlock.LowPriorityUnlock()
	}
	var aofLock *AofLock
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		aofLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
		aofLock = NewAofLock()
	}

	copy(aofLock.buf, lock.buf)
	aofLock.HandleType = AOF_LOCK_TYPE_LOAD

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) AofAcked(buf []byte, succed bool) error {
	if self.closed {
		return io.EOF
	}

	var aofLock *AofLock
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		aofLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
		aofLock = NewAofLock()
	}

	copy(aofLock.buf, buf)
	if succed {
		aofLock.Result = protocol.RESULT_SUCCED
	} else {
		aofLock.Result = protocol.RESULT_ERROR
	}
	aofLock.HandleType = AOF_LOCK_TYPE_ACK_FILE

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) Acked(commandResult *protocol.LockResultCommand) error {
	if self.closed {
		return io.EOF
	}

	if self.lockDbGlock.lowPriority == 1 {
		self.lockDbGlock.LowPriorityLock()
		self.lockDbGlock.LowPriorityUnlock()
	}
	var aofLock *AofLock
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		aofLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
		aofLock = NewAofLock()
	}

	aofLock.CommandType = commandResult.CommandType
	aofLock.SetRequestId(commandResult.RequestId)
	aofLock.Flag = commandResult.Flag
	aofLock.DbId = commandResult.DbId
	aofLock.LockId = commandResult.LockId
	aofLock.LockKey = commandResult.LockKey
	aofLock.AofFlag = 0
	aofLock.StartTime = 0
	aofLock.ExpriedFlag = 0
	aofLock.ExpriedTime = 0
	aofLock.Count = commandResult.Count
	aofLock.Rcount = commandResult.Rcount
	aofLock.Result = commandResult.Result
	aofLock.Lcount = commandResult.Lcount
	aofLock.Lrcount = commandResult.Lrcount
	aofLock.HandleType = AOF_LOCK_TYPE_ACK_ACKED

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) Run() {
	self.aof.handeLockAofChannel(self)
	for {
		self.queueGlock.Lock()
		aofLock := self.pullAofLock()
		for aofLock != nil {
			self.queueGlock.Unlock()
			self.Handle(aofLock)
			self.queueGlock.Lock()
			aofLock = self.pullAofLock()
		}
		self.queuePulled = true
		self.queueGlock.Unlock()

		self.aof.waitLockAofChannel(self)
		if self.closed {
			self.queueGlock.Lock()
			if self.lockDbGlockAcquired {
				self.lockDbGlock.LowUnSetPriority()
				self.lockDbGlockAcquired = false
			}
			self.queueGlock.Unlock()
			self.aof.syncFileAofChannel(self)
			_ = self.serverProtocol.Close()
			self.aof.RemoveAofChannel(self)
			close(self.closedWaiter)
			return
		}

		select {
		case <-self.queueWaiter:
			self.aof.handeLockAofChannel(self)
		case <-time.After(200 * time.Millisecond):
			self.aof.syncFileAofChannel(self)
			<-self.queueWaiter
			self.aof.handeLockAofChannel(self)
		}
	}
}

func (self *AofChannel) Handle(aofLock *AofLock) {
	switch aofLock.HandleType {
	case AOF_LOCK_TYPE_FILE:
		self.HandleLock(aofLock)
	case AOF_LOCK_TYPE_LOAD:
		self.HandleLoad(aofLock)
	case AOF_LOCK_TYPE_ACK_FILE:
		self.HandleAofAcked(aofLock)
	case AOF_LOCK_TYPE_ACK_ACKED:
		self.HandleAcked(aofLock)
	}

	self.glock.Lock()
	if self.freeLockIndex < self.freeLockMax {
		self.freeLocks[self.freeLockIndex] = aofLock
		self.freeLockIndex++
	}
	self.glock.Unlock()
}

func (self *AofChannel) HandleLock(aofLock *AofLock) {
	err := aofLock.Encode()
	if err != nil {
		self.aof.slock.Log().Errorf("Aof push lock encode error %v", err)
		if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && aofLock.CommandType == protocol.COMMAND_LOCK && aofLock.lock != nil {
			lockManager := aofLock.lock.manager
			lockManager.lockDb.DoAckLock(aofLock.lock, false)
		}
		return
	}
	self.aof.PushLock(aofLock)
}

func (self *AofChannel) HandleLoad(aofLock *AofLock) {
	err := aofLock.Decode()
	if err != nil {
		return
	}

	expriedTime := uint16(0)
	if aofLock.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME == 0 {
		expriedTime = uint16(int64(aofLock.CommandTime+uint64(aofLock.ExpriedTime)) - self.lockDb.currentTime)
	}

	lockCommand := self.serverProtocol.GetLockCommand()
	lockCommand.CommandType = aofLock.CommandType
	lockCommand.RequestId = aofLock.GetRequestId()
	lockCommand.Flag = aofLock.Flag | 0x04
	lockCommand.DbId = aofLock.DbId
	lockCommand.LockId = aofLock.LockId
	lockCommand.LockKey = aofLock.LockKey
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		lockCommand.TimeoutFlag = protocol.TIMEOUT_FLAG_REQUIRE_ACKED
	} else {
		lockCommand.TimeoutFlag = 0
	}
	lockCommand.Timeout = 3
	lockCommand.ExpriedFlag = aofLock.ExpriedFlag
	lockCommand.Expried = expriedTime + 1
	lockCommand.Count = aofLock.Count
	lockCommand.Rcount = aofLock.Rcount

	err = self.serverProtocol.ProcessLockCommand(lockCommand)
	if err == nil {
		return
	}
	self.aof.slock.Log().Errorf("Aof load lock Processlockcommand error %v", err)
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		_ = self.aof.lockLoaded(self.serverProtocol.(*MemWaiterServerProtocol), lockCommand, protocol.RESULT_ERROR, 0, 0)
	}
}

func (self *AofChannel) HandleAofAcked(aofLock *AofLock) {
	err := aofLock.Decode()
	if err != nil {
		return
	}

	if self.aof.slock.state == STATE_LEADER {
		db := self.aof.slock.replicationManager.GetAckDB(aofLock.DbId)
		if db != nil {
			_ = db.ProcessAofed(aofLock)
		}
		return
	}

	db := self.aof.slock.replicationManager.GetOrNewAckDB(aofLock.DbId)
	if db != nil {
		_ = db.ProcessAckAofed(aofLock)
	}
}

func (self *AofChannel) HandleAcked(aofLock *AofLock) {
	db := self.aof.slock.replicationManager.GetAckDB(aofLock.DbId)
	if db != nil {
		_ = db.Process(aofLock)
	}
}

type Aof struct {
	slock              *SLock
	glock              *sync.Mutex
	dataDir            string
	aofFileIndex       uint32
	aofFile            *AofFile
	aofGlock           *sync.Mutex
	replGlock          *sync.Mutex
	channels           []*AofChannel
	channelCount       uint32
	channelActiveCount uint32
	channelFlushWaiter chan bool
	freeLockQueues     []*AofLockQueue
	freeLockQueueGlock *sync.Mutex
	freeLockQueueIndex int
	rewritedWaiter     chan bool
	rewriteSize        uint32
	aofLockCount       uint64
	aofId              uint32
	isRewriting        bool
	inited             bool
	closed             bool
}

func NewAof() *Aof {
	return &Aof{nil, &sync.Mutex{}, "", 0, nil, &sync.Mutex{}, &sync.Mutex{},
		make([]*AofChannel, 0), 0, 0, nil, make([]*AofLockQueue, 256),
		&sync.Mutex{}, 0, nil, 0, 0, 0, false, false, false}
}

func (self *Aof) Init() error {
	self.rewriteSize = uint32(Config.AofFileRewriteSize)
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return err
	}

	self.dataDir = dataDir
	if _, err := os.Stat(self.dataDir); os.IsNotExist(err) {
		return err
	}
	self.slock.Log().Infof("Aof config data dir %s", self.dataDir)

	_ = self.WaitFlushAofChannel()
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	return nil
}

func (self *Aof) LoadAndInit() error {
	self.rewriteSize = uint32(Config.AofFileRewriteSize)
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return err
	}

	self.dataDir = dataDir
	if _, err := os.Stat(self.dataDir); os.IsNotExist(err) {
		return err
	}
	self.slock.Log().Infof("Aof config data dir %s", self.dataDir)

	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	if len(appendFiles) > 0 {
		aofFileIndex, err := strconv.ParseInt(appendFiles[len(appendFiles)-1][11:], 10, 64)
		if err != nil {
			return err
		}
		self.aofFileIndex = uint32(aofFileIndex)
	}

	self.aofFile = NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex+1)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aofFile.Open()
	if err != nil {
		return err
	}
	self.aofFileIndex++
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aofFileIndex)

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	err = self.LoadAofFiles(aofFilenames, time.Now().Unix(), func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		err := self.LoadLock(lock)
		if err != nil {
			return true, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	self.slock.Log().Infof("Aof loaded files %v", aofFilenames)

	_ = self.WaitFlushAofChannel()
	if len(appendFiles) > 0 {
		go self.rewriteAofFiles()
	}
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	return nil
}

func (self *Aof) LoadMaxId() ([16]byte, error) {
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return [16]byte{}, err
	}

	self.dataDir = dataDir
	if _, err := os.Stat(self.dataDir); os.IsNotExist(err) {
		return [16]byte{}, err
	}

	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return [16]byte{}, err
	}

	aofLock := NewAofLock()
	aofLock.AofIndex = 1
	fileAofId := aofLock.GetRequestId()
	if len(appendFiles) > 0 {
		aofFileIndex, err := strconv.ParseInt(appendFiles[len(appendFiles)-1][11:], 10, 64)
		if err == nil {
			aofLock.AofIndex = uint32(aofFileIndex)
			aofLock.AofId = 0
			fileAofId = aofLock.GetRequestId()
		}
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	for i := len(aofFilenames) - 1; i >= 0; i-- {
		aofFile := NewAofFile(self, filepath.Join(self.dataDir, aofFilenames[i]), os.O_RDONLY, int(Config.AofFileBufferSize))
		err := aofFile.Open()
		if err != nil {
			return fileAofId, err
		}
		err = aofFile.ReadTail(aofLock)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return fileAofId, err
		}

		err = aofLock.Decode()
		if err != nil {
			return fileAofId, err
		}
		return aofLock.GetRequestId(), nil
	}
	return fileAofId, nil
}

func (self *Aof) GetCurrentAofID() [16]byte {
	if !self.inited {
		return [16]byte{}
	}

	aofLock := NewAofLock()
	aofLock.AofIndex = self.aofFileIndex
	aofLock.AofId = self.aofId
	return aofLock.GetRequestId()
}

func (self *Aof) FindAofFiles() ([]string, string, error) {
	appendFiles := make([]string, 0)
	rewriteFile := ""
	aofIndexs := make(map[uint32]string)
	maxAofIndex, minAofIndex := uint32(0), uint32(0xffffffff)

	err := filepath.Walk(self.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		fileName := info.Name()
		if len(fileName) >= 11 && fileName[:10] == "append.aof" {
			aofIndex, err := strconv.ParseInt(fileName[11:], 10, 64)
			if err == nil {
				aofIndexs[uint32(aofIndex)] = fileName
				if uint32(aofIndex) > maxAofIndex {
					maxAofIndex = uint32(aofIndex)
				}
				if uint32(aofIndex) < minAofIndex {
					minAofIndex = uint32(aofIndex)
				}
			}
		} else if fileName == "rewrite.aof" {
			rewriteFile = fileName
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	if minAofIndex != 0xffffffff && maxAofIndex-minAofIndex >= 0x7fffffff {
		for i := maxAofIndex; i > 0; i++ {
			if fileName, ok := aofIndexs[i]; ok {
				appendFiles = append(appendFiles, fileName)
			} else {
				return nil, "", errors.New("append.aof file index error")
			}
		}
		for i := uint32(0); i <= minAofIndex; i++ {
			if fileName, ok := aofIndexs[i]; ok {
				appendFiles = append(appendFiles, fileName)
			} else {
				return nil, "", errors.New("append.aof file index error")
			}
		}
	} else {
		for i := minAofIndex; i <= maxAofIndex; i++ {
			if fileName, ok := aofIndexs[i]; ok {
				appendFiles = append(appendFiles, fileName)
			} else {
				return nil, "", errors.New("append.aof file index error")
			}
		}
	}

	return appendFiles, rewriteFile, nil
}

func (self *Aof) LoadAofFiles(filenames []string, expriedTime int64, iterFunc func(string, *AofFile, *AofLock, bool) (bool, error)) error {
	lock := NewAofLock()

	for _, filename := range filenames {
		err := self.LoadAofFile(filename, lock, expriedTime, iterFunc)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func (self *Aof) LoadAofFile(filename string, lock *AofLock, expriedTime int64, iterFunc func(string, *AofFile, *AofLock, bool) (bool, error)) error {
	aofFile := NewAofFile(self, filepath.Join(self.dataDir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
	err := aofFile.Open()
	if err != nil {
		return err
	}

	firstLock := true
	for {
		err := aofFile.ReadLock(lock)
		if err == io.EOF {
			err := aofFile.Close()
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

		if lock.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME == 0 {
			if int64(lock.CommandTime+uint64(lock.ExpriedTime)) <= expriedTime {
				continue
			}
		}

		isStop, iterErr := iterFunc(filename, aofFile, lock, firstLock)
		if iterErr != nil {
			return iterErr
		}

		if !isStop {
			return io.EOF
		}
		firstLock = false
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

	if self.aofFile != nil {
		self.aofGlock.Lock()
		_ = self.aofFile.Close()
		self.aofFile = nil
		self.aofGlock.Unlock()
	}
	self.slock.logger.Infof("Aof closed")
}

func (self *Aof) NewAofChannel(lockDb *LockDB, lockDbGlock *PriorityMutex) *AofChannel {
	self.glock.Lock()
	serverProtocol := NewMemWaiterServerProtocol(self.slock)
	aofChannel := NewAofChannel(self, lockDb, lockDbGlock)
	_ = serverProtocol.SetResultCallback(self.lockLoaded)
	aofChannel.serverProtocol = serverProtocol
	self.channels = append(self.channels, aofChannel)
	self.channelCount++
	self.glock.Unlock()
	go aofChannel.Run()
	return aofChannel
}

func (self *Aof) CloseAofChannel(aofChannel *AofChannel) *AofChannel {
	aofChannel.queueGlock.Lock()
	aofChannel.closed = true
	if aofChannel.queuePulled {
		aofChannel.queueWaiter <- false
		aofChannel.queuePulled = false
	}
	aofChannel.queueGlock.Unlock()
	return aofChannel
}

func (self *Aof) RemoveAofChannel(aofChannel *AofChannel) *AofChannel {
	self.glock.Lock()
	channels := make([]*AofChannel, 0)
	for _, c := range self.channels {
		if c != aofChannel {
			channels = append(channels, c)
		}
	}
	self.channels = channels
	self.channelCount = uint32(len(channels))
	self.glock.Unlock()
	return aofChannel
}

func (self *Aof) handeLockAofChannel(_ *AofChannel) {
	atomic.AddUint32(&self.channelActiveCount, 1)
}

func (self *Aof) waitLockAofChannel(_ *AofChannel) {
	atomic.AddUint32(&self.channelActiveCount, 0xffffffff)
	if !atomic.CompareAndSwapUint32(&self.channelActiveCount, 0, 0) {
		return
	}

	self.aofGlock.Lock()
	if self.aofFile.windex > 0 && self.aofFile.ackIndex > 0 {
		err := self.aofFile.Flush()
		if err != nil {
			self.slock.Log().Errorf("Aof flush file error %v", err)
		}
	}
	if self.channelFlushWaiter != nil {
		close(self.channelFlushWaiter)
		self.channelFlushWaiter = nil
	}
	self.aofGlock.Unlock()
}

func (self *Aof) syncFileAofChannel(_ *AofChannel) {
	if !atomic.CompareAndSwapUint32(&self.channelActiveCount, 0, 0) {
		return
	}

	self.aofGlock.Lock()
	if self.aofFile.windex > 0 || self.aofFile.dirtied {
		self.Flush()
	}
	self.aofGlock.Unlock()
}

func (self *Aof) WaitFlushAofChannel() error {
	var channelFlushWaiter chan bool
	self.aofGlock.Lock()
	if self.channelFlushWaiter == nil {
		channelFlushWaiter = make(chan bool, 1)
		self.channelFlushWaiter = channelFlushWaiter
	} else {
		channelFlushWaiter = self.channelFlushWaiter
	}
	self.aofGlock.Unlock()

	if atomic.CompareAndSwapUint32(&self.channelActiveCount, 0, 0) {
		self.aofGlock.Lock()
		if channelFlushWaiter == self.channelFlushWaiter {
			self.channelFlushWaiter = nil
		}
		self.aofGlock.Unlock()
		return nil
	}

	<-channelFlushWaiter
	return nil
}

func (self *Aof) LoadLock(lock *AofLock) error {
	db := self.slock.dbs[lock.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lock.DbId)
	}

	fashHash := (uint32(lock.LockKey[0])<<24 | uint32(lock.LockKey[1])<<16 | uint32(lock.LockKey[2])<<8 | uint32(lock.LockKey[3])) ^ (uint32(lock.LockKey[4])<<24 | uint32(lock.LockKey[5])<<16 | uint32(lock.LockKey[6])<<8 | uint32(lock.LockKey[7])) ^ (uint32(lock.LockKey[8])<<24 | uint32(lock.LockKey[9])<<16 | uint32(lock.LockKey[10])<<8 | uint32(lock.LockKey[11])) ^ (uint32(lock.LockKey[12])<<24 | uint32(lock.LockKey[13])<<16 | uint32(lock.LockKey[14])<<8 | uint32(lock.LockKey[15]))
	aofChannel := db.aofChannels[fashHash%uint32(db.managerMaxGlocks)]
	return aofChannel.Load(lock)
}

func (self *Aof) loadLockAck(lockResult *protocol.LockResultCommand) error {
	db := self.slock.dbs[lockResult.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lockResult.DbId)
	}

	fashHash := (uint32(lockResult.LockKey[0])<<24 | uint32(lockResult.LockKey[1])<<16 | uint32(lockResult.LockKey[2])<<8 | uint32(lockResult.LockKey[3])) ^ (uint32(lockResult.LockKey[4])<<24 | uint32(lockResult.LockKey[5])<<16 | uint32(lockResult.LockKey[6])<<8 | uint32(lockResult.LockKey[7])) ^ (uint32(lockResult.LockKey[8])<<24 | uint32(lockResult.LockKey[9])<<16 | uint32(lockResult.LockKey[10])<<8 | uint32(lockResult.LockKey[11])) ^ (uint32(lockResult.LockKey[12])<<24 | uint32(lockResult.LockKey[13])<<16 | uint32(lockResult.LockKey[14])<<8 | uint32(lockResult.LockKey[15]))
	aofChannel := db.aofChannels[fashHash%uint32(db.managerMaxGlocks)]
	return aofChannel.Acked(lockResult)
}

func (self *Aof) PushLock(lock *AofLock) {
	self.aofGlock.Lock()
	self.aofId++
	_ = lock.UpdateAofIndexId(self.aofFileIndex, self.aofId)

	werr := self.aofFile.WriteLock(lock)
	if werr != nil {
		self.slock.Log().Errorf("Aof append file write error %v", werr)
	}
	if uint32(self.aofFile.GetSize()) >= self.rewriteSize {
		_ = self.RewriteAofFile()
	}
	self.replGlock.Lock()
	self.aofGlock.Unlock()
	perr := self.slock.replicationManager.PushLock(lock)
	if perr != nil {
		self.slock.Log().Errorf("Aof push ring buffer queue error %v", perr)
	}
	self.replGlock.Unlock()

	if werr != nil || perr != nil {
		if lock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && lock.CommandType == protocol.COMMAND_LOCK && lock.lock != nil {
			lockManager := lock.lock.manager
			lockManager.lockDb.DoAckLock(lock.lock, false)
		}
	}
	atomic.AddUint64(&self.aofLockCount, 1)
}

func (self *Aof) AppendLock(lock *AofLock) {
	self.aofGlock.Lock()
	if lock.AofIndex != self.aofFileIndex || self.aofFile == nil {
		self.aofFileIndex = lock.AofIndex - 1
		self.aofId = lock.AofId
		_ = self.RewriteAofFile()
	}

	err := self.aofFile.WriteLock(lock)
	if err != nil {
		self.slock.Log().Errorf("Aof append file write error %v", err)
	}
	self.aofId = lock.AofId
	self.aofGlock.Unlock()
	atomic.AddUint64(&self.aofLockCount, 1)
}

func (self *Aof) lockAcked(buf []byte, succed bool) error {
	aofId := uint32(buf[3]) | uint32(buf[4])<<8 | uint32(buf[5])<<16 | uint32(buf[6])<<24
	db := self.slock.dbs[buf[20]]
	if db == nil {
		return nil
	}
	aofChannel := db.aofChannels[aofId%uint32(db.managerMaxGlocks)]
	if aofChannel.closed {
		return nil
	}
	return aofChannel.AofAcked(buf, succed)
}

func (self *Aof) lockLoaded(_ *MemWaiterServerProtocol, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.slock.state == STATE_FOLLOWER {
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED == 0 {
			return nil
		}

		db := self.slock.replicationManager.GetOrNewAckDB(command.DbId)
		if db != nil {
			return db.ProcessAcked(command, result, lcount, lrcount)
		}
		return nil
	}
	return nil
}

func (self *Aof) Flush() {
	err := self.aofFile.Flush()
	if err != nil {
		self.slock.Log().Errorf("Aof flush file error %v", err)
		return
	}
	err = self.aofFile.Sync()
	if err != nil {
		self.slock.Log().Errorf("Aof Sync file error %v", err)
		return
	}
}

func (self *Aof) OpenAofFile(aofIndex uint32) (*AofFile, error) {
	if aofIndex == 0 {
		aofFile := NewAofFile(self, filepath.Join(self.dataDir, "rewrite.aof"), os.O_WRONLY, int(Config.AofFileBufferSize))
		err := aofFile.Open()
		if err != nil {
			return nil, err
		}
		return aofFile, nil
	}

	aofFile := NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", aofIndex)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := aofFile.Open()
	if err != nil {
		return nil, err
	}
	return aofFile, nil
}

func (self *Aof) Reset(aofFileIndex uint32) error {
	defer self.aofGlock.Unlock()
	self.aofGlock.Lock()
	if self.isRewriting {
		return errors.New("Aof Rewriting")
	}

	if self.aofFile != nil {
		self.Flush()

		err := self.aofFile.Close()
		if err != nil {
			self.slock.Log().Errorf("Aof close file %s.%d error %v", "append.aof", self.aofFileIndex, err)
			return err
		}
		self.aofFile = nil
	}

	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	if rewriteFile != "" {
		err := os.Remove(filepath.Join(self.dataDir, rewriteFile))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", rewriteFile, err)
			return err
		}
	}

	for _, appendFile := range appendFiles {
		err := os.Remove(filepath.Join(self.dataDir, appendFile))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", appendFile, err)
			return err
		}
	}

	self.aofFileIndex = aofFileIndex
	self.aofId = 0
	self.aofFile = NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex+1)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aofFile.Open()
	if err != nil {
		return err
	}
	self.aofFileIndex++
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aofFileIndex)
	return nil
}

func (self *Aof) RewriteAofFile() error {
	if self.aofFile != nil {
		self.Flush()

		err := self.aofFile.Close()
		if err != nil {
			self.slock.Log().Errorf("Aof close file %s.%d error %v", "append.aof", self.aofFileIndex, err)
		}
	}

	aofFilename := fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex+1)
	aofFile := NewAofFile(self, filepath.Join(self.dataDir, aofFilename), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := aofFile.Open()
	if err != nil {
		self.slock.Log().Infof("Aof open current file %s.%d error %v", "append.aof", self.aofFileIndex, err)
		return err
	}
	self.aofFile = aofFile
	self.aofFileIndex++
	self.aofId = 0
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aofFileIndex)

	go self.rewriteAofFiles()
	return nil
}

func (self *Aof) WaitRewriteAofFiles() error {
	self.glock.Lock()
	if !self.isRewriting {
		self.glock.Unlock()
		return nil
	}

	rewritedWaiter := self.rewritedWaiter
	if rewritedWaiter == nil {
		rewritedWaiter = make(chan bool, 1)
		self.rewritedWaiter = rewritedWaiter
	}
	self.glock.Unlock()
	<-rewritedWaiter
	return nil
}

func (self *Aof) rewriteAofFiles() {
	self.glock.Lock()
	if self.isRewriting {
		self.glock.Unlock()
		return
	}
	self.isRewriting = true
	self.glock.Unlock()

	defer func() {
		self.glock.Lock()
		self.isRewriting = false
		if self.rewritedWaiter != nil {
			close(self.rewritedWaiter)
			self.rewritedWaiter = nil
		}
		self.glock.Unlock()
	}()

	aofFilenames, err := self.findRewriteAofFiles()
	if err != nil || len(aofFilenames) == 0 {
		return
	}

	rewriteAofFile, aofFiles, err := self.loadRewriteAofFiles(aofFilenames)
	if err != nil {
		return
	}

	self.clearRewriteAofFiles(aofFilenames)
	totalAofSize := len(aofFilenames)*12 - len(aofFiles)*12
	for _, aofFile := range aofFiles {
		totalAofSize += aofFile.GetSize()
	}
	self.slock.Log().Infof("Aof rewrite file size %d to %d", totalAofSize, rewriteAofFile.GetSize())
}

func (self *Aof) findRewriteAofFiles() ([]string, error) {
	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return nil, err
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	for _, appendFile := range appendFiles {
		aofFileIndex, err := strconv.ParseInt(appendFile[11:], 10, 64)
		if err != nil {
			continue
		}

		if uint32(aofFileIndex) >= self.aofFileIndex {
			if uint32(aofFileIndex)-self.aofFileIndex < 0x7fffffff {
				continue
			}
		}
		aofFilenames = append(aofFilenames, appendFile)
	}
	return aofFilenames, nil
}

func (self *Aof) loadRewriteAofFiles(aofFilenames []string) (*AofFile, []*AofFile, error) {
	rewriteAofFile := NewAofFile(self, filepath.Join(self.dataDir, "rewrite.aof.tmp"), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := rewriteAofFile.Open()
	if err != nil {
		self.slock.Log().Infof("Aof open current file rewrite.aof.tmp error %v", err)
		return nil, nil, err
	}

	lockCommand := &protocol.LockCommand{}
	aofFiles := make([]*AofFile, 0)
	expriedTime := time.Now().Unix()
	if self.slock.state != STATE_LEADER {
		expriedTime -= 300
	}
	lerr := self.LoadAofFiles(aofFilenames, expriedTime, func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		db := self.slock.GetDB(lock.DbId)
		if db == nil {
			return true, nil
		}

		lockCommand.CommandType = lock.CommandType
		lockCommand.DbId = lock.DbId
		lockCommand.LockId = lock.LockId
		lockCommand.LockKey = lock.LockKey
		if !db.HasLock(lockCommand) {
			return true, nil
		}

		lock.AofFlag |= AOF_FLAG_REWRITEd
		lock.buf[55] |= AOF_FLAG_REWRITEd
		err = rewriteAofFile.WriteLock(lock)
		if err != nil {
			return true, err
		}

		if firstLock {
			aofFiles = append(aofFiles, aofFile)
		}
		return true, nil
	})
	if lerr != nil {
		self.slock.Log().Errorf("Aof load and rewrite file error %v", err)
	}

	err = rewriteAofFile.Flush()
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite flush file error %v", err)
	}

	err = rewriteAofFile.Close()
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite close file error %v", err)
	}
	return rewriteAofFile, aofFiles, lerr
}

func (self *Aof) clearRewriteAofFiles(aofFilenames []string) {
	for _, aofFilename := range aofFilenames {
		err := os.Remove(filepath.Join(self.dataDir, aofFilename))
		if err != nil {
			self.slock.Log().Errorf("Aof rewrite remove file error %s %v", aofFilename, err)
			continue
		}
		self.slock.Log().Infof("Aof rewrite remove file %s", aofFilename)
	}
	err := os.Rename(filepath.Join(self.dataDir, "rewrite.aof.tmp"), filepath.Join(self.dataDir, "rewrite.aof"))
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite rename rewrite.aof.tmp to rewrite.aof error %v", err)
	}
}

func (self *Aof) clearAofFiles() error {
	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return err
	}

	err = filepath.Walk(self.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		fileName := info.Name()
		if fileName == rewriteFile {
			return nil
		}

		for _, appendFile := range appendFiles {
			if appendFile == fileName {
				return nil
			}
		}

		err = os.Remove(filepath.Join(self.dataDir, fileName))
		if err != nil {
			self.slock.Log().Errorf("Aof clear remove file error %s %v", fileName, err)
		}
		return nil
	})
	return err
}

func (self *Aof) getLockQueue() *AofLockQueue {
	self.freeLockQueueGlock.Lock()
	if self.freeLockQueueIndex > 0 {
		self.freeLockQueueIndex--
		queue := self.freeLockQueues[self.freeLockQueueIndex]
		self.freeLockQueueGlock.Unlock()
		return queue
	}

	bufSize := int(Config.AofQueueSize) * 2
	queue := &AofLockQueue{make([]*AofLock, bufSize), 0, 0, bufSize, nil}
	self.freeLockQueueGlock.Unlock()
	return queue
}

func (self *Aof) freeLockQueue(queue *AofLockQueue) {
	self.freeLockQueueGlock.Lock()
	if self.freeLockQueueIndex >= 256 {
		self.freeLockQueueGlock.Unlock()
		return
	}

	queue.rindex, queue.windex = 0, 0
	queue.next = nil
	self.freeLockQueues[self.freeLockQueueIndex] = queue
	self.freeLockQueueIndex++
	self.freeLockQueueGlock.Unlock()
}
