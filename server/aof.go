package server

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const AOF_LOCK_TYPE_FILE = 0
const AOF_LOCK_TYPE_LOAD = 1
const AOF_LOCK_TYPE_REPLAY = 2
const AOF_LOCK_TYPE_ACK_FILE = 3
const AOF_LOCK_TYPE_ACK_ACKED = 4
const AOF_LOCK_TYPE_CONSISTENCY_BARRIER = 5

const AOF_FLAG_REWRITED = 0x0001
const AOF_FLAG_TIMEOUTED = 0x0002
const AOF_FLAG_EXPRIED = 0x0004
const AOF_FLAG_UPDATED = 0x0008
const AOF_FLAG_REQUIRE_ACKED = 0x1000
const AOF_FLAG_CONTAINS_DATA = 0x2000

func FormatAofId(aofId [16]byte) string {
	return fmt.Sprintf("%x", [16]byte{aofId[7], aofId[6], aofId[5], aofId[4], aofId[3], aofId[2], aofId[1], aofId[0], aofId[15], aofId[14], aofId[13], aofId[12], aofId[11], aofId[10], aofId[9], aofId[8]})
}

func ParseAofId(aofIdString string) ([16]byte, error) {
	if len(aofIdString) != 32 {
		return [16]byte{}, errors.New("len error")
	}
	buf, err := hex.DecodeString(aofIdString)
	if err != nil {
		return [16]byte{}, err
	}
	return [16]byte{buf[7], buf[6], buf[5], buf[4], buf[3], buf[2], buf[1], buf[0], buf[15], buf[14], buf[13], buf[12], buf[11], buf[10], buf[9], buf[8]}, nil
}

type AofLock struct {
	HandleType  uint8
	CommandType uint8
	AofIndex    uint32
	AofOffset   uint32
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
	data        []byte
	lock        *Lock
}

func NewAofLock() *AofLock {
	return &AofLock{0, 0, 0, 0, 0, 0, 0, [16]byte{},
		[16]byte{}, 0, 0, 0, 0, 0, 0, 0,
		0, 0, make([]byte, 64), nil, nil}

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

	self.AofOffset, self.AofIndex = uint32(buf[3])|uint32(buf[4])<<8|uint32(buf[5])<<16|uint32(buf[6])<<24, uint32(buf[7])|uint32(buf[8])<<8|uint32(buf[9])<<16|uint32(buf[10])<<24
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

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.AofOffset), byte(self.AofOffset>>8), byte(self.AofOffset>>16), byte(self.AofOffset>>24), byte(self.AofIndex), byte(self.AofIndex>>8), byte(self.AofIndex>>16), byte(self.AofIndex>>24)
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

func (self *AofLock) UpdateAofId(aofIndex uint32, aofOffset uint32) error {
	self.AofIndex = aofIndex
	self.AofOffset = aofOffset

	buf := self.buf
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(aofOffset), byte(aofOffset>>8), byte(aofOffset>>16), byte(aofOffset>>24), byte(aofIndex), byte(aofIndex>>8), byte(aofIndex>>16), byte(aofIndex>>24)
	return nil
}

func (self *AofLock) GetAofId() [16]byte {
	return [16]byte{byte(self.AofOffset), byte(self.AofOffset >> 8), byte(self.AofOffset >> 16), byte(self.AofOffset >> 24), byte(self.AofIndex), byte(self.AofIndex >> 8), byte(self.AofIndex >> 16), byte(self.AofIndex >> 24),
		byte(self.CommandTime), byte(self.CommandTime >> 8), byte(self.CommandTime >> 16), byte(self.CommandTime >> 24), byte(self.CommandTime >> 32), byte(self.CommandTime >> 40), byte(self.CommandTime >> 48), byte(self.CommandTime >> 56)}
}

func (self *AofLock) SetAofId(buf [16]byte) {
	self.AofOffset, self.AofIndex = uint32(buf[0])|uint32(buf[1])<<8|uint32(buf[2])<<16|uint32(buf[3])<<24, uint32(buf[4])|uint32(buf[5])<<8|uint32(buf[6])<<16|uint32(buf[7])<<24
	self.CommandTime = uint64(buf[8]) | uint64(buf[9])<<8 | uint64(buf[10])<<16 | uint64(buf[11])<<24 | uint64(buf[12])<<32 | uint64(buf[13])<<40 | uint64(buf[14])<<48 | uint64(buf[15])<<56
}

type AofFile struct {
	slock       *SLock
	aof         *Aof
	filename    string
	file        *os.File
	dataFile    *os.File
	mode        int
	bufSize     int
	rbuf        *bufio.Reader
	wbuf        []byte
	dlbuf       []byte
	drbuf       *bufio.Reader
	dwbuf       []byte
	windex      int
	dwindex     int
	size        int
	dataSize    int
	ackRequests [][]byte
	dirtied     bool
	ackIndex    int
}

func NewAofFile(aof *Aof, filename string, mode int, bufSize int) *AofFile {
	bufSize = bufSize - bufSize%64
	ackRequests := make([][]byte, bufSize/64)
	return &AofFile{aof.slock, aof, filename, nil, nil, mode, bufSize,
		nil, nil, make([]byte, 4), nil, nil, 0, 0, 0,
		0, ackRequests, false, 0}
}

func (self *AofFile) Open() error {
	mode := self.mode
	dataFilename := fmt.Sprintf("%s.%s", self.filename, "dat")
	if mode == os.O_WRONLY {
		mode |= os.O_CREATE
		mode |= os.O_APPEND

		fileinfo, err := os.Stat(self.filename)
		if err == nil {
			self.size = int(fileinfo.Size())
		}
		fileinfo, err = os.Stat(dataFilename)
		if err == nil {
			self.dataSize = int(fileinfo.Size())
		}
	}

	file, err := os.OpenFile(self.filename, mode, 0644)
	if err != nil {
		return err
	}
	dataFile, err := os.OpenFile(dataFilename, mode, 0644)
	if err != nil && self.mode == os.O_WRONLY {
		_ = file.Close()
		return err
	}

	self.file = file
	self.dataFile = dataFile
	if self.mode == os.O_WRONLY {
		if self.size == 0 {
			err = self.WriteHeader()
		} else if self.size < 12 {
			err = self.file.Truncate(0)
			if err == nil {
				err = self.WriteHeader()
			}
		} else {
			err = nil
		}
		if err != nil {
			_ = self.file.Close()
			if self.dataFile != nil {
				_ = self.dataFile.Close()
			}
			self.file = nil
			self.dataFile = nil
			return err
		}
		self.wbuf = make([]byte, self.bufSize)
	} else {
		self.rbuf = bufio.NewReaderSize(self.file, self.bufSize)
		err = self.ReadHeader()
		if err != nil {
			_ = self.file.Close()
			if self.dataFile != nil {
				_ = self.dataFile.Close()
			}
			self.file = nil
			self.dataFile = nil
			self.rbuf = nil
			return err
		}
	}
	return nil
}

func (self *AofFile) ReadHeader() error {
	buf := make([]byte, 12)
	n, err := self.rbuf.Read(buf)
	if err != nil {
		return err
	}
	if n != 12 {
		return errors.New("File is not AOF FIle")
	}
	if string(buf[:8]) != "SLOCKAOF" {
		return errors.New("File is not AOF File")
	}

	version := uint16(buf[8]) | uint16(buf[9])<<8
	if version != 0x0001 {
		return errors.New("AOF File Unknown Version")
	}
	headerLen := uint16(buf[10]) | uint16(buf[11])<<8
	if headerLen > 0 {
		n, err = self.rbuf.Read(make([]byte, headerLen))
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
	buf := make([]byte, 12)
	buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7] = 'S', 'L', 'O', 'C', 'K', 'A', 'O', 'F'
	buf[8], buf[9], buf[10], buf[11] = 0x01, 0x00, 0x00, 0x00
	n, err := self.file.Write(buf)
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
		return errors.New("File not open")
	}
	if self.dataFile == nil {
		return io.EOF
	}
	buf := lock.GetBuf()
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}
	fileinfo, err := self.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileinfo.Size()
	if fileSize < 76 {
		return io.EOF
	}
	n, err := self.file.ReadAt(buf, fileSize-64)
	if err != nil {
		return err
	}
	lockLen := uint16(buf[0]) | uint16(buf[1])<<8
	if n != int(lockLen)+2 {
		return errors.New("Lock Len error")
	}
	return nil
}

func (self *AofFile) ReadLockData(lock *AofLock) error {
	if self.dataFile == nil {
		return errors.New("data file error")
	}
	if self.drbuf == nil {
		self.drbuf = bufio.NewReaderSize(self.dataFile, self.bufSize*64)
	}
	buf := self.dlbuf
	if len(buf) != 4 {
		return errors.New("buf error")
	}
	n, err := self.drbuf.Read(buf)
	if err != nil {
		return err
	}
	for n < 4 {
		nn, nerr := self.drbuf.Read(buf[n:])
		if nerr != nil {
			return nerr
		}
		n += nn
	}
	dataLen := int(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
	aofLockData := make([]byte, dataLen+4)
	aofLockData[0], aofLockData[1], aofLockData[2], aofLockData[3] = buf[0], buf[1], buf[2], buf[3]
	if dataLen <= 0 {
		lock.data = aofLockData
		return nil
	}

	n, err = self.drbuf.Read(aofLockData[4:])
	if err != nil {
		return err
	}
	for n < dataLen {
		nn, nerr := self.drbuf.Read(aofLockData[n+4:])
		if nerr != nil {
			return nerr
		}
		n += nn
	}
	lock.data = aofLockData
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
	if lock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && lock.CommandType == protocol.COMMAND_LOCK {
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

func (self *AofFile) AppendLock(lock *AofLock) error {
	if self.file == nil {
		return errors.New("File Unopen")
	}

	buf := lock.GetBuf()
	if len(buf) < 64 {
		return errors.New("Buffer Len error")
	}
	buf[0], buf[1] = 62, 0

	copy(self.wbuf[self.windex:], buf)
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

func (self *AofFile) WriteLockData(lock *AofLock) error {
	if self.dataFile == nil {
		return errors.New("data file error")
	}
	if self.dwbuf == nil {
		self.dwbuf = make([]byte, self.bufSize*64)
	}
	dataLen, bufLen := len(lock.data), len(self.dwbuf)
	if self.windex > 0 {
		if dataLen <= bufLen-self.dwindex {
			copy(self.dwbuf[self.dwindex:], lock.data)
			self.dwindex += dataLen
			self.dataSize += dataLen
			return nil
		}
		err := self.Flush()
		if err != nil {
			return err
		}
	} else if self.dwindex > 0 {
		err := self.Flush()
		if err != nil {
			return err
		}
	}

	if self.windex > 0 || self.dwindex > 0 {
		return errors.New("write lock data error")
	}
	n, err := self.dataFile.Write(lock.data)
	if err != nil {
		return err
	}
	for n < dataLen {
		nn, nerr := self.dataFile.Write(lock.data[n:])
		if nerr != nil {
			return nerr
		}
		n += nn
	}
	self.dataSize += dataLen
	return nil
}

func (self *AofFile) Flush() error {
	if self.file != nil && self.windex > 0 {
		for tn := 0; tn < self.windex; {
			n, err := self.file.Write(self.wbuf[tn:self.windex])
			if err != nil {
				self.windex = 0
				self.dwindex = 0
				for i := 0; i < self.ackIndex; i++ {
					_ = self.aof.lockAcked(self.ackRequests[i], false)
				}
				self.ackIndex = 0
				return err
			}
			tn += n
		}
		self.windex = 0
		self.dirtied = true
	}

	if self.dataFile != nil && self.dwindex > 0 {
		for tn := 0; tn < self.dwindex; {
			n, err := self.dataFile.Write(self.dwbuf[tn:self.dwindex])
			if err != nil {
				self.dwindex = 0
				for i := 0; i < self.ackIndex; i++ {
					_ = self.aof.lockAcked(self.ackRequests[i], false)
				}
				self.ackIndex = 0
				return err
			}
			tn += n
		}
		self.dwindex = 0
	}

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
	if self.file != nil {
		err := self.file.Sync()
		if err != nil {
			return err
		}
	}
	if self.dataFile != nil {
		err := self.dataFile.Sync()
		if err != nil {
			return err
		}
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
	var err error = nil
	if self.file != nil {
		err = self.file.Close()
	}
	if self.dataFile != nil {
		derr := self.dataFile.Close()
		if derr != nil && err == nil {
			err = derr
		}
	}
	self.file = nil
	self.dataFile = nil
	self.wbuf = nil
	self.rbuf = nil
	self.dwbuf = nil
	self.drbuf = nil
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
	lockDbGlockIndex        uint16
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

func NewAofChannel(aof *Aof, lockDb *LockDB, lockDbGlockIndex uint16, lockDbGlock *PriorityMutex) *AofChannel {
	freeLockMax := int(Config.AofQueueSize) / 64
	return &AofChannel{aof, &sync.Mutex{}, lockDb, lockDbGlockIndex, lockDbGlock, nil, nil,
		0, make(chan bool, 1), &sync.Mutex{}, nil, make([]*AofLock, freeLockMax),
		0, freeLockMax, freeLockMax * 2, false, false,
		false, make(chan bool, 1)}
}

func (self *AofChannel) getAofLock() *AofLock {
	if self.freeLockIndex > 0 {
		self.glock.Lock()
		if self.freeLockIndex > 0 {
			self.freeLockIndex--
			aofLock := self.freeLocks[self.freeLockIndex]
			self.glock.Unlock()
			return aofLock
		}
		self.glock.Unlock()
	}
	return NewAofLock()
}

func (self *AofChannel) freeAofLock(aofLock *AofLock) {
	aofLock.lock = nil
	aofLock.data = nil
	if self.freeLockIndex < self.freeLockMax {
		self.glock.Lock()
		if self.freeLockIndex < self.freeLockMax {
			self.freeLocks[self.freeLockIndex] = aofLock
			self.freeLockIndex++
		}
		self.glock.Unlock()
	}
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
		self.lockDbGlockAcquired = self.lockDbGlock.LowSetPriority()
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

func (self *AofChannel) Push(dbId uint8, lock *Lock, commandType uint8, lockCommand *protocol.LockCommand, unLockCommand *protocol.LockCommand, aofFlag uint16, lockData []byte) error {
	if self.closed {
		return io.EOF
	}

	aofLock := self.getAofLock()
	aofLock.CommandType = commandType
	aofLock.AofIndex = 0
	aofLock.AofOffset = 0
	if lock.expriedTime > self.lockDb.currentTime {
		aofLock.CommandTime = uint64(self.lockDb.currentTime)
	} else {
		aofLock.CommandTime = uint64(lock.expriedTime)
	}
	if commandType == protocol.COMMAND_LOCK {
		aofLock.Flag = lockCommand.Flag & 0x12
	} else {
		aofLock.Flag = 0
	}
	aofLock.DbId = dbId
	aofLock.LockId = lockCommand.LockId
	aofLock.LockKey = lockCommand.LockKey
	aofLock.AofFlag = aofFlag
	startTimeSeconds := aofLock.CommandTime - uint64(lock.startTime)
	if startTimeSeconds >= 0xffff {
		aofLock.StartTime = 0xffff
	} else {
		aofLock.StartTime = uint16(startTimeSeconds)
	}
	aofLock.ExpriedFlag = lockCommand.ExpriedFlag
	aofLock.ExpriedTime = self.aof.GetAofLockExpriedTime(lockCommand, lock, aofLock)
	if unLockCommand == nil {
		aofLock.Count = lockCommand.Count
		if commandType == protocol.COMMAND_UNLOCK {
			aofLock.Rcount = 0
		} else {
			aofLock.Rcount = lockCommand.Rcount
		}
	} else {
		aofLock.Count = unLockCommand.Count
		aofLock.Rcount = unLockCommand.Rcount
	}
	if lockCommand.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 {
		aofLock.AofFlag |= AOF_FLAG_REQUIRE_ACKED
		aofLock.lock = lock
	} else {
		aofLock.lock = nil
	}
	aofLock.HandleType = AOF_LOCK_TYPE_FILE
	if lockData != nil {
		aofLock.AofFlag |= AOF_FLAG_CONTAINS_DATA
		aofLock.data = lockData
	}

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) Load(fromAofLock *AofLock) error {
	if self.closed {
		return io.EOF
	}

	if atomic.LoadUint32(&self.lockDbGlock.lowPriority) != 0 {
		self.lockDbGlock.LowPriorityLock()
		self.lockDbGlock.LowPriorityUnlock()
	}
	aofLock := self.getAofLock()
	copy(aofLock.buf, fromAofLock.buf)
	aofLock.data = fromAofLock.data
	aofLock.HandleType = AOF_LOCK_TYPE_LOAD

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) Replay(fromAofLock *AofLock) error {
	if self.closed {
		return io.EOF
	}

	if atomic.LoadUint32(&self.lockDbGlock.lowPriority) != 0 {
		self.lockDbGlock.LowPriorityLock()
		self.lockDbGlock.LowPriorityUnlock()
	}
	aofLock := self.getAofLock()
	copy(aofLock.buf, fromAofLock.buf)
	aofLock.data = fromAofLock.data
	aofLock.HandleType = AOF_LOCK_TYPE_REPLAY

	self.queueGlock.Lock()
	self.pushAofLock(aofLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *AofChannel) AofAcked(buf []byte, succed bool) error {
	aofLock := self.getAofLock()
	copy(aofLock.buf, buf)
	aofLock.data = nil
	aofLock.Flag = 0
	aofLock.AofFlag = 0
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
	aofLock := self.getAofLock()
	aofLock.CommandType = commandResult.CommandType
	aofLock.SetAofId(commandResult.RequestId)
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
	if commandResult.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
		aofLock.data = commandResult.Data.Data
		aofLock.AofFlag |= AOF_FLAG_CONTAINS_DATA
	}

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
			self.queuePulled = false
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
		self.freeAofLock(aofLock)
	case AOF_LOCK_TYPE_LOAD:
		self.HandleLoad(aofLock)
		self.freeAofLock(aofLock)
	case AOF_LOCK_TYPE_REPLAY:
		self.HandleReplay(aofLock)
		self.freeAofLock(aofLock)
	case AOF_LOCK_TYPE_ACK_FILE:
		self.HandleAofAcked(aofLock)
		self.freeAofLock(aofLock)
	case AOF_LOCK_TYPE_ACK_ACKED:
		self.HandleAcked(aofLock)
		self.freeAofLock(aofLock)
	case AOF_LOCK_TYPE_CONSISTENCY_BARRIER:
		self.HandleConsistencyBarrierCommand(aofLock)
	}
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
	err = self.aof.PushLock(self.lockDbGlockIndex, aofLock)
	if err != nil {
		if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && aofLock.CommandType == protocol.COMMAND_LOCK && aofLock.lock != nil {
			lockManager := aofLock.lock.manager
			lockManager.lockDb.DoAckLock(aofLock.lock, false)
		}
	}
}

func (self *AofChannel) HandleLoad(aofLock *AofLock) {
	err := aofLock.Decode()
	if err != nil {
		return
	}

	lockCommand := self.serverProtocol.GetLockCommand()
	lockCommand.CommandType = aofLock.CommandType
	lockCommand.RequestId = aofLock.GetAofId()
	lockCommand.Flag = aofLock.Flag | protocol.LOCK_FLAG_FROM_AOF
	lockCommand.DbId = aofLock.DbId
	lockCommand.LockId = aofLock.LockId
	lockCommand.LockKey = aofLock.LockKey
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		lockCommand.TimeoutFlag = protocol.TIMEOUT_FLAG_REQUIRE_ACKED
	} else {
		lockCommand.TimeoutFlag = 0
	}
	lockCommand.Timeout = 0
	lockCommand.ExpriedFlag = aofLock.ExpriedFlag
	lockCommand.Expried = self.aof.GetLockCommandExpriedTime(self.lockDb, aofLock)
	lockCommand.Count = aofLock.Count
	lockCommand.Rcount = aofLock.Rcount
	if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		lockCommand.Data = protocol.NewLockCommandDataFromOriginBytes(aofLock.data)
		lockCommand.Flag |= protocol.LOCK_FLAG_CONTAINS_DATA
	}

	err = self.serverProtocol.ProcessLockCommand(lockCommand)
	if err == nil {
		return
	}
	self.aof.slock.Log().Errorf("Aof load lock Processlockcommand error %v", err)
}

func (self *AofChannel) HandleReplay(aofLock *AofLock) {
	err := aofLock.Decode()
	if err != nil {
		return
	}

	lockCommand := self.serverProtocol.GetLockCommand()
	lockCommand.CommandType = aofLock.CommandType
	lockCommand.RequestId = aofLock.GetAofId()
	lockCommand.Flag = aofLock.Flag | protocol.LOCK_FLAG_FROM_AOF
	lockCommand.DbId = aofLock.DbId
	lockCommand.LockId = aofLock.LockId
	lockCommand.LockKey = aofLock.LockKey
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		if self.aof.slock.state != STATE_LEADER {
			db := self.aof.slock.replicationManager.GetOrNewAckDB(aofLock.DbId)
			switch aofLock.CommandType {
			case protocol.COMMAND_LOCK:
				_ = db.ProcessFollowerPushAckLock(self.lockDbGlockIndex, aofLock)
			case protocol.COMMAND_UNLOCK:
				_ = db.ProcessFollowerPushAckUnLock(self.lockDbGlockIndex, aofLock)
			}
		}
		lockCommand.TimeoutFlag = protocol.TIMEOUT_FLAG_REQUIRE_ACKED
	} else {
		lockCommand.TimeoutFlag = 0
	}
	lockCommand.Timeout = 0
	lockCommand.ExpriedFlag = aofLock.ExpriedFlag
	lockCommand.Expried = self.aof.GetLockCommandExpriedTime(self.lockDb, aofLock)
	lockCommand.Count = aofLock.Count
	lockCommand.Rcount = aofLock.Rcount
	if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		lockCommand.Data = protocol.NewLockCommandDataFromOriginBytes(aofLock.data)
		lockCommand.Flag |= protocol.LOCK_FLAG_CONTAINS_DATA
	}

	err = self.serverProtocol.ProcessLockCommand(lockCommand)
	if err == nil {
		return
	}
	self.aof.slock.Log().Errorf("Aof replay lock Processlockcommand error %v", err)
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 {
		_ = self.aof.lockLoaded(self, self.serverProtocol.(*MemWaiterServerProtocol), lockCommand, protocol.RESULT_ERROR, 0, 0)
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
			_ = db.ProcessLeaderAofed(self.lockDbGlockIndex, aofLock)
		}
		return
	}
	db := self.aof.slock.replicationManager.GetOrNewAckDB(aofLock.DbId)
	if db != nil {
		_ = db.ProcessFollowerAckAofed(self.lockDbGlockIndex, aofLock)
	}
}

func (self *AofChannel) HandleAcked(aofLock *AofLock) {
	db := self.aof.slock.replicationManager.GetAckDB(aofLock.DbId)
	if db != nil {
		_ = db.ProcessLeaderAcked(self.lockDbGlockIndex, aofLock)
	}
}

func (self *AofChannel) HandleConsistencyBarrierCommand(aofLock *AofLock) {
	if aofLock.CommandType != 0 {
		return
	}
	self.aof.glock.Lock()
	aofLock.Count--
	if aofLock.Count == 0 {
		go self.aof.rewriteAofFiles()
	}
	self.aof.glock.Unlock()
}

type Aof struct {
	slock              *SLock
	glock              *sync.Mutex
	dataDir            string
	aofFileIndex       uint32
	aofFileOffset      uint32
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
	isWaitRewite       bool
	isRewriting        bool
	inited             bool
	closed             bool
}

func NewAof() *Aof {
	return &Aof{nil, &sync.Mutex{}, "", 1, 0, nil, &sync.Mutex{}, &sync.Mutex{},
		make([]*AofChannel, 0), 0, 0, nil, make([]*AofLockQueue, 256),
		&sync.Mutex{}, 0, nil, 0, 0, false, false, false, false}
}

func (self *Aof) Init() ([16]byte, error) {
	self.rewriteSize = uint32(Config.AofFileRewriteSize)
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return [16]byte{}, err
	}

	self.dataDir = dataDir
	if _, serr := os.Stat(self.dataDir); os.IsNotExist(serr) {
		err = os.Mkdir(self.dataDir, 0755)
		if err != nil {
			return [16]byte{}, serr
		}
	}
	self.slock.Log().Infof("Aof config data dir %s", self.dataDir)

	appendFiles, _, ferr := self.FindAofFiles()
	if ferr != nil {
		return [16]byte{}, ferr
	}
	var aofLock *AofLock = nil
	if len(appendFiles) > 0 {
		aofFileIndex, perr := strconv.ParseUint(appendFiles[len(appendFiles)-1][11:], 10, 64)
		if perr != nil {
			return [16]byte{}, perr
		}
		self.aofFileIndex = uint32(aofFileIndex)
		aofLock, err = self.LoadFileMaxAofLock(fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex))
		if err != nil {
			if err != io.EOF {
				return [16]byte{}, err
			}
			self.aofFileOffset = 0
		} else {
			self.aofFileOffset = aofLock.AofOffset
		}
		self.slock.Log().Infof("Aof init current file %s.%d by id %d", "append.aof", self.aofFileIndex, self.aofFileOffset)
	}

	_ = self.WaitFlushAofChannel()
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	if aofLock != nil {
		return aofLock.GetAofId(), nil
	}
	return [16]byte{}, nil
}

func (self *Aof) LoadAndInit() error {
	self.rewriteSize = uint32(Config.AofFileRewriteSize)
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return err
	}

	self.dataDir = dataDir
	if _, serr := os.Stat(self.dataDir); os.IsNotExist(serr) {
		err = os.Mkdir(self.dataDir, 0755)
		if err != nil {
			return serr
		}
	}
	self.slock.Log().Infof("Aof config data dir %s", self.dataDir)

	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return err
	}
	if len(appendFiles) > 0 {
		aofFileIndex, perr := strconv.ParseUint(appendFiles[len(appendFiles)-1][11:], 10, 64)
		if perr != nil {
			return perr
		}
		self.aofFileIndex = uint32(aofFileIndex)
	} else {
		self.aofFileIndex = 1
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	err = self.LoadAofFiles(aofFilenames, time.Now().Unix(), func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		lerr := self.LoadLock(lock)
		if lerr != nil {
			return true, lerr
		}
		if lock.AofIndex == self.aofFileIndex {
			self.aofFileOffset = lock.AofOffset
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	self.slock.Log().Infof("Aof loaded files %v", aofFilenames)

	self.aofFile = NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aofFile.Open()
	if err != nil {
		self.aofFile = nil
		return err
	}
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", self.aofFileIndex)

	_ = self.WaitFlushAofChannel()
	if len(appendFiles) > 0 {
		go self.rewriteAofFiles()
	}
	self.inited = true
	self.slock.Log().Infof("Aof init finish")
	return nil
}

func (self *Aof) Load() error {
	if self.aofFile != nil {
		return nil
	}
	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return err
	}
	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	err = self.LoadAofFiles(aofFilenames, time.Now().Unix(), func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		lerr := self.LoadLock(lock)
		if lerr != nil {
			return true, lerr
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	self.slock.Log().Infof("Aof loaded files %v", aofFilenames)

	self.aofFile = NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", self.aofFileIndex)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aofFile.Open()
	if err != nil {
		self.aofFile = nil
		return err
	}
	self.slock.Log().Infof("Aof open current file %s.%d", "append.aof", self.aofFileIndex)

	_ = self.WaitFlushAofChannel()
	if len(appendFiles) > 0 {
		go self.rewriteAofFiles()
	}
	self.slock.Log().Infof("Aof load finish")
	return nil
}

func (self *Aof) LoadMaxAofId() ([16]byte, error) {
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		return [16]byte{}, err
	}

	self.dataDir = dataDir
	if _, serr := os.Stat(self.dataDir); os.IsNotExist(serr) {
		return [16]byte{}, serr
	}

	appendFiles, rewriteFile, err := self.FindAofFiles()
	if err != nil {
		return [16]byte{}, err
	}

	aofLock := NewAofLock()
	aofLock.AofIndex = 1
	fileAofId := aofLock.GetAofId()
	if len(appendFiles) > 0 {
		aofFileIndex, perr := strconv.ParseUint(appendFiles[len(appendFiles)-1][11:], 10, 64)
		if perr == nil {
			aofLock.AofIndex = uint32(aofFileIndex)
			aofLock.AofOffset = 0
			fileAofId = aofLock.GetAofId()
		}
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	for i := len(aofFilenames) - 1; i >= 0; i-- {
		aofFile := NewAofFile(self, filepath.Join(self.dataDir, aofFilenames[i]), os.O_RDONLY, int(Config.AofFileBufferSize))
		err = aofFile.Open()
		if err != nil {
			return fileAofId, err
		}
		err = aofFile.ReadTail(aofLock)
		if err != nil {
			_ = aofFile.Close()
			if err == io.EOF {
				continue
			}
			return fileAofId, err
		}

		err = aofLock.Decode()
		if err != nil {
			_ = aofFile.Close()
			return fileAofId, err
		}
		_ = aofFile.Close()
		return aofLock.GetAofId(), nil
	}
	return fileAofId, nil
}

func (self *Aof) LoadFileMaxAofLock(filename string) (*AofLock, error) {
	aofFile := NewAofFile(self, filepath.Join(self.dataDir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
	err := aofFile.Open()
	if err != nil {
		return nil, err
	}
	aofLock := NewAofLock()
	err = aofFile.ReadTail(aofLock)
	if err != nil {
		_ = aofFile.Close()
		return nil, err
	}
	err = aofLock.Decode()
	if err != nil {
		_ = aofFile.Close()
		return nil, err
	}
	_ = aofFile.Close()
	return aofLock, nil
}

func (self *Aof) GetCurrentAofID() [16]byte {
	if !self.inited {
		return [16]byte{}
	}

	aofLock := NewAofLock()
	aofLock.AofIndex = self.aofFileIndex
	aofLock.AofOffset = self.aofFileOffset
	return aofLock.GetAofId()
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
		if len(fileName) >= 11 && strings.HasPrefix(fileName, "append.aof.") && !strings.HasSuffix(fileName, ".dat") {
			aofIndex, err := strconv.ParseUint(fileName[11:], 10, 64)
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
		err = aofFile.ReadLock(lock)
		if err != nil {
			_ = aofFile.Close()
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = lock.Decode()
		if err != nil {
			_ = aofFile.Close()
			return err
		}
		if lock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			err = aofFile.ReadLockData(lock)
			if err != nil {
				_ = aofFile.Close()
				return err
			}
		} else {
			lock.data = nil
		}

		if lock.CommandType != protocol.COMMAND_LOCK || lock.Flag&protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED == 0 {
			if lock.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME != 0 {
				if int64(lock.CommandTime+uint64(lock.ExpriedTime)/1000) <= expriedTime {
					continue
				}
			} else if lock.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
				if int64(lock.CommandTime+uint64(lock.ExpriedTime)*60) <= expriedTime {
					continue
				}
			} else if lock.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME == 0 {
				if lock.ExpriedTime > 0 && int64(lock.CommandTime+uint64(lock.ExpriedTime)) <= expriedTime {
					continue
				}
			}
		}

		isStop, iterErr := iterFunc(filename, aofFile, lock, firstLock)
		if iterErr != nil {
			_ = aofFile.Close()
			return iterErr
		}
		if !isStop {
			_ = aofFile.Close()
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

	self.aofGlock.Lock()
	if self.aofFile != nil {
		_ = self.aofFile.Close()
		self.aofFile = nil
	}
	self.aofGlock.Unlock()
	self.slock.logger.Infof("Aof closed")
}

func (self *Aof) NewAofChannel(lockDb *LockDB, lockDbGlockIndex uint16, lockDbGlock *PriorityMutex) *AofChannel {
	self.glock.Lock()
	serverProtocol := NewMemWaiterServerProtocol(self.slock)
	aofChannel := NewAofChannel(self, lockDb, lockDbGlockIndex, lockDbGlock)
	_ = serverProtocol.SetResultCallback(func(serverProtocol *MemWaiterServerProtocol, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8, data []byte) error {
		return self.lockLoaded(aofChannel, serverProtocol, command, result, lcount, lrcount)
	})
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
	if self.aofFile != nil {
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
	self.Flush()
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
		queueCount := 0
		for _, channel := range self.channels {
			channel.queueGlock.Lock()
			queueCount += channel.queueCount
			channel.queueGlock.Unlock()
		}

		if queueCount == 0 {
			self.aofGlock.Lock()
			if channelFlushWaiter == self.channelFlushWaiter {
				self.channelFlushWaiter = nil
			}
			self.aofGlock.Unlock()
			return nil
		}
	}

	<-channelFlushWaiter
	return nil
}

func (self *Aof) ExecuteConsistencyBarrierCommand(commandType uint8) bool {
	channels := self.channels
	count := uint16(len(channels))
	if count == 0 {
		return false
	}
	aofLock := NewAofLock()
	aofLock.CommandType = commandType
	aofLock.Count = count
	aofLock.HandleType = AOF_LOCK_TYPE_CONSISTENCY_BARRIER
	for _, aofChannel := range channels {
		aofChannel.queueGlock.Lock()
		aofChannel.pushAofLock(aofLock)
		aofChannel.queueGlock.Unlock()
	}
	return true
}

func (self *Aof) LoadLock(aofLock *AofLock) error {
	db := self.slock.dbs[aofLock.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(aofLock.DbId)
	}
	fastHash := (uint32(aofLock.LockKey[0]) | uint32(aofLock.LockKey[1])<<8 | uint32(aofLock.LockKey[2])<<16 | uint32(aofLock.LockKey[3])<<24) ^ (uint32(aofLock.LockKey[4]) | uint32(aofLock.LockKey[5])<<8 | uint32(aofLock.LockKey[6])<<16 | uint32(aofLock.LockKey[7])<<24) ^ (uint32(aofLock.LockKey[8]) | uint32(aofLock.LockKey[9])<<8 | uint32(aofLock.LockKey[10])<<16 | uint32(aofLock.LockKey[11])<<24) ^ (uint32(aofLock.LockKey[12])<<24 | uint32(aofLock.LockKey[13])<<16 | uint32(aofLock.LockKey[14])<<8 | uint32(aofLock.LockKey[15]))
	aofChannel := db.aofChannels[fastHash%uint32(db.managerMaxGlocks)]
	return aofChannel.Load(aofLock)
}

func (self *Aof) ReplayLock(aofLock *AofLock) error {
	db := self.slock.dbs[aofLock.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(aofLock.DbId)
	}
	fastHash := (uint32(aofLock.LockKey[0]) | uint32(aofLock.LockKey[1])<<8 | uint32(aofLock.LockKey[2])<<16 | uint32(aofLock.LockKey[3])<<24) ^ (uint32(aofLock.LockKey[4]) | uint32(aofLock.LockKey[5])<<8 | uint32(aofLock.LockKey[6])<<16 | uint32(aofLock.LockKey[7])<<24) ^ (uint32(aofLock.LockKey[8]) | uint32(aofLock.LockKey[9])<<8 | uint32(aofLock.LockKey[10])<<16 | uint32(aofLock.LockKey[11])<<24) ^ (uint32(aofLock.LockKey[12])<<24 | uint32(aofLock.LockKey[13])<<16 | uint32(aofLock.LockKey[14])<<8 | uint32(aofLock.LockKey[15]))
	aofChannel := db.aofChannels[fastHash%uint32(db.managerMaxGlocks)]
	return aofChannel.Replay(aofLock)
}

func (self *Aof) loadLockAck(lockResult *protocol.LockResultCommand) error {
	db := self.slock.dbs[lockResult.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lockResult.DbId)
	}
	fastHash := (uint32(lockResult.LockKey[0]) | uint32(lockResult.LockKey[1])<<8 | uint32(lockResult.LockKey[2])<<16 | uint32(lockResult.LockKey[3])<<24) ^ (uint32(lockResult.LockKey[4]) | uint32(lockResult.LockKey[5])<<8 | uint32(lockResult.LockKey[6])<<16 | uint32(lockResult.LockKey[7])<<24) ^ (uint32(lockResult.LockKey[8]) | uint32(lockResult.LockKey[9])<<8 | uint32(lockResult.LockKey[10])<<16 | uint32(lockResult.LockKey[11])<<24) ^ (uint32(lockResult.LockKey[12])<<24 | uint32(lockResult.LockKey[13])<<16 | uint32(lockResult.LockKey[14])<<8 | uint32(lockResult.LockKey[15]))
	aofChannel := db.aofChannels[fastHash%uint32(db.managerMaxGlocks)]
	return aofChannel.Acked(lockResult)
}

func (self *Aof) PushLock(glockIndex uint16, aofLock *AofLock) error {
	self.aofGlock.Lock()
	if self.aofFile == nil {
		err := self.RewriteAofFile(true)
		if err != nil || self.aofFile == nil {
			self.aofGlock.Unlock()
			return errors.New("file not open")
		}
	}
	self.aofFileOffset++
	_ = aofLock.UpdateAofId(self.aofFileIndex, self.aofFileOffset)

	werr := self.aofFile.WriteLock(aofLock)
	if werr == nil && aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		werr = self.aofFile.WriteLockData(aofLock)
	}
	if uint32(self.aofFile.GetSize()) >= self.rewriteSize {
		_ = self.RewriteAofFile(true)
	}
	self.aofLockCount++
	self.replGlock.Lock()
	self.aofGlock.Unlock()
	perr := self.slock.replicationManager.PushLock(glockIndex, aofLock)
	self.replGlock.Unlock()
	if werr != nil {
		self.slock.Log().Errorf("Aof append file write error %v", werr)
		if perr != nil {
			self.slock.Log().Errorf("Aof push ring buffer queue error %v", perr)
		} else {
			_ = self.slock.replicationManager.WakeupServerChannel()
		}
		return werr
	}
	if perr != nil {
		self.slock.Log().Errorf("Aof push ring buffer queue error %v", perr)
	} else {
		_ = self.slock.replicationManager.WakeupServerChannel()
	}
	return perr
}

func (self *Aof) AppendLock(aofLock *AofLock) bool {
	self.aofGlock.Lock()
	if aofLock.AofIndex != self.aofFileIndex || self.aofFile == nil {
		self.aofFileIndex = aofLock.AofIndex - 1
		err := self.RewriteAofFile(false)
		if err != nil || self.aofFile == nil {
			self.aofGlock.Unlock()
			return false
		}
		self.aofFileOffset = aofLock.AofOffset
	}

	err := self.aofFile.WriteLock(aofLock)
	if err != nil {
		self.slock.Log().Errorf("Aof append file write error %v", err)
	}
	if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		err = self.aofFile.WriteLockData(aofLock)
		if err != nil {
			self.slock.Log().Errorf("Aof append file write data error %v", err)
		}
	}
	self.aofFileOffset = aofLock.AofOffset
	self.aofLockCount++
	self.aofGlock.Unlock()
	return self.isWaitRewite
}

func (self *Aof) lockAcked(buf []byte, succed bool) error {
	if len(buf) < 64 {
		return errors.New("buf size error")
	}
	db := self.slock.dbs[buf[20]]
	if db == nil {
		db = self.slock.GetOrNewDB(buf[20])
	}
	fastHash := (uint32(buf[37]) | uint32(buf[38])<<8 | uint32(buf[39])<<16 | uint32(buf[40])<<24) ^ (uint32(buf[41]) | uint32(buf[42])<<8 | uint32(buf[43])<<16 | uint32(buf[44])<<24) ^ (uint32(buf[45]) | uint32(buf[46])<<8 | uint32(buf[47])<<16 | uint32(buf[48])<<24) ^ (uint32(buf[49])<<24 | uint32(buf[50])<<16 | uint32(buf[51])<<8 | uint32(buf[52]))
	aofChannel := db.aofChannels[fastHash%uint32(db.managerMaxGlocks)]
	return aofChannel.AofAcked(buf, succed)
}

func (self *Aof) lockLoaded(aofChannel *AofChannel, _ *MemWaiterServerProtocol, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.slock.state != STATE_LEADER {
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED == 0 || command.CommandType != protocol.COMMAND_LOCK {
			return nil
		}
		db := self.slock.replicationManager.GetOrNewAckDB(command.DbId)
		if db != nil {
			return db.ProcessFollowerAckLocked(aofChannel.lockDbGlockIndex, command, result, lcount, lrcount)
		}
		return nil
	}
	return nil
}

func (self *Aof) Flush() {
	if self.aofFile == nil {
		return
	}
	if self.aofFile.windex > 0 || self.aofFile.ackIndex > 0 {
		err := self.aofFile.Flush()
		if err != nil {
			self.slock.Log().Errorf("Aof flush file error %v", err)
			return
		}
	}
	if self.aofFile.dirtied {
		err := self.aofFile.Sync()
		if err != nil {
			self.slock.Log().Errorf("Aof Sync file error %v", err)
		}
	}
}

func (self *Aof) FlushWithLocked() {
	self.aofGlock.Lock()
	self.Flush()
	self.aofGlock.Unlock()
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

func (self *Aof) Reset(aofFileIndex uint32, aofFileOffset uint32) error {
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
		err = os.Remove(filepath.Join(self.dataDir, rewriteFile))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", rewriteFile, err)
			return err
		}
		_ = os.Remove(filepath.Join(self.dataDir, fmt.Sprintf("%s.%s", rewriteFile, "dat")))
	}
	for _, appendFile := range appendFiles {
		err = os.Remove(filepath.Join(self.dataDir, appendFile))
		if err != nil {
			self.slock.Log().Errorf("Aof clear files remove %s error %v", appendFile, err)
			return err
		}
		_ = os.Remove(filepath.Join(self.dataDir, fmt.Sprintf("%s.%s", appendFile, "dat")))
	}

	self.aofFile = NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", aofFileIndex)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err = self.aofFile.Open()
	if err != nil {
		self.aofFile = nil
		return err
	}
	self.aofFileIndex = aofFileIndex
	self.aofFileOffset = aofFileOffset
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", aofFileIndex)
	return nil
}

func (self *Aof) RewriteAofFile(startRewite bool) error {
	if self.aofFile != nil {
		self.Flush()
		err := self.aofFile.Close()
		if err != nil {
			self.slock.Log().Errorf("Aof close file %s.%d error %v", "append.aof", self.aofFileIndex, err)
		}
		self.aofFile = nil
	}

	aofFileIndex := self.aofFileIndex + 1
	if aofFileIndex == 0 {
		aofFileIndex = 1
	}
	aofFile := NewAofFile(self, filepath.Join(self.dataDir, fmt.Sprintf("%s.%d", "append.aof", aofFileIndex)), os.O_WRONLY, int(Config.AofFileBufferSize))
	err := aofFile.Open()
	if err != nil {
		self.slock.Log().Infof("Aof open current file %s.%d error %v", "append.aof", aofFileIndex, err)
		return err
	}
	self.aofFile = aofFile
	self.aofFileIndex = aofFileIndex
	self.aofFileOffset = 0
	self.slock.Log().Infof("Aof create current file %s.%d", "append.aof", aofFileIndex)

	if startRewite {
		go self.rewriteAofFiles()
	} else {
		self.isWaitRewite = true
	}
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
	self.isWaitRewite = false
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
		aofFileIndex, perr := strconv.ParseUint(appendFile[11:], 10, 64)
		if perr != nil {
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
	lerr := self.LoadAofFiles(aofFilenames, expriedTime, func(filename string, aofFile *AofFile, aofLock *AofLock, firstLock bool) (bool, error) {
		db := self.slock.GetDB(aofLock.DbId)
		if db == nil {
			return true, nil
		}

		lockCommand.CommandType = aofLock.CommandType
		lockCommand.Flag = aofLock.Flag
		lockCommand.DbId = aofLock.DbId
		lockCommand.LockId = aofLock.LockId
		lockCommand.LockKey = aofLock.LockKey
		lockCommand.ExpriedFlag = aofLock.ExpriedFlag
		lockCommand.Expried = self.GetLockCommandExpriedTime(db, aofLock)
		lockCommand.Count = aofLock.Count
		lockCommand.Rcount = aofLock.Rcount
		if !db.HasLock(lockCommand, aofLock.data) {
			return true, nil
		}

		aofLock.AofFlag |= AOF_FLAG_REWRITED
		aofLock.buf[55] |= AOF_FLAG_REWRITED
		err = rewriteAofFile.AppendLock(aofLock)
		if err != nil {
			return true, err
		}
		if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			err = rewriteAofFile.WriteLockData(aofLock)
			if err != nil {
				return true, err
			}
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
		_ = os.Remove(filepath.Join(self.dataDir, fmt.Sprintf("%s.%s", aofFilename, "dat")))
		self.slock.Log().Infof("Aof rewrite remove file %s", aofFilename)
	}
	err := os.Rename(filepath.Join(self.dataDir, "rewrite.aof.tmp"), filepath.Join(self.dataDir, "rewrite.aof"))
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite rename rewrite.aof.tmp to rewrite.aof error %v", err)
	}
	err = os.Rename(filepath.Join(self.dataDir, "rewrite.aof.tmp.dat"), filepath.Join(self.dataDir, "rewrite.aof.dat"))
	if err != nil {
		self.slock.Log().Errorf("Aof rewrite rename rewrite.aof.tmp.dat to rewrite.aof.dat error %v", err)
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
		_ = os.Remove(filepath.Join(self.dataDir, fmt.Sprintf("%s.%s", fileName, "dat")))
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

	size := int(Config.AofQueueSize) / 64
	queue := &AofLockQueue{make([]*AofLock, size), 0, 0, size, nil}
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

func (self *Aof) GetLockCommandExpriedTime(lockDb *LockDB, aofLock *AofLock) uint16 {
	if aofLock.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
		return aofLock.ExpriedTime
	}
	if aofLock.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME != 0 {
		return aofLock.ExpriedTime
	}
	if aofLock.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
		expriedTimeSeconds := lockDb.currentTime - int64(aofLock.CommandTime)
		if expriedTimeSeconds >= 0 {
			expriedTimeMinutes := expriedTimeSeconds / 60
			if expriedTimeSeconds < 60 || expriedTimeSeconds%60 != 0 {
				expriedTimeMinutes++
			}
			if aofLock.ExpriedTime > uint16(expriedTimeMinutes) {
				return aofLock.ExpriedTime - uint16(expriedTimeMinutes)
			}
			return 1
		}
		return aofLock.ExpriedTime
	} else if aofLock.ExpriedTime > 0 {
		expriedTimeSeconds := lockDb.currentTime - int64(aofLock.CommandTime)
		if expriedTimeSeconds >= 0 {
			if aofLock.ExpriedTime > uint16(expriedTimeSeconds) {
				return aofLock.ExpriedTime - uint16(expriedTimeSeconds)
			}
			return 1
		}
		return aofLock.ExpriedTime
	}
	return aofLock.ExpriedTime
}

func (self *Aof) GetAofLockExpriedTime(lockCommand *protocol.LockCommand, lock *Lock, aofLock *AofLock) uint16 {
	if lockCommand.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
		return lockCommand.Expried
	}
	if lockCommand.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME != 0 {
		return lockCommand.Expried
	}
	if lockCommand.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
		expriedTimeSeconds := lock.expriedTime - int64(aofLock.CommandTime)
		if expriedTimeSeconds >= 60 && expriedTimeSeconds%60 == 0 {
			return uint16(expriedTimeSeconds / 60)
		}
		if expriedTimeSeconds > 0 {
			return uint16(expriedTimeSeconds/60) + 1
		}
		return 1
	}
	if lock.expriedTime > 0 {
		expriedTimeSeconds := lock.expriedTime - int64(aofLock.CommandTime)
		if expriedTimeSeconds > 0 {
			return uint16(expriedTimeSeconds)
		}
		return 1
	}
	return lockCommand.Expried
}
