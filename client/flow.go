package client

import (
	"github.com/snower/slock/protocol"
	"sync"
	"time"
)

type MaxConcurrentFlow struct {
	db       *Database
	flowKey  [16]byte
	count    uint16
	timeout  uint32
	expried  uint32
	flowLock *Lock
	glock    *sync.Mutex
}

func NewMaxConcurrentFlow(db *Database, flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow {
	if count > 0 {
		count -= 1
	}
	return &MaxConcurrentFlow{db, flowKey, count, timeout, expried, nil, &sync.Mutex{}}
}

func (self *MaxConcurrentFlow) GetTimeoutFlag() uint16 {
	return uint16((self.timeout & 0xffff) >> 16)
}

func (self *MaxConcurrentFlow) SetTimeoutFlag(flag uint16) uint16 {
	oflag := self.GetTimeoutFlag()
	self.timeout = (self.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (self *MaxConcurrentFlow) GetExpriedFlag() uint16 {
	return uint16((self.expried & 0xffff) >> 16)
}

func (self *MaxConcurrentFlow) SetExpriedFlag(flag uint16) uint16 {
	oflag := self.GetExpriedFlag()
	self.expried = (self.expried & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (self *MaxConcurrentFlow) Acquire() *LockError {
	self.glock.Lock()
	if self.flowLock == nil {
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, self.timeout, self.expried, self.count, 0}
	}
	self.glock.Unlock()
	return self.flowLock.Lock()
}

func (self *MaxConcurrentFlow) Release() *LockError {
	self.glock.Lock()
	if self.flowLock == nil {
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, self.timeout, self.expried, self.count, 0}
	}
	self.glock.Unlock()
	return self.flowLock.Unlock()
}

type TokenBucketFlow struct {
	db          *Database
	flowKey     [16]byte
	count       uint16
	timeout     uint32
	period      float64
	expriedFlag uint16
	flowLock    *Lock
	glock       *sync.Mutex
}

func NewTokenBucketFlow(db *Database, flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	if count > 0 {
		count -= 1
	}
	return &TokenBucketFlow{db, flowKey, count, timeout, period, 0, nil, &sync.Mutex{}}
}

func (self *TokenBucketFlow) GetTimeoutFlag() uint16 {
	return uint16((self.timeout & 0xffff) >> 16)
}

func (self *TokenBucketFlow) SetTimeoutFlag(flag uint16) uint16 {
	oflag := self.GetTimeoutFlag()
	self.timeout = (self.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (self *TokenBucketFlow) GetExpriedFlag() uint16 {
	return self.expriedFlag
}

func (self *TokenBucketFlow) SetExpriedFlag(flag uint16) uint16 {
	oflag := self.GetExpriedFlag()
	self.expriedFlag = flag
	return oflag
}

func (self *TokenBucketFlow) Acquire() *LockError {
	self.glock.Lock()
	expried := uint32(0)
	now := time.Now().Nanosecond() / 1e6
	if self.period <= 1 {
		expried = (1000 - uint32(now%1000)) | 0x04000000
	} else {
		now /= 1000
		if uint32(self.period)%60 == 0 {
			expried = uint32(((now/60+1)*60)%120 + (60 - (now % 60)))
		} else {
			expried = uint32(int(self.period) - (now % int(self.period)))
		}
	}
	expried |= uint32(self.expriedFlag) << 16
	self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, 0, expried, self.count, 0}
	self.glock.Unlock()

	err := self.flowLock.Lock()
	if err != nil && err.Result == protocol.RESULT_TIMEOUT {
		self.glock.Lock()
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, self.timeout, uint32(self.period), self.count, 0}
		self.glock.Unlock()
		return self.flowLock.Lock()
	}
	return err
}
