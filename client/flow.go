package client

import (
	"github.com/snower/slock/protocol"
	"math"
	"sync"
	"time"
)

type MaxConcurrentFlow struct {
	db       *Database
	flowKey  [16]byte
	count    uint16
	timeout  uint32
	expried  uint32
	priority uint8
	flowLock *Lock
	glock    *sync.Mutex
}

func NewMaxConcurrentFlow(db *Database, flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow {
	if count > 0 {
		count -= 1
	}
	return &MaxConcurrentFlow{db, flowKey, count, timeout, expried, 0, nil, &sync.Mutex{}}
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

func (self *MaxConcurrentFlow) GetPriority() uint8 {
	return self.priority
}

func (self *MaxConcurrentFlow) SetPriority(priority uint8) uint8 {
	oPriority := self.priority
	self.priority = priority
	return oPriority
}

func (self *MaxConcurrentFlow) Acquire() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.flowLock == nil {
		timeout := self.timeout
		if self.priority > 0 {
			timeout |= protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
		}
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, timeout, self.expried, self.count, self.priority}
	}
	self.glock.Unlock()
	return self.flowLock.Lock()
}

func (self *MaxConcurrentFlow) Release() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.flowLock == nil {
		timeout := self.timeout
		if self.priority > 0 {
			timeout |= protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
		}
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, timeout, self.expried, self.count, self.priority}
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
	priority    uint8
	flowLock    *Lock
	glock       *sync.Mutex
}

func NewTokenBucketFlow(db *Database, flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	if count > 0 {
		count -= 1
	}
	return &TokenBucketFlow{db, flowKey, count, timeout, period, 0, 0, nil, &sync.Mutex{}}
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

func (self *TokenBucketFlow) GetPriority() uint8 {
	return self.priority
}

func (self *TokenBucketFlow) SetPriority(priority uint8) uint8 {
	oPriority := self.priority
	self.priority = priority
	return oPriority
}

func (self *TokenBucketFlow) Acquire() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	timeout := self.timeout
	if self.priority > 0 {
		timeout |= protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
	}
	if self.period < 3 {
		expried := uint32(math.Ceil(self.period*1000)) | 0x04000000
		expried |= uint32(self.expriedFlag) << 16
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, timeout, expried, self.count, self.priority}
		self.glock.Unlock()
		return self.flowLock.Lock()
	}

	now := time.Now().UnixNano() / 1e9
	expried := uint32(int64(math.Ceil(self.period)) - (now % int64(math.Ceil(self.period))))
	expried |= uint32(self.expriedFlag) << 16
	self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, 0, expried, self.count, self.priority}
	self.glock.Unlock()

	result, err := self.flowLock.Lock()
	if err != nil && result != nil && result.Result == protocol.RESULT_TIMEOUT {
		self.glock.Lock()
		expried = uint32(math.Ceil(self.period))
		expried |= uint32(self.expriedFlag) << 16
		self.flowLock = &Lock{self.db, self.db.GenLockId(), self.flowKey, timeout, expried, self.count, self.priority}
		self.glock.Unlock()
		return self.flowLock.Lock()
	}
	return result, err
}
