package client

import (
	"github.com/snower/slock/protocol"
	"sync"
)

const EVENT_MODE_DEFAULT_SET = 0
const EVENT_MODE_DEFAULT_CLEAR = 1

type Event struct {
	db        *Database
	eventKey  [16]byte
	timeout   uint32
	expried   uint32
	eventLock *Lock
	checkLock *Lock
	waitLock  *Lock
	glock     *sync.Mutex
	setedMode uint8
}

func NewEvent(db *Database, eventKey [16]byte, timeout uint32, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultSetEvent(db *Database, eventKey [16]byte, timeout uint32, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultClearEvent(db *Database, eventKey [16]byte, timeout uint32, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_CLEAR}
}

func (self *Event) GetEventKey() [16]byte {
	return self.eventKey
}

func (self *Event) GetTimeout() uint32 {
	return self.timeout
}

func (self *Event) GetExpried() uint32 {
	return self.expried
}

func (self *Event) Mode() uint8 {
	return self.setedMode
}

func (self *Event) Clear() (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()
		return self.eventLock.LockUpdate()
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	result, err := self.eventLock.Unlock()
	if err == nil {
		return result, nil
	}
	if result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR {
		return result, nil
	}
	return result, err
}

func (self *Event) ClearWithUnsetData() (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()
		return self.eventLock.LockUpdateWithData(protocol.NewLockCommandDataUnsetData())
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	result, err := self.eventLock.UnlockWithData(protocol.NewLockCommandDataUnsetData())
	if err == nil {
		return result, nil
	}
	if result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR {
		return result, nil
	}
	return result, err
}

func (self *Event) Set() (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		result, err := self.eventLock.Unlock()
		if err == nil {
			return result, nil
		}
		if result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR {
			return result, nil
		}
		return result, err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()
	return self.eventLock.LockUpdate()
}

func (self *Event) SetWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		result, err := self.eventLock.UnlockWithData(data)
		if err == nil {
			return result, nil
		}
		if result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR {
			return result, nil
		}
		return result, err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()
	return self.eventLock.LockUpdateWithData(data)
}

func (self *Event) IsSet() (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.checkLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, 0, 0, 0, 0}
		result, err := self.checkLock.Lock()
		if err == nil {
			return true, nil
		}
		if result != nil && result.Result == protocol.RESULT_TIMEOUT {
			return false, nil
		}
		return false, err
	}

	self.checkLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, 0x02000000, 0, 1, 0}
	result, err := self.checkLock.Lock()
	if err == nil {
		return true, nil
	}
	if result != nil && (result.Result == protocol.RESULT_UNOWN_ERROR || result.Result == protocol.RESULT_TIMEOUT) {
		return false, nil
	}
	return false, err
}

func (self *Event) Wait(timeout uint32) (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout, 0, 0, 0}
		result, err := self.waitLock.Lock()
		if err == nil {
			return result, nil
		}
		if result != nil && result.Result == protocol.RESULT_TIMEOUT {
			return result, WaitTimeout
		}
		return result, err
	}

	self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	result, err := self.waitLock.Lock()
	if err == nil {
		return result, nil
	}
	if result != nil && result.Result == protocol.RESULT_TIMEOUT {
		return result, WaitTimeout
	}
	return result, err
}

func (self *Event) WaitAndTimeoutRetryClear(timeout uint32) (*protocol.LockResultCommand, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout, 0, 0, 0}
		result, err := self.waitLock.Lock()
		if err == nil {
			return result, nil
		}
		if result != nil && result.Result == protocol.RESULT_TIMEOUT {
			self.glock.Lock()
			if self.eventLock == nil {
				self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
			}
			self.glock.Unlock()

			rresult, rerr := self.eventLock.LockUpdate()
			if rerr == nil {
				if rresult.Result == protocol.RESULT_SUCCED {
					_, _ = self.eventLock.Unlock()
					return rresult, nil
				}
				if rresult.Result == protocol.RESULT_LOCKED_ERROR {
					return result, WaitTimeout
				}
			}
		}
		return result, err
	}

	self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	result, err := self.waitLock.Lock()
	if err == nil {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
		}
		self.glock.Unlock()
		_, _ = self.eventLock.Unlock()
		return result, nil
	}
	if result != nil && result.Result == protocol.RESULT_TIMEOUT {
		return result, WaitTimeout
	}
	return result, err
}
