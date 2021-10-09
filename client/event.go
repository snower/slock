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

func (self *Event) Clear() error {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		err := self.eventLock.LockUpdate()
		if err.Result == protocol.RESULT_SUCCED {
			return nil
		}
		return err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	err := self.eventLock.Unlock()
	if err == nil {
		return nil
	}
	if err.Result == protocol.RESULT_UNLOCK_ERROR {
		return nil
	}
	return err
}

func (self *Event) Set() error {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		err := self.eventLock.Unlock()
		if err == nil {
			return nil
		}
		if err.Result == protocol.RESULT_UNLOCK_ERROR {
			return nil
		}
		return err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	err := self.eventLock.LockUpdate()
	if err.Result == protocol.RESULT_SUCCED {
		return nil
	}
	return err
}

func (self *Event) IsSet() (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.checkLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, 0, 0, 0, 0}
		err := self.checkLock.Lock()
		if err == nil {
			return true, nil
		}
		if err.Result == protocol.RESULT_TIMEOUT {
			return false, nil
		}
		return false, err
	}

	self.checkLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, 0, 0, 0, 0}
	err := self.checkLock.Lock()
	if err == nil {
		return false, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return true, nil
	}
	return false, err
}

func (self *Event) Wait(timeout uint32) (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout, 0, 0, 0}
		err := self.waitLock.Lock()
		if err == nil {
			return true, nil
		}
		if err.Result == protocol.RESULT_TIMEOUT {
			return false, nil
		}
		return false, err.Err
	}

	self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	err := self.waitLock.Lock()
	if err == nil {
		return true, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (self *Event) WaitAndTimeoutRetryClear(timeout uint32) (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout, 0, 0, 0}
		err := self.waitLock.Lock()
		if err == nil {
			return true, nil
		}

		if err.Result == protocol.RESULT_TIMEOUT {
			self.glock.Lock()
			if self.eventLock == nil {
				self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
			}
			self.glock.Unlock()

			rerr := self.eventLock.LockUpdate()
			if rerr.Result == 0 {
				if rerr.CommandResult.Result == protocol.RESULT_SUCCED {
					_ = self.eventLock.Unlock()
					return true, nil
				}
				if rerr.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
					return false, nil
				}
			}
		}
		return false, err
	}

	self.waitLock = &Lock{self.db, self.db.GenLockId(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	err := self.waitLock.Lock()
	if err == nil {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
		}
		self.glock.Unlock()
		_ = self.eventLock.Unlock()
		return true, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}
