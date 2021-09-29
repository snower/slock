package client

import (
    "errors"
    "github.com/snower/slock/protocol"
    "sync"
)

const EVENT_MODE_DEFAULT_SET = 0
const EVENT_MODE_DEFAULT_CLEAR = 1

type Event struct {
    db          *Database
    event_key   [16]byte
    timeout     uint32
    expried     uint32
    event_lock  *Lock
    check_lock  *Lock
    wait_lock   *Lock
    glock       *sync.Mutex
    seted_mode  uint8
}

func NewEvent(db *Database, event_key [16]byte, timeout uint32, expried uint32) *Event {
    return &Event{db, event_key, timeout, expried, nil,
        nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultSetEvent(db *Database, event_key [16]byte, timeout uint32, expried uint32) *Event {
    return &Event{db, event_key, timeout, expried, nil,
        nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultClearEvent(db *Database, event_key [16]byte, timeout uint32, expried uint32) *Event {
    return &Event{db, event_key, timeout, expried, nil,
        nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_CLEAR}
}

func (self *Event) GetEventKey() [16]byte {
    return self.event_key
}

func (self *Event) GetTimeout() uint32 {
    return self.timeout
}

func (self *Event) GetExpried() uint32 {
    return self.expried
}

func (self *Event) Mode() uint8 {
    return self.seted_mode
}

func (self *Event) Clear() error {
    if self.seted_mode == EVENT_MODE_DEFAULT_SET {
        self.glock.Lock()
        if self.event_lock == nil {
            self.event_lock = &Lock{self.db, self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
        }
        self.glock.Unlock()

        err := self.event_lock.LockUpdate()
        if err == nil {
            return nil
        }
        if err.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
            return nil
        }
        return errors.New("unknown command result")
    }

    self.glock.Lock()
    if self.event_lock == nil {
        self.event_lock = &Lock{self.db, self.event_key, self.event_key, self.timeout, self.expried, 1, 0}
    }
    self.glock.Unlock()

    err := self.event_lock.Unlock()
    if err == nil {
        return nil
    }
    if err.Result == protocol.RESULT_UNLOCK_ERROR {
        return nil
    }
    return err.Err
}

func (self *Event) Set() error {
    if self.seted_mode == EVENT_MODE_DEFAULT_SET {
        self.glock.Lock()
        if self.event_lock == nil {
            self.event_lock = &Lock{self.db, self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
        }
        self.glock.Unlock()

        err := self.event_lock.Unlock()
        if err == nil {
            return nil
        }
        if err.Result == protocol.RESULT_UNLOCK_ERROR {
            return nil
        }
        return err.Err
    }

    self.glock.Lock()
    if self.event_lock == nil {
        self.event_lock = &Lock{self.db, self.event_key, self.event_key, self.timeout, self.expried, 1, 0}
    }
    self.glock.Unlock()

    err := self.event_lock.LockUpdate()
    if err == nil {
        return err
    }
    if err.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
        return nil
    }
    return errors.New("unknown command result")
}

func (self *Event) IsSet() (bool, error){
    if self.seted_mode == EVENT_MODE_DEFAULT_SET {
        self.check_lock = &Lock{self.db, self.event_key, self.db.GenLockId(), 0, 0, 0, 0}
        err := self.check_lock.Lock()
        if err == nil {
            return true, nil
        }
        if err.Result == protocol.RESULT_TIMEOUT {
            return false, nil
        }
        return false, err.Err
    }

    self.check_lock = &Lock{self.db, self.event_key, self.db.GenLockId(), 0x02000000, 0, 1, 0}
    err := self.check_lock.Lock()
    if err == nil {
        return true, nil
    }
    if err.Result == protocol.RESULT_UNOWN_ERROR || err.Result == protocol.RESULT_TIMEOUT {
        return false, nil
    }
    return false, err.Err
}

func (self *Event) Wait(timeout uint32) (bool, error) {
    if self.seted_mode == EVENT_MODE_DEFAULT_SET {
        self.wait_lock = &Lock{self.db, self.event_key, self.db.GenLockId(), timeout, 0, 0, 0}
        err := self.wait_lock.Lock()
        if err == nil {
            return true, nil
        }
        if err.Result == protocol.RESULT_TIMEOUT {
            return false, nil
        }
        return false, err.Err
    }

    self.wait_lock = &Lock{self.db, self.event_key, self.db.GenLockId(), timeout | 0x02000000, 0, 1, 0}
    err := self.wait_lock.Lock()
    if err == nil {
        return true, nil
    }
    if err.Result == protocol.RESULT_TIMEOUT {
        return false, nil
    }
    return false, err
}

func (self *Event) WaitAndTimeoutRetryClear(timeout uint32) (bool, error) {
    if self.seted_mode == EVENT_MODE_DEFAULT_SET {
        self.wait_lock = &Lock{self.db, self.event_key, self.db.GenLockId(), timeout, 0, 0, 0}
        err := self.wait_lock.Lock()
        if err == nil {
            return true, nil
        }

        if err.Result == protocol.RESULT_TIMEOUT {
            self.glock.Lock()
            if self.event_lock == nil {
                self.event_lock = &Lock{self.db, self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
            }
            self.glock.Unlock()

            rerr := self.event_lock.LockUpdate()
            if rerr != nil {
                if rerr.CommandResult.Result == protocol.RESULT_SUCCED {
                    _ = self.event_lock.Unlock()
                    return true, nil
                }
                if rerr.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
                    return false, nil
                }
            }
        }
        return false, err.Err
    }
    return self.Wait(timeout)
}