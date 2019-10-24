package client

import (
    "sync"
    "github.com/snower/slock/protocol"
)

type Event struct {
    db *Database
    event_key [16]byte
    timeout uint32
    expried uint32
    event_lock *Lock
    check_lock *Lock
    wait_lock *Lock
    glock *sync.Mutex
}

func NewEvent(db *Database, event_key [16]byte, timeout uint32, expried uint32) *Event {
    return &Event{db, event_key, timeout, expried, nil, nil, nil, &sync.Mutex{}}
}

func (self *Event) Clear() error{
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.event_lock == nil {
        self.event_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
    }
    err := self.event_lock.Lock()
    if err != nil && err.Result != protocol.RESULT_LOCKED_ERROR {
        return err
    }

    return nil
}

func (self *Event) Set() error{
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.event_lock == nil {
        self.event_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
    }
    err := self.event_lock.Unlock()
    if err != nil && err.Result != protocol.RESULT_UNLOCK_ERROR {
        return err
    }

    return nil
}

func (self *Event) IsSet() (bool, error){
    defer self.glock.Unlock()
    self.glock.Lock()

    self.check_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, 0, 0, 0, 0}

    err := self.check_lock.Lock()

    if err != nil && err.Result != protocol.RESULT_TIMEOUT {
        return true, nil
    }

    return false, err
}

func (self *Event) Wait(timeout uint32) (bool, error) {
    defer self.glock.Unlock()
    self.glock.Lock()

    self.wait_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, timeout, 0, 0, 0}

    err := self.wait_lock.Lock()

    if err == nil {
        return true, nil
    }

    return false, err
}

type CycleEvent struct {
    Event
}

func NewCycleEvent(db *Database, event_key [16]byte, timeout uint32, expried uint32) *CycleEvent {
    return &CycleEvent{Event{db, event_key, timeout, expried, nil, nil, nil, &sync.Mutex{}}}
}

func (self *CycleEvent) Wait(timeout uint32) (bool, error) {
    defer self.glock.Unlock()
    self.glock.Lock()

    self.wait_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, timeout, 0, 0, 0}

    err := self.wait_lock.Lock()

    if err == nil {
        return true, nil
    }

    if err.Result != protocol.RESULT_TIMEOUT {
        if self.event_lock == nil {
            self.event_lock = &Lock{self.db, self.db.GetRequestId(), self.event_key, self.event_key, self.timeout, self.expried, 0, 0}
        }
        _, err := self.event_lock.DoLock(0x02)
        if err != nil && err.Result != protocol.RESULT_LOCKED_ERROR {
            return false, err
        }

        _, err = self.event_lock.DoUnlock(0x00)
        if err != nil {
            return true, nil
        }
        return true, nil
    }

    return false, err
}