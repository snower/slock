package client

import "github.com/snower/slock/protocol"

type Semaphore struct {
    db              *Database
    semaphore_key   [16]byte
    timeout         uint32
    expried         uint32
    count           uint16
}

func NewSemaphore(db *Database, semaphore_key [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
    return &Semaphore{db, semaphore_key, timeout, expried, count}
}

func (self *Semaphore) Acquire() error {
    lock := &Lock{self.db, [16]byte{}, self.db.GenLockId(), self.semaphore_key, self.timeout, self.expried, self.count, 0}
    _, err := lock.DoLock(0)
    return err
}

func (self *Semaphore) Release() error {
    lock := &Lock{self.db, [16]byte{}, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
    _, err := lock.DoUnlock(0x01)
    return err
}

func (self *Semaphore) ReleaseN(n int) (int, error) {
    lock := &Lock{self.db, [16]byte{}, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
    for i := 0; i < n; i++{
        lock_result_command, err := lock.DoUnlock(0x01)
        if err != nil {
            return i + 1, err
        }
        if lock_result_command.Result == protocol.RESULT_UNLOCK_ERROR || lock_result_command.Result == protocol.RESULT_UNOWN_ERROR {
            return i + 1, nil
        }
    }
    return n, nil
}

func (self *Semaphore) ReleaseAll() error {
    lock := &Lock{self.db, [16]byte{}, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
    for {
        lock_result_command, err := lock.DoUnlock(0x01)
        if err != nil {
            return err
        }
        if lock_result_command.Result == protocol.RESULT_UNLOCK_ERROR || lock_result_command.Result == protocol.RESULT_UNOWN_ERROR {
            return nil
        }
    }
}

func (self *Semaphore) Count() (int, error) {
    lock := &Lock{self.db, [16]byte{}, self.db.GenLockId(), self.semaphore_key, 0, 0, self.count, 0}
    lock_result_command, err := lock.DoLock(0x01)
    if err != nil {
        return 0, nil
    }

    if lock_result_command.Result == protocol.RESULT_UNLOCK_ERROR {
        return 0, nil
    }

    if lock_result_command.Result == protocol.RESULT_UNOWN_ERROR {
        return int(lock_result_command.Lcount), nil
    }

    if lock_result_command.Result == protocol.RESULT_TIMEOUT {
        return int(self.count), nil
    }
    return 0, err
}
