package server

import (
    "bytes"
    "errors"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type AofLock struct {
    DbId            uint8
    CommandType     uint8
    Flag            uint8
    LockKey         [2]uint64
    LockId          [2]uint64
    StartTime       uint64
    ExpriedTime     uint64
    Count           uint16
    Rcount          uint8
}

type AofFile struct {
    slock       *SLock
    aof         *Aof
    filename    string
    file        *os.File
    mode        int
    glock       *sync.Mutex
    buf         []byte
    buf_size    int
    buf_index   int
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile{
    buf := make([]byte, buf_size)
    return &AofFile{aof.slock, aof, filename, nil, mode, &sync.Mutex{}, buf, buf_size, 0}
}

func (self *AofFile) Open() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    mode := self.mode
    if mode == os.O_WRONLY {
        mode |= os.O_CREATE
    }
    file, err := os.OpenFile(self.filename, mode, 0644)
    if err != nil {
        return err
    }

    self.file = file
    if self.mode == os.O_WRONLY {
        err = self.WriteHeader()
        if err != nil {
            self.file.Close()
            return err
        }
    } else {
        err = self.ReadHeader()
        if err != nil {
            self.file.Close()
            return err
        }
    }

    return nil
}

func (self *AofFile) ReadHeader() error {
    n, err := self.file.Read(self.buf)
    if err != nil {
        return err
    }

    self.buf_size = n
    if self.buf_size < 12 {
        return errors.New("File is not AOF File")
    }

    if bytes.Compare(self.buf[:8], []byte("SLOCKAOF")) != 0 {
        return errors.New("File is not AOF File")
    }

    version := uint16(self.buf[8]) | uint16(self.buf[9] << 8)
    if version != 0x0001 {
        return errors.New("AOF File Unknown Version")
    }

    header_len := uint16(self.buf[10]) | uint16(self.buf[11] << 8)
    if header_len != 0x0000 {
        return errors.New("AOF File Header Len Error")
    }

    self.buf_index += int(12 + header_len)
    return nil
}

func (self *AofFile) WriteHeader() error {
    self.buf[0], self.buf[1], self.buf[2], self.buf[3], self.buf[4], self.buf[5], self.buf[6], self.buf[6] = 'S', 'L', 'O', 'C', 'K', 'A', 'O', 'F'
    self.buf[8], self.buf[9], self.buf[10], self.buf[11] = 0x01, 0x00, 0x00, 0x00
    n, err := self.file.Write(self.buf[:12])
    if n != 12 {
        return err
    }
    return nil
}

func (self *AofFile) ReadLock(lock *AofLock) error {
    buf_index := self.buf_index
    data_len := uint16(self.buf[buf_index]) | uint16(self.buf[buf_index + 1] << 8)
    if int(data_len) < self.buf_size - self.buf_index {
        return errors.New("Buffer Len Error")
    }

    lock.DbId, lock.CommandType, lock.Flag = self.buf[buf_index + 2], self.buf[buf_index + 3], self.buf[buf_index + 4]

    lock.LockKey[0] = uint64(self.buf[buf_index+5]) | uint64(self.buf[buf_index+6])<<8 | uint64(self.buf[buf_index+7])<<16 | uint64(self.buf[buf_index+8])<<24 | uint64(self.buf[buf_index+9])<<32 | uint64(self.buf[buf_index+10])<<40 | uint64(self.buf[buf_index+11])<<48 | uint64(self.buf[buf_index+12])<<56
    lock.LockKey[1] = uint64(self.buf[buf_index+13]) | uint64(self.buf[buf_index+14])<<8 | uint64(self.buf[buf_index+15])<<16 | uint64(self.buf[buf_index+16])<<24 | uint64(self.buf[buf_index+17])<<32 | uint64(self.buf[buf_index+18])<<40 | uint64(self.buf[buf_index+19])<<48 | uint64(self.buf[buf_index+20])<<56

    lock.LockId[0] = uint64(self.buf[buf_index+21]) | uint64(self.buf[buf_index+22])<<8 | uint64(self.buf[buf_index+23])<<16 | uint64(self.buf[buf_index+24])<<24 | uint64(self.buf[buf_index+25])<<32 | uint64(self.buf[buf_index+26])<<40 | uint64(self.buf[buf_index+27])<<48 | uint64(self.buf[buf_index+28])<<56
    lock.LockId[1] = uint64(self.buf[buf_index+29]) | uint64(self.buf[buf_index+30])<<8 | uint64(self.buf[buf_index+31])<<16 | uint64(self.buf[buf_index+32])<<24 | uint64(self.buf[buf_index+33])<<32 | uint64(self.buf[buf_index+34])<<40 | uint64(self.buf[buf_index+35])<<48 | uint64(self.buf[buf_index+36])<<56

    lock.StartTime = uint64(self.buf[buf_index+37]) | uint64(self.buf[buf_index+38])<<8 | uint64(self.buf[buf_index+39])<<16 | uint64(self.buf[buf_index+40])<<24 | uint64(self.buf[buf_index+41])<<32 | uint64(self.buf[buf_index+42])<<40 | uint64(self.buf[buf_index+43])<<48 | uint64(self.buf[buf_index+44])<<56
    lock.ExpriedTime = uint64(self.buf[buf_index+45]) | uint64(self.buf[buf_index+46])<<8 | uint64(self.buf[buf_index+47])<<16 | uint64(self.buf[buf_index+48])<<24 | uint64(self.buf[buf_index+49])<<32 | uint64(self.buf[buf_index+50])<<40 | uint64(self.buf[buf_index+51])<<48 | uint64(self.buf[buf_index+52])<<56

    lock.Count = uint16(self.buf[buf_index+53]) | uint16(self.buf[buf_index+54])<<8
    lock.Rcount = self.buf[buf_index + 55]

    self.buf_index += int(data_len) + 2
    return nil
}

func (self *AofFile) WriteLock(lock *AofLock) error {
    buf_len := 56
    if self.buf_size - self.buf_index < buf_len {
        return errors.New("Buffer Len Error")
    }

    buf_index := self.buf_index

    self.buf[buf_index], self.buf[buf_index + 1] = byte(buf_len), byte(buf_len << 8)
    self.buf[buf_index+2], self.buf[buf_index + 3], self.buf[buf_index + 4] = lock.DbId, lock.CommandType, lock.Flag

    self.buf[buf_index+5], self.buf[buf_index+6], self.buf[buf_index+7], self.buf[buf_index+8], self.buf[buf_index+9], self.buf[buf_index+10], self.buf[buf_index+11], self.buf[buf_index+12] = byte(lock.LockKey[0]), byte(lock.LockKey[0] >> 8), byte(lock.LockKey[0] >> 16), byte(lock.LockKey[0] >> 24), byte(lock.LockKey[0] >> 32), byte(lock.LockKey[0] >> 40), byte(lock.LockKey[0] >> 48), byte(lock.LockKey[0] >> 56)
    self.buf[buf_index+13], self.buf[buf_index+14], self.buf[buf_index+15], self.buf[buf_index+16], self.buf[buf_index+17], self.buf[buf_index+18], self.buf[buf_index+19], self.buf[buf_index+20] = byte(lock.LockKey[1]), byte(lock.LockKey[1] >> 8), byte(lock.LockKey[1] >> 16), byte(lock.LockKey[1] >> 24), byte(lock.LockKey[1] >> 32), byte(lock.LockKey[1] >> 40), byte(lock.LockKey[1] >> 48), byte(lock.LockKey[1] >> 56)

    self.buf[buf_index+21], self.buf[buf_index+22], self.buf[buf_index+23], self.buf[buf_index+24], self.buf[buf_index+25], self.buf[buf_index+26], self.buf[buf_index+27], self.buf[buf_index+28] = byte(lock.LockId[0]), byte(lock.LockId[0] >> 8), byte(lock.LockId[0] >> 16), byte(lock.LockId[0] >> 24), byte(lock.LockId[0] >> 32), byte(lock.LockId[0] >> 40), byte(lock.LockId[0] >> 48), byte(lock.LockId[0] >> 56)
    self.buf[buf_index+29], self.buf[buf_index+30], self.buf[buf_index+31], self.buf[buf_index+32], self.buf[buf_index+33], self.buf[buf_index+34], self.buf[buf_index+35], self.buf[buf_index+36] = byte(lock.LockId[1]), byte(lock.LockId[1] >> 8), byte(lock.LockId[1] >> 16), byte(lock.LockId[1] >> 24), byte(lock.LockId[1] >> 32), byte(lock.LockId[1] >> 40), byte(lock.LockId[1] >> 48), byte(lock.LockId[1] >> 56)

    self.buf[buf_index+37], self.buf[buf_index+38], self.buf[buf_index+39], self.buf[buf_index+40], self.buf[buf_index+40], self.buf[buf_index+42], self.buf[buf_index+43], self.buf[buf_index+44] = byte(lock.StartTime), byte(lock.StartTime >> 8), byte(lock.StartTime >> 16), byte(lock.StartTime >> 24), byte(lock.StartTime >> 32), byte(lock.StartTime >> 40), byte(lock.StartTime >> 48), byte(lock.StartTime >> 56)
    self.buf[buf_index+45], self.buf[buf_index+46], self.buf[buf_index+47], self.buf[buf_index+48], self.buf[buf_index+49], self.buf[buf_index+50], self.buf[buf_index+51], self.buf[buf_index+52] = byte(lock.ExpriedTime), byte(lock.ExpriedTime >> 8), byte(lock.ExpriedTime >> 16), byte(lock.ExpriedTime >> 24), byte(lock.ExpriedTime >> 32), byte(lock.ExpriedTime >> 40), byte(lock.ExpriedTime >> 48), byte(lock.ExpriedTime >> 56)

    self.buf[buf_index+53], self.buf[buf_index+54] = byte(lock.Count), byte(lock.Count << 8)
    self.buf[buf_index + 55] = lock.Rcount

    self.buf_index += buf_len
    return nil
}

func (self *AofFile) Flush() error {
    if self.buf_index == 0 {
        return nil
    }

    if self.file == nil {
        return errors.New("Aof File is Closed")
    }

    n, err := self.file.Write(self.buf[:self.buf_index])
    if err != nil {
        return nil
    }

    err = self.file.Sync()
    if err == nil {
        self.buf_index -= n
    }
    return err
}

func (self *AofFile) Close() error {
    err := self.file.Close()
    if err == nil {
        self.file = nil
    }
    return err
}


func (self *AofFile) Lock() {
    self.glock.Lock()
}

func (self *AofFile) Unlock() {
    self.glock.Unlock()
}

type AofChannel struct {
    slock           *SLock
    aof             *Aof
    lock_db         *LockDB
    channel         chan *AofLock
    free_locks      []*AofLock
    free_lock_index int
}

func (self *AofChannel) Push(lock *Lock, command_type uint8) error {
    var aof_lock *AofLock
    if self.free_lock_index < 0 {
        aof_lock = &AofLock{lock.manager.db_id, command_type, lock.command.Flag, lock.command.LockKey, lock.command.LockId,
            uint64(lock.start_time), uint64(lock.expried_time), lock.command.Count, lock.command.Rcount}
    } else {
        aof_lock = self.free_locks[self.free_lock_index]
        self.free_lock_index--
        aof_lock.DbId = lock.manager.db_id
        aof_lock.CommandType = command_type
        aof_lock.Flag = lock.command.Flag
        aof_lock.LockKey = lock.command.LockKey
        aof_lock.LockId = lock.command.LockId
        aof_lock.StartTime = uint64(lock.start_time)
        aof_lock.ExpriedTime = uint64(lock.expried_time)
        aof_lock.Count = lock.command.Count
        aof_lock.Rcount = lock.command.Rcount
    }

    self.channel <- aof_lock
    return nil
}

func (self *AofChannel) Handle() {
    self.aof.ActiveChannel(self)
    for {
        select {
        case aof_lock := <- self.channel:
            self.aof.PushLock(aof_lock)
        default:
            self.aof.UnActiveChannel(self)
            if self.aof.is_stop {
                return
            }
            aof_lock := <- self.channel
            self.aof.PushLock(aof_lock)
        }
    }
}

type Aof struct {
    slock                   *SLock
    glock                   *sync.Mutex
    data_dir                string
    current_file            *AofFile
    channels                []*AofChannel
    actived_channel_count   int
    is_stop                 bool
    close_waiter            chan bool
}

func NewAof() *Aof {
    return &Aof{nil, &sync.Mutex{}, "",nil, make([]*AofChannel, 0), 0, false, nil}
}

func (self *Aof) LoadAndInit() error {
    data_dir, err := filepath.Abs(Config.DataDir)
    if err != nil {
        return err
    }

    self.data_dir = data_dir
    if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
        err = os.Mkdir(self.data_dir, 0755)
        if err != nil {
            return err
        }
    }

    self.current_file = NewAofFile(self, filepath.Join(self.data_dir, "append.aof.1"), os.O_WRONLY, 4096)
    err = self.current_file.Open()
    if err != nil {
        return err
    }
    return nil
}

func (self *Aof) Close()  {
    self.glock.Lock()
    if self.is_stop {
        self.glock.Unlock()
        return
    }

    self.is_stop = true
    if self.actived_channel_count > 0 {
        self.close_waiter = make(chan bool, 1)
        self.glock.Unlock()
        <- self.close_waiter
        self.glock.Lock()
        self.close_waiter = nil
        self.current_file.Close()
        self.current_file = nil
    }
    self.glock.Unlock()
}

func (self *Aof) NewAofChannel(lock_db *LockDB) *AofChannel {
    aof_channel := &AofChannel{self.slock, self, lock_db, make(chan *AofLock, 4096), make([]*AofLock, 64), 63}
    aof_channel.Handle()
    return aof_channel
}

func (self *Aof) ActiveChannel(channel *AofChannel) {
    self.glock.Lock()
    self.actived_channel_count++
    self.glock.Unlock()
}

func (self *Aof) UnActiveChannel(channel *AofChannel) {
    self.glock.Lock()
    self.actived_channel_count--
    if self.actived_channel_count == 0 {
        self.glock.Unlock()

        self.current_file.Lock()
        for ; !self.is_stop; {
            ferr := self.current_file.Flush()
            if ferr == nil {
                break
            }
            time.Sleep(1e10)
        }
        self.current_file.Unlock()

        self.glock.Lock()
        if self.is_stop {
            self.close_waiter <- true
        }
    }
    self.glock.Unlock()
}

func (self *Aof) PushLock(lock *AofLock) {
    self.current_file.Lock()
    err := self.current_file.WriteLock(lock)
    if err != nil {
        for ; !self.is_stop; {
            ferr := self.current_file.Flush()
            if ferr == nil {
                break
            }
            time.Sleep(1e10)
        }
    }
    self.current_file.Unlock()
}