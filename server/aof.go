package server

import (
    "bufio"
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
    buf         []byte
    rbuf        *bufio.Reader
    wbuf        *bufio.Writer
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile{
    return &AofFile{aof.slock, aof, filename, nil, mode, make([]byte, 64), nil, nil}
}

func (self *AofFile) Open() error {
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
        self.wbuf = bufio.NewWriterSize(self.file, 4096)
        err = self.WriteHeader()
        if err != nil {
            self.file.Close()
            return err
        }
    } else {
        self.rbuf = bufio.NewReaderSize(self.file, 4096)
        err = self.ReadHeader()
        if err != nil {
            self.file.Close()
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

    return nil
}

func (self *AofFile) WriteHeader() error {
    self.buf[0], self.buf[1], self.buf[2], self.buf[3], self.buf[4], self.buf[5], self.buf[6], self.buf[6] = 'S', 'L', 'O', 'C', 'K', 'A', 'O', 'F'
    self.buf[8], self.buf[9], self.buf[10], self.buf[11] = 0x01, 0x00, 0x00, 0x00
    n, err := self.wbuf.Write(self.buf[:12])
    if n != 12 {
        return err
    }
    return self.wbuf.Flush()
}

func (self *AofFile) ReadLock(lock *AofLock) error {
    n, err := self.rbuf.Read(self.buf[:2])
    if err != nil {
        return err
    }
    
    if n != 2 {
        return errors.New("Lock Len error")
    }
    
    lock_len := uint16(self.buf[0]) | uint16(self.buf[1] << 8)
    n, err = self.rbuf.Read(self.buf[:lock_len])
    if err != nil {
        return err
    }

    if n != 2 {
        return errors.New("Lock Len error")
    }

    lock.DbId, lock.CommandType, lock.Flag = self.buf[2], self.buf[3], self.buf[4]

    lock.LockKey[0] = uint64(self.buf[5]) | uint64(self.buf[6])<<8 | uint64(self.buf[7])<<16 | uint64(self.buf[8])<<24 | uint64(self.buf[9])<<32 | uint64(self.buf[10])<<40 | uint64(self.buf[11])<<48 | uint64(self.buf[12])<<56
    lock.LockKey[1] = uint64(self.buf[13]) | uint64(self.buf[14])<<8 | uint64(self.buf[15])<<16 | uint64(self.buf[16])<<24 | uint64(self.buf[17])<<32 | uint64(self.buf[18])<<40 | uint64(self.buf[19])<<48 | uint64(self.buf[20])<<56

    lock.LockId[0] = uint64(self.buf[21]) | uint64(self.buf[22])<<8 | uint64(self.buf[23])<<16 | uint64(self.buf[24])<<24 | uint64(self.buf[25])<<32 | uint64(self.buf[26])<<40 | uint64(self.buf[27])<<48 | uint64(self.buf[28])<<56
    lock.LockId[1] = uint64(self.buf[29]) | uint64(self.buf[30])<<8 | uint64(self.buf[31])<<16 | uint64(self.buf[32])<<24 | uint64(self.buf[33])<<32 | uint64(self.buf[34])<<40 | uint64(self.buf[35])<<48 | uint64(self.buf[36])<<56

    lock.StartTime = uint64(self.buf[37]) | uint64(self.buf[38])<<8 | uint64(self.buf[39])<<16 | uint64(self.buf[40])<<24 | uint64(self.buf[41])<<32 | uint64(self.buf[42])<<40 | uint64(self.buf[43])<<48 | uint64(self.buf[44])<<56
    lock.ExpriedTime = uint64(self.buf[45]) | uint64(self.buf[46])<<8 | uint64(self.buf[47])<<16 | uint64(self.buf[48])<<24 | uint64(self.buf[49])<<32 | uint64(self.buf[50])<<40 | uint64(self.buf[51])<<48 | uint64(self.buf[52])<<56

    lock.Count = uint16(self.buf[53]) | uint16(self.buf[54])<<8
    lock.Rcount = self.buf[55]

    return nil
}

func (self *AofFile) WriteLock(lock *AofLock) error {
    buf_len := 54

    self.buf[0], self.buf[1] = byte(buf_len), byte(buf_len << 8)
    self.buf[2], self.buf[3], self.buf[4] = lock.DbId, lock.CommandType, lock.Flag

    self.buf[5], self.buf[6], self.buf[7], self.buf[8], self.buf[9], self.buf[10], self.buf[11], self.buf[12] = byte(lock.LockKey[0]), byte(lock.LockKey[0] >> 8), byte(lock.LockKey[0] >> 16), byte(lock.LockKey[0] >> 24), byte(lock.LockKey[0] >> 32), byte(lock.LockKey[0] >> 40), byte(lock.LockKey[0] >> 48), byte(lock.LockKey[0] >> 56)
    self.buf[13], self.buf[14], self.buf[15], self.buf[16], self.buf[17], self.buf[18], self.buf[19], self.buf[20] = byte(lock.LockKey[1]), byte(lock.LockKey[1] >> 8), byte(lock.LockKey[1] >> 16), byte(lock.LockKey[1] >> 24), byte(lock.LockKey[1] >> 32), byte(lock.LockKey[1] >> 40), byte(lock.LockKey[1] >> 48), byte(lock.LockKey[1] >> 56)

    self.buf[21], self.buf[22], self.buf[23], self.buf[24], self.buf[25], self.buf[26], self.buf[27], self.buf[28] = byte(lock.LockId[0]), byte(lock.LockId[0] >> 8), byte(lock.LockId[0] >> 16), byte(lock.LockId[0] >> 24), byte(lock.LockId[0] >> 32), byte(lock.LockId[0] >> 40), byte(lock.LockId[0] >> 48), byte(lock.LockId[0] >> 56)
    self.buf[29], self.buf[30], self.buf[31], self.buf[32], self.buf[33], self.buf[34], self.buf[35], self.buf[36] = byte(lock.LockId[1]), byte(lock.LockId[1] >> 8), byte(lock.LockId[1] >> 16), byte(lock.LockId[1] >> 24), byte(lock.LockId[1] >> 32), byte(lock.LockId[1] >> 40), byte(lock.LockId[1] >> 48), byte(lock.LockId[1] >> 56)

    self.buf[37], self.buf[38], self.buf[39], self.buf[40], self.buf[40], self.buf[42], self.buf[43], self.buf[44] = byte(lock.StartTime), byte(lock.StartTime >> 8), byte(lock.StartTime >> 16), byte(lock.StartTime >> 24), byte(lock.StartTime >> 32), byte(lock.StartTime >> 40), byte(lock.StartTime >> 48), byte(lock.StartTime >> 56)
    self.buf[45], self.buf[46], self.buf[47], self.buf[48], self.buf[49], self.buf[50], self.buf[51], self.buf[52] = byte(lock.ExpriedTime), byte(lock.ExpriedTime >> 8), byte(lock.ExpriedTime >> 16), byte(lock.ExpriedTime >> 24), byte(lock.ExpriedTime >> 32), byte(lock.ExpriedTime >> 40), byte(lock.ExpriedTime >> 48), byte(lock.ExpriedTime >> 56)

    self.buf[53], self.buf[54] = byte(lock.Count), byte(lock.Count << 8)
    self.buf[55] = lock.Rcount

    n, err := self.wbuf.Write(self.buf[:buf_len])
    if err != nil {
        return err
    }

    if n != buf_len {
        return errors.New("Write buf error")
    }
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
    aof_file                *AofFile
    aof_file_glock          *sync.Mutex
    channels                []*AofChannel
    actived_channel_count   int
    is_stop                 bool
    close_waiter            chan bool
}

func NewAof() *Aof {
    return &Aof{nil, &sync.Mutex{}, "",nil, &sync.Mutex{}, make([]*AofChannel, 0), 0, false, nil}
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