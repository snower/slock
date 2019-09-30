package server

import (
    "bufio"
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
    "io"
    "math/rand"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "sync"
    "sync/atomic"
    "time"
)

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var request_id_index uint64 = 0

type AofLock struct {
    DbId            uint8
    CommandType     uint8
    Flag            uint8
    LockKey         [16]byte
    LockId          [16]byte
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
    buf_size    int
    buf         []byte
    rbuf        *bufio.Reader
    wbuf        *bufio.Writer
    size        int
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile{
    return &AofFile{aof.slock, aof, filename, nil, mode, buf_size, make([]byte, 64), nil, nil, 0}
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
        self.wbuf = bufio.NewWriterSize(self.file, self.buf_size)
        err = self.WriteHeader()
        if err != nil {
            self.file.Close()
            return err
        }
    } else {
        self.rbuf = bufio.NewReaderSize(self.file, self.buf_size)
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

    if string(self.buf[:8]) == "SLOCKAOF" {
        return errors.New("File is not AOF File")
    }

    version := uint16(self.buf[8]) | uint16(self.buf[9])<<8
    if version != 0x0001 {
        return errors.New("AOF File Unknown Version")
    }

    header_len := uint16(self.buf[10]) | uint16(self.buf[11])<<8
    if header_len != 0x0000 {
        return errors.New("AOF File Header Len Error")
    }

    if header_len > 0 {
        n, err := self.rbuf.Read(make([]byte, header_len))
        if err != nil {
            return err
        }

        if n != int(header_len) {
            return errors.New("File is not AOF FIle")
        }
    }

    self.size += 12 + int(header_len)
    return nil
}

func (self *AofFile) WriteHeader() error {
    self.buf[0], self.buf[1], self.buf[2], self.buf[3], self.buf[4], self.buf[5], self.buf[6], self.buf[6] = 'S', 'L', 'O', 'C', 'K', 'A', 'O', 'F'
    self.buf[8], self.buf[9], self.buf[10], self.buf[11] = 0x01, 0x00, 0x00, 0x00
    n, err := self.wbuf.Write(self.buf[:12])
    if n != 12 {
        return err
    }

    self.size += 12
    return self.wbuf.Flush()
}

func (self *AofFile) ReadLock(lock *AofLock) error {
    buf := self.buf
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    n, err := self.rbuf.Read(buf[:56])
    if err != nil {
        return err
    }
    
    lock_len := uint16(buf[0]) | uint16(buf[1])<<8
    if n != int(lock_len) + 2 {
        nn, nerr := self.rbuf.Read(buf[n:56])
        if nerr != nil {
            return err
        }
        n += nn
        if n != int(lock_len) + 2 {
            return errors.New("Lock Len error")
        }
    }

    lock.DbId, lock.CommandType, lock.Flag = buf[2], buf[3], buf[4]

    lock.LockKey[0], lock.LockKey[1], lock.LockKey[2], lock.LockKey[3], lock.LockKey[4], lock.LockKey[5], lock.LockKey[6], lock.LockKey[7],
        lock.LockKey[8], lock.LockKey[9], lock.LockKey[10], lock.LockKey[11], lock.LockKey[12], lock.LockKey[13], lock.LockKey[14], lock.LockKey[15] =
        buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
        buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19], buf[20]

    lock.LockId[0], lock.LockId[1], lock.LockId[2], lock.LockId[3], lock.LockId[4], lock.LockId[5], lock.LockId[6], lock.LockId[7],
        lock.LockId[8], lock.LockId[9], lock.LockId[10], lock.LockId[11], lock.LockId[12], lock.LockId[13], lock.LockId[14], lock.LockId[15] =
        buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

    lock.StartTime = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
    lock.ExpriedTime = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

    lock.Count = uint16(buf[53]) | uint16(buf[54])<<8
    lock.Rcount = buf[55]

    self.size += 2 + int(lock_len)
    return nil
}

func (self *AofFile) WriteLock(lock *AofLock) error {
    buf := self.buf
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    buf_len := 54

    buf[0], buf[1] = byte(buf_len), byte(buf_len << 8)
    buf[2], buf[3], buf[4] = lock.DbId, lock.CommandType, lock.Flag

    buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
        buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19], buf[20] =
        lock.LockKey[0], lock.LockKey[1], lock.LockKey[2], lock.LockKey[3], lock.LockKey[4], lock.LockKey[5], lock.LockKey[6], lock.LockKey[7],
        lock.LockKey[8], lock.LockKey[9], lock.LockKey[10], lock.LockKey[11], lock.LockKey[12], lock.LockKey[13], lock.LockKey[14], lock.LockKey[15]

    buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36] =
        lock.LockId[0], lock.LockId[1], lock.LockId[2], lock.LockId[3], lock.LockId[4], lock.LockId[5], lock.LockId[6], lock.LockId[7],
        lock.LockId[8], lock.LockId[9], lock.LockId[10], lock.LockId[11], lock.LockId[12], lock.LockId[13], lock.LockId[14], lock.LockId[15]

    buf[37], buf[38], buf[39], buf[40], buf[40], buf[42], buf[43], buf[44] = byte(lock.StartTime), byte(lock.StartTime >> 8), byte(lock.StartTime >> 16), byte(lock.StartTime >> 24), byte(lock.StartTime >> 32), byte(lock.StartTime >> 40), byte(lock.StartTime >> 48), byte(lock.StartTime >> 56)
    buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52] = byte(lock.ExpriedTime), byte(lock.ExpriedTime >> 8), byte(lock.ExpriedTime >> 16), byte(lock.ExpriedTime >> 24), byte(lock.ExpriedTime >> 32), byte(lock.ExpriedTime >> 40), byte(lock.ExpriedTime >> 48), byte(lock.ExpriedTime >> 56)

    buf[53], buf[54] = byte(lock.Count), byte(lock.Count << 8)
    buf[55] = lock.Rcount

    n, err := self.wbuf.Write(buf[:buf_len + 2])
    if err != nil {
        return err
    }

    if n != buf_len + 2 {
        return errors.New("Write buf error")
    }

    self.size += buf_len + 2
    return nil
}

func (self *AofFile) Flush() error {
    if self.file == nil {
        return nil
    }

    err := self.wbuf.Flush()
    if err != nil {
        return err
    }

    err = self.file.Sync()
    if err != nil {
        return err
    }
    return nil
}

func (self *AofFile) Close() error {
    err := self.file.Close()
    if err == nil {
        self.file = nil
        self.wbuf = nil
        self.rbuf = nil
    }
    return err
}

func (self *AofFile) GetSize() int {
    return self.size
}

type AofChannel struct {
    slock           *SLock
    aof             *Aof
    lock_db         *LockDB
    channel         chan *AofLock
    free_locks      []*AofLock
    free_lock_index int32
    free_lock_max   int32
    closed          bool
    is_stop         bool
}

func (self *AofChannel) Push(lock *Lock, command_type uint8) error {
    if self.is_stop {
        return errors.New("Closed")
    }

    var aof_lock *AofLock
    free_lock_index := self.free_lock_index
    if free_lock_index >= 0 && atomic.CompareAndSwapInt32(&self.free_lock_index, free_lock_index, free_lock_index - 1) {
        aof_lock = self.free_locks[free_lock_index]
        aof_lock.DbId = lock.manager.db_id
        aof_lock.CommandType = command_type
        aof_lock.Flag = lock.command.Flag
        aof_lock.LockKey = lock.command.LockKey
        aof_lock.LockId = lock.command.LockId
        aof_lock.StartTime = uint64(lock.start_time)
        aof_lock.ExpriedTime = uint64(lock.expried_time)
        aof_lock.Count = lock.command.Count
        aof_lock.Rcount = lock.command.Rcount
    } else {
        aof_lock = &AofLock{lock.manager.db_id, command_type, lock.command.Flag, lock.command.LockKey, lock.command.LockId,
            uint64(lock.start_time), uint64(lock.expried_time), lock.command.Count, lock.command.Rcount}
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
            free_lock_index := self.free_lock_index
            if self.free_lock_index < self.free_lock_max && atomic.CompareAndSwapInt32(&self.free_lock_index, free_lock_index, free_lock_index + 1) {
                self.free_locks[free_lock_index+1] = aof_lock
            }
        default:
            self.aof.UnActiveChannel(self)
            if self.closed {
                self.is_stop = true
                return
            }
            aof_lock := <- self.channel
            if aof_lock == nil {
                if self.closed {
                    self.is_stop = true
                    return
                }
                continue
            }

            self.aof.ActiveChannel(self)
            self.aof.PushLock(aof_lock)
            free_lock_index := self.free_lock_index
            if self.free_lock_index < self.free_lock_max && atomic.CompareAndSwapInt32(&self.free_lock_index, free_lock_index, free_lock_index + 1) {
                self.free_locks[free_lock_index+1] = aof_lock
            }
        }
    }
}

type Aof struct {
    slock                   *SLock
    glock                   *sync.Mutex
    data_dir                string
    aof_file_index          int
    aof_file                *AofFile
    aof_file_glock          *sync.Mutex
    channels                []*AofChannel
    channel_count           int
    actived_channel_count   int
    is_stop                 bool
    close_waiter            chan bool
    rewrite_size            int
    is_rewriting            bool
    aof_lock_count          uint64
}

func NewAof() *Aof {
    return &Aof{nil, &sync.Mutex{}, "",0, nil, &sync.Mutex{}, make([]*AofChannel, 0),
        0,0, false, nil, 0, false, 0}
}

func (self *Aof) LoadAndInit() error {
    self.rewrite_size = int(Config.AofFileRewriteSize)
    data_dir, err := filepath.Abs(Config.DataDir)
    if err != nil {
        return err
    }

    self.data_dir = data_dir
    if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
        return err
    }

    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return err
    }

    if len(append_files) > 0 {
        aof_file_index, err := strconv.Atoi(append_files[len(append_files) -1][11:])
        if err != nil {
            return err
        }
        self.aof_file_index = aof_file_index
    }

    self.aof_file = NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index + 1)), os.O_WRONLY, int(Config.AofFileBufferSize))
    err = self.aof_file.Open()
    if err != nil {
        return err
    }
    self.aof_file_index++
    self.slock.Log().Infof("Aof File Create %s", self.aof_file.filename)

    server_protocol := &BinaryServerProtocol{self.slock, &sync.Mutex{}, nil, [16]byte{}, NewLockCommandQueue(4, 16, 256),
        NewLockCommandQueue(4, 16, 256), make([]byte, 64), make([]byte, 64), 0, false, true}

    if rewrite_file != "" {
        err := self.LoadAofFile(rewrite_file, server_protocol)
        if err != nil {
            return err
        }
        self.slock.Log().Infof("Aof File Load %s", filepath.Join(self.data_dir, rewrite_file))
    }

    for _, append_file := range append_files {
        err := self.LoadAofFile(append_file, server_protocol)
        if err != nil {
            return err
        }
        self.slock.Log().Infof("Aof File Load %s", filepath.Join(self.data_dir, append_file))
    }

    if len(append_files) > 0 {
        go self.RewriteAofFile()
    }
    return nil
}

func (self *Aof) FindAofFiles() ([]string, string, error) {
    append_files := make([]string, 0)
    rewrite_file := ""

    err := filepath.Walk(self.data_dir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        if info.IsDir() {
            return nil
        }

        file_name := info.Name()
        if len(file_name) >= 11 && file_name[:10] == "append.aof" {
            append_files = append(append_files, file_name)
        } else if file_name == "rewrite.aof" {
            rewrite_file = file_name
        }
        return nil
    })
    if err != nil {
        return nil, "", err
    }

    sort.Strings(append_files)
    return append_files, rewrite_file, nil
}

func (self *Aof) LoadAofFile(filename string, server_protocol *BinaryServerProtocol) error {
    aof_file := NewAofFile(self, filepath.Join(self.data_dir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
    err := aof_file.Open()
    if err != nil {
        return err
    }

    lock := &AofLock{}
    now := time.Now().Unix()
    for {
        err := aof_file.ReadLock(lock)
        if err == io.EOF {
            return nil
        }

        if err != nil {
            return err
        }

        if int64(lock.ExpriedTime) <= now {
            continue
        }

        command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: lock.CommandType, RequestId: self.GetRequestId()},
            Flag: lock.Flag, DbId: lock.DbId, LockId: lock.LockId, LockKey: lock.LockKey, TimeoutFlag: 0, Timeout: 5,
            ExpriedFlag: 0x1200, Expried: uint16(int64(lock.ExpriedTime) - now), Count: lock.Count, Rcount: lock.Rcount}
        err = server_protocol.ProcessLockCommand(command)
        if err != nil {
            return err
        }
    }
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
        self.aof_file.Close()
        self.aof_file = nil
    }
    self.glock.Unlock()
}

func (self *Aof) NewAofChannel(lock_db *LockDB) *AofChannel {
    self.glock.Lock()
    aof_channel := &AofChannel{self.slock, self, lock_db, make(chan *AofLock, Config.AofQueueSize), make([]*AofLock, Config.AofQueueSize), 63, int32(Config.AofQueueSize - 1), false, false}
    for i :=0; i < 64; i++ {
        aof_channel.free_locks[i] = &AofLock{}
    }
    go aof_channel.Handle()
    self.channel_count++
    self.glock.Unlock()
    return aof_channel
}

func (self *Aof) CloseAofChannel(aof_channel *AofChannel) *AofChannel {
    self.glock.Lock()
    aof_channel.channel <- nil
    self.channel_count--
    aof_channel.closed = true
    aof_channel.lock_db = nil
    self.glock.Unlock()
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

        self.aof_file_glock.Lock()
        self.Flush()
        self.aof_file_glock.Unlock()

        self.glock.Lock()
        if self.is_stop {
            self.close_waiter <- true
        }
    }
    self.glock.Unlock()
}

func (self *Aof) PushLock(lock *AofLock) {
    self.aof_file_glock.Lock()
    err := self.aof_file.WriteLock(lock)
    if err != nil {
        for ; !self.is_stop; {
            self.slock.Log().Errorf("Aof File Write Error %v", err)
            time.Sleep(1e10)

            err := self.aof_file.WriteLock(lock)
            if err == nil {
                break
            }
        }
    }
    self.aof_lock_count++


    if self.aof_file.GetSize() >= self.rewrite_size {
        self.Flush()

        err := self.aof_file.Close()
        if err != nil {
            self.slock.Log().Errorf("Aof File Close Error %s %v", self.aof_file.filename, err)
        }

        for ; !self.is_stop; {
            aof_file := NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index + 1)), os.O_WRONLY, int(Config.AofFileBufferSize))
            err := aof_file.Open()
            if err != nil {
                time.Sleep(1e10)
                continue
            }
            self.aof_file = aof_file
            self.aof_file_index++
            self.slock.Log().Infof("Aof File Create %s", filepath.Join(self.data_dir, self.aof_file.filename))

            go self.RewriteAofFile()
            break
        }
    }
    self.aof_file_glock.Unlock()
}

func (self *Aof) Flush() {
    for ; !self.is_stop; {
        err := self.aof_file.Flush()
        if err != nil {
            self.slock.Log().Errorf("Aof File Flush Error %v", err)
            time.Sleep(1e10)
        }
        break
    }
}

func (self *Aof) RewriteAofFile() {
    self.glock.Lock()
    if self.is_rewriting{
        self.glock.Unlock()
        return
    }
    self.is_rewriting = true
    self.glock.Unlock()

    defer func() {
        self.glock.Lock()
        self.is_rewriting = false
        self.glock.Unlock()
    }()

    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return
    }

    if len(append_files) == 0 && rewrite_file == "" {
        return
    }

    total_aof_size := 0
    rewrite_aof_file := NewAofFile(self, filepath.Join(self.data_dir, "rewrite.aof.tmp"), os.O_WRONLY, int(Config.AofFileBufferSize))
    err = rewrite_aof_file.Open()
    if err != nil {
        return
    }

    if rewrite_file != "" {
        aof_size, err := self.LoadRewriteAofFile(rewrite_file, rewrite_aof_file)
        if err != nil {
            return
        }

        total_aof_size += aof_size
    }

    for _, append_file := range append_files {
        aof_file_index, err := strconv.Atoi(append_file[11:])
        if err != nil {
            return
        }

        if aof_file_index >= self.aof_file_index {
            continue
        }

        aof_size, err := self.LoadRewriteAofFile(append_file, rewrite_aof_file)
        if err != nil {
            return
        }

        total_aof_size += aof_size
    }

    if rewrite_aof_file.Flush() != nil {
        return
    }

    err = rewrite_aof_file.Close()
    if err != nil {
        self.slock.Log().Errorf("Aof Rewrite Close File Error %s %v", filepath.Join(self.data_dir, "rewrite.aof.tmp"), err)
    }

    if rewrite_file != "" {
        err := os.Remove(filepath.Join(self.data_dir, rewrite_file))
        if err != nil {
            self.slock.Log().Errorf("Aof Rewrite Remove File Error %s %v", filepath.Join(self.data_dir, rewrite_file), err)
        }
    }

    for _, append_file := range append_files {
        aof_file_index, err := strconv.Atoi(append_file[11:])
        if err != nil {
            self.slock.Log().Errorf("Aof Rewrite Find Max File Error %s %v", filepath.Join(self.data_dir, append_file), err)
        }

        if aof_file_index >= self.aof_file_index {
            continue
        }

        err = os.Remove(filepath.Join(self.data_dir, append_file))
        if err != nil {
            self.slock.Log().Errorf("Aof Rewrite Remove File Error %s %v", filepath.Join(self.data_dir, append_file), err)
        }else{
            self.slock.Log().Infof("Aof Rewrite Remove File %s", filepath.Join(self.data_dir, append_file))
        }
    }
    err = os.Rename(filepath.Join(self.data_dir, "rewrite.aof.tmp"), filepath.Join(self.data_dir, "rewrite.aof"))
    if err != nil {
        self.slock.Log().Errorf("Aof Rewrite Rename Error %s %v", filepath.Join(self.data_dir, "rewrite.aof.tmp"), err)
    }
    self.slock.Log().Infof("Aof Rewrite %d to %d", total_aof_size, rewrite_aof_file.GetSize())
}

func (self *Aof) LoadRewriteAofFile(filename string, rewrite_aof_file *AofFile) (int, error) {
    aof_file := NewAofFile(self, filepath.Join(self.data_dir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
    err := aof_file.Open()
    if err != nil {
        return 0, err
    }

    lock := &AofLock{}
    now := time.Now().Unix()
    for {
        err := aof_file.ReadLock(lock)
        if err == io.EOF {
            return aof_file.GetSize(), nil
        }

        if err != nil {
            return 0, err
        }

        if int64(lock.ExpriedTime) <= now {
            continue
        }

        err = rewrite_aof_file.WriteLock(lock)
        if err != nil {
            return 0, err
        }
    }
}

func (self *Aof) GetRequestId() [16]byte {
    now := uint32(time.Now().Unix())
    request_id_index := atomic.AddUint64(&request_id_index, 1)
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
    }
}