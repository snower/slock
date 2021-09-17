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
    CommandType     uint8
    AofIndex        uint32
    AofId           uint32
    CommandTime     uint64
    Flag            uint8
    DbId            uint8
    LockId          [16]byte
    LockKey         [16]byte
    AofFlag         uint16
    StartTime       uint16
    ExpriedFlag     uint16
    ExpriedTime     uint16
    Count           uint16
    Rcount          uint8
    LockType        uint8
    buf             []byte
    lock            *Lock
}

func NewAofLock() *AofLock {
    return &AofLock{0, 0, 0, 0, 0, 0,  [16]byte{},
        [16]byte{}, 0, 0, 0, 0, 0, 0, 0, make([]byte, 64), nil}

}

func (self *AofLock) GetBuf() []byte {
    return self.buf
}

func (self *AofLock) Decode() error {
    buf := self.buf
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    self.CommandType = buf[2]

    self.AofId, self.AofIndex = uint32(buf[3]) | uint32(buf[4])<<8 | uint32(buf[5])<<16 | uint32(buf[6])<<24, uint32(buf[7]) | uint32(buf[8])<<8 | uint32(buf[9])<<16 | uint32(buf[10])<<24
    self.CommandTime = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Flag, self.DbId = buf[19], buf[20]

    self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15] =
        buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

    self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15] =
        buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
        buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]


    self.StartTime, self.AofFlag, self.ExpriedTime, self.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8

    self.Count = uint16(buf[61]) | uint16(buf[62])<<8
    self.Rcount = buf[63]
    
    return nil
}

func (self *AofLock) Encode() error {
    buf := self.buf
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    buf[2] = self.CommandType

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.AofId), byte(self.AofId >> 8), byte(self.AofId >> 16), byte(self.AofId >> 24), byte(self.AofIndex), byte(self.AofIndex >> 8), byte(self.AofIndex >> 16), byte(self.AofIndex >> 24)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.CommandTime), byte(self.CommandTime >> 8), byte(self.CommandTime >> 16), byte(self.CommandTime >> 24), byte(self.CommandTime >> 32), byte(self.CommandTime >> 40), byte(self.CommandTime >> 48), byte(self.CommandTime >> 56)

    buf[19], buf[20] = self.Flag, self.DbId

    buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36] =
        self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15]

    buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
        buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52] =
        self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15]

    buf[53], buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60] = byte(self.StartTime), byte(self.StartTime >> 8), byte(self.AofFlag), byte(self.AofFlag >> 8), byte(self.ExpriedTime), byte(self.ExpriedTime >> 8), byte(self.ExpriedFlag), byte(self.ExpriedFlag >> 8)

    buf[61], buf[62] = byte(self.Count), byte(self.Count >> 8)
    buf[63] = self.Rcount
    
    return nil
}

func (self *AofLock) UpdateAofIndexId(aof_index uint32, aof_id uint32) error {
    self.AofIndex = aof_index
    self.AofId = aof_id

    buf := self.buf
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.AofId), byte(self.AofId >> 8), byte(self.AofId >> 16), byte(self.AofId >> 24), byte(self.AofIndex), byte(self.AofIndex >> 8), byte(self.AofIndex >> 16), byte(self.AofIndex >> 24)
    return nil
}

func (self *AofLock) GetRequestId() [16]byte {
    request_id := [16]byte{}
    request_id[0], request_id[1], request_id[2], request_id[3], request_id[4], request_id[5], request_id[6], request_id[7] = byte(self.AofId), byte(self.AofId >> 8), byte(self.AofId >> 16), byte(self.AofId >> 24), byte(self.AofIndex), byte(self.AofIndex >> 8), byte(self.AofIndex >> 16), byte(self.AofIndex >> 24)
    request_id[8], request_id[9], request_id[10], request_id[11], request_id[12], request_id[13], request_id[14], request_id[15] = byte(self.CommandTime), byte(self.CommandTime >> 8), byte(self.CommandTime >> 16), byte(self.CommandTime >> 24), byte(self.CommandTime >> 32), byte(self.CommandTime >> 40), byte(self.CommandTime >> 48), byte(self.CommandTime >> 56)
    return request_id
}

type AofFileAckRequest struct {
    db_id       uint8
    request_id  [16]byte
}

type AofFile struct {
    slock           *SLock
    aof             *Aof
    filename        string
    file            *os.File
    mode            int
    buf_size        int
    buf             []byte
    rbuf            *bufio.Reader
    wbuf            []byte
    windex          int
    size            int
    ack_requests    []AofFileAckRequest
    ack_index       int
}

func NewAofFile(aof *Aof, filename string, mode int, buf_size int) *AofFile{
    buf_size = buf_size - buf_size % 64
    ack_requests := make([]AofFileAckRequest, buf_size / 64)
    return &AofFile{aof.slock, aof, filename, nil, mode, buf_size,
        make([]byte, 64), nil, nil, 0, 0, ack_requests, 0}
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
        self.wbuf = make([]byte, self.buf_size)
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
    n, err := self.file.Write(self.buf[:12])
    if n != 12 {
        return err
    }

    self.size += 12
    return nil
}

func (self *AofFile) ReadLock(lock *AofLock) error {
    buf := lock.GetBuf()
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }

    n, err := self.rbuf.Read(buf)
    if err != nil {
        return err
    }
    
    lock_len := uint16(buf[0]) | uint16(buf[1])<<8
    if n != int(lock_len) + 2 {
        nn, nerr := self.rbuf.Read(buf[n:64])
        if nerr != nil {
            return err
        }
        n += nn
        if n != int(lock_len) + 2 {
            return errors.New("Lock Len error")
        }
    }

    self.size += 2 + int(lock_len)
    return nil
}

func (self *AofFile) WriteLock(lock *AofLock) error {
    buf := lock.GetBuf()
    if len(buf) < 64 {
        return errors.New("Buffer Len error")
    }
    buf[0], buf[1] = 62, 0

    copy(self.wbuf[self.windex:], buf)
    self.windex += 64
    if self.windex >= len(self.wbuf) {
        err := self.Flush()
        if err != nil {
            return err
        }
    }

    self.size += 64
    if lock.AofFlag & 0x1000 != 0 {
        self.ack_requests[self.ack_index].db_id = lock.DbId
        self.ack_requests[self.ack_index].request_id = lock.GetRequestId()
        self.ack_index++
    }
    return nil
}

func (self *AofFile) Flush() error {
    if self.file == nil {
        return nil
    }

    tn := 0
    for ; tn < self.windex; {
        n, err := self.file.Write(self.wbuf[tn:self.windex])
        if err != nil {
            return err
        }
        tn += n
    }
    self.windex = 0

    err := self.file.Sync()
    if err != nil {
        return err
    }

    for i:= 0; i < self.ack_index; i++ {
        self.aof.AofFileFlushRequested(&self.ack_requests[i], true)
    }
    self.ack_index = 0
    return nil
}

func (self *AofFile) Close() error {
    if self.ack_index > 0 {
        for i:= 0; i < self.ack_index; i++ {
            self.aof.AofFileFlushRequested(&self.ack_requests[i], false)
        }
        self.ack_index = 0
    }

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
    glock           *sync.Mutex
    aof             *Aof
    lock_db         *LockDB
    channel         chan *AofLock
    server_protocol ServerProtocol
    free_locks      []*AofLock
    free_lock_index int32
    free_lock_max   int32
    buf             []byte
    closed          bool
    is_stop         bool
}

func (self *AofChannel) Push(lock *Lock, command_type uint8) error {
    if self.is_stop {
        return errors.New("Aof Channel Closed")
    }

    var aof_lock *AofLock = nil

    self.glock.Lock()
    if self.free_lock_index > 0 {
        self.free_lock_index--
        aof_lock = self.free_locks[self.free_lock_index]
    }
    self.glock.Unlock()

    if aof_lock != nil {
        aof_lock.CommandType = command_type
        aof_lock.AofIndex = 0
        aof_lock.AofId = 0
        if lock.expried_time > self.lock_db.current_time {
            aof_lock.CommandTime = uint64(self.lock_db.current_time)
        } else {
            aof_lock.CommandTime = uint64(lock.expried_time)
        }
        aof_lock.Flag = lock.command.Flag
        aof_lock.DbId = lock.manager.db_id
        aof_lock.LockId = lock.command.LockId
        aof_lock.LockKey = lock.command.LockKey
        aof_lock.AofFlag = 0
        if aof_lock.CommandTime - uint64(lock.start_time) > 0xffff {
            aof_lock.StartTime = 0
        } else {
            aof_lock.StartTime = uint16(aof_lock.CommandTime - uint64(lock.start_time))
        }
        aof_lock.ExpriedFlag = lock.command.ExpriedFlag & 0xefff
        if lock.command.ExpriedFlag & 0x4000 == 0 {
            aof_lock.ExpriedTime = uint16(uint64(lock.expried_time) - aof_lock.CommandTime)
        } else {
            aof_lock.ExpriedTime = 0
        }
        aof_lock.Count = lock.command.Count
        aof_lock.Rcount = lock.command.Rcount
        if lock.command.TimeoutFlag & 0x1000 != 0 {
            aof_lock.AofFlag |= 0x1000
            aof_lock.lock = lock
        } else {
            aof_lock.lock = nil
        }
    } else {
        command_time := self.lock_db.current_time
        if lock.expried_time <= command_time {
            command_time = lock.expried_time
        }
        start_time := uint16(0)
        if command_time - lock.start_time <= 0xffff {
            start_time = uint16(command_time - lock.start_time)
        }
        expried_time := uint16(0)
        if lock.command.ExpriedFlag & 0x4000 == 0 {
            expried_time = uint16(lock.expried_time - command_time)
        }
        aof_lock = &AofLock{command_type, 0, 0, uint64(command_time), lock.command.Flag, lock.manager.db_id,  lock.command.LockId,
            lock.command.LockKey, lock.command.ExpriedFlag & 0xefff, 0, start_time, expried_time, lock.command.Count,
            lock.command.Rcount, 0, self.buf, nil}
        if lock.command.TimeoutFlag & 0x1000 != 0 {
            aof_lock.AofFlag |= 0x1000
            aof_lock.lock = lock
        }
    }

    aof_lock.LockType = 0
    self.channel <- aof_lock
    return nil
}

func (self *AofChannel) Load(lock *AofLock) error {
    if self.is_stop {
        return errors.New("Aof Channel Closed")
    }

    var aof_lock *AofLock = nil
    self.glock.Lock()
    if self.free_lock_index > 0 {
        self.free_lock_index--
        aof_lock = self.free_locks[self.free_lock_index]
    }
    self.glock.Unlock()

    if aof_lock != nil {
        aof_lock.CommandType = lock.CommandType
        aof_lock.AofIndex = lock.AofIndex
        aof_lock.AofId = lock.AofId
        aof_lock.CommandTime = lock.CommandTime
        aof_lock.Flag = lock.Flag
        aof_lock.DbId = lock.DbId
        aof_lock.LockId = lock.LockId
        aof_lock.LockKey = lock.LockKey
        aof_lock.AofFlag = lock.AofFlag
        aof_lock.StartTime = lock.StartTime
        aof_lock.ExpriedFlag = lock.ExpriedFlag
        aof_lock.ExpriedTime = lock.ExpriedTime
        aof_lock.Count = lock.Count
        aof_lock.Rcount = lock.Rcount
    } else {
        aof_lock = &AofLock{lock.CommandType, lock.AofIndex, lock.AofId, lock.CommandTime, lock.Flag, lock.DbId,  lock.LockId,
            lock.LockKey, lock.AofFlag, lock.StartTime, lock.ExpriedFlag, lock.ExpriedTime, lock.Count,
            lock.Rcount, 0, self.buf, nil}
    }

    aof_lock.LockType = 1
    self.channel <- aof_lock
    return nil
}

func (self *AofChannel) Handle() {
    self.aof.ChannelRun(self)
    self.aof.ChannelActive(self)
    for {
        select {
        case aof_lock := <- self.channel:
            if aof_lock == nil {
                continue
            }

            self.HandleLock(aof_lock)
        default:
            self.aof.ChannelUnActive(self)
            if self.closed {
                self.DoStop()
                return
            }

            aof_lock := <- self.channel
            if aof_lock == nil {
                if self.closed {
                    self.DoStop()
                    return
                }
                self.aof.ChannelActive(self)
                continue
            }

            self.aof.ChannelActive(self)
            self.HandleLock(aof_lock)
        }
    }
}

func (self *AofChannel) HandleLock(aof_lock *AofLock)  {
    if aof_lock.LockType == 0 {
        err := aof_lock.Encode()
        if err != nil {
            self.slock.Log().Errorf("Aof Push Encode Error %v", err)
            if aof_lock.AofFlag & 0x1000 != 0 && aof_lock.lock != nil {
                lock_manager := aof_lock.lock.manager
                lock_manager.lock_db.DoAckLock(aof_lock.lock, false)
            }
        } else {
            self.aof.PushLock(aof_lock)
        }

        self.glock.Lock()
        if self.free_lock_index < self.free_lock_max {
            self.free_locks[self.free_lock_index] = aof_lock
            self.free_lock_index++
        }
        self.glock.Unlock()
        return
    }

    expried_time := uint16(0)
    if aof_lock.ExpriedFlag & 0x4000 == 0 {
        expried_time = uint16(int64(aof_lock.CommandTime + uint64(aof_lock.ExpriedTime)) - self.lock_db.current_time)
    }

    lock_command := self.server_protocol.GetLockCommand()
    lock_command.CommandType = aof_lock.CommandType
    lock_command.RequestId = aof_lock.GetRequestId()
    lock_command.Flag = aof_lock.Flag
    lock_command.DbId = aof_lock.DbId
    lock_command.LockId = aof_lock.LockId
    lock_command.LockKey = aof_lock.LockKey
    if aof_lock.AofFlag & 0x1000 != 0 {
        lock_command.TimeoutFlag = 0x1000
    } else {
        lock_command.TimeoutFlag = 0
    }
    lock_command.Timeout = 3
    lock_command.ExpriedFlag = aof_lock.ExpriedFlag | 0x1000
    lock_command.Expried = expried_time + 1
    lock_command.Count = aof_lock.Count
    lock_command.Rcount = aof_lock.Rcount
    err := self.server_protocol.ProcessLockCommand(lock_command)
    if err != nil {
        self.slock.Log().Errorf("Aof Load ProcessLockCommand Error %v", err)
        if aof_lock.AofFlag & 0x1000 != 0 {
            self.aof.LockLoaded(self.server_protocol.(*MemWaiterServerProtocol), lock_command, protocol.RESULT_ERROR, 0, 0)
        }
    }

    self.glock.Lock()
    if self.free_lock_index < self.free_lock_max {
        self.free_locks[self.free_lock_index] = aof_lock
        self.free_lock_index++
    }
    self.glock.Unlock()
}

func (self *AofChannel) DoStop()  {
    self.is_stop = true
    self.server_protocol.Close()
    self.lock_db = nil
    self.aof.ChannelStop(self)
}

type Aof struct {
    slock                       *SLock
    glock                       *sync.Mutex
    data_dir                    string
    aof_file_index              uint32
    aof_file                    *AofFile
    aof_file_glock              *sync.Mutex
    channels                    []*AofChannel
    channel_count               uint32
    actived_channel_count       uint32
    stoped_channel_waiter       chan bool
    unactived_channel_waiter    chan bool
    rewrited_waiter             chan bool
    rewrite_size                uint32
    aof_lock_count              uint64
    aof_id                      uint32
    is_rewriting                bool
    is_stop                     bool
}

func NewAof() *Aof {
    return &Aof{nil, &sync.Mutex{}, "",0, nil, &sync.Mutex{}, make([]*AofChannel, 0),
        0, 0, nil, nil, nil, 0, 0, 0, false, false}
}

func (self *Aof) Init() error {
    self.rewrite_size = uint32(Config.AofFileRewriteSize)
    data_dir, err := filepath.Abs(Config.DataDir)
    if err != nil {
        return err
    }

    self.data_dir = data_dir
    if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
        return err
    }
    self.slock.Log().Infof("Aof Data Dir %s", self.data_dir)

    if self.actived_channel_count > 0 {
        self.unactived_channel_waiter = make(chan bool, 1)
        <- self.unactived_channel_waiter
    }
    return nil
}

func (self *Aof) LoadAndInit() error {
    self.rewrite_size = uint32(Config.AofFileRewriteSize)
    data_dir, err := filepath.Abs(Config.DataDir)
    if err != nil {
        return err
    }

    self.data_dir = data_dir
    if _, err := os.Stat(self.data_dir); os.IsNotExist(err) {
        return err
    }
    self.slock.Log().Infof("Aof Data Dir %s", self.data_dir)

    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return err
    }

    if len(append_files) > 0 {
        aof_file_index, err := strconv.Atoi(append_files[len(append_files) -1][11:])
        if err != nil {
            return err
        }
        self.aof_file_index = uint32(aof_file_index)
    }

    self.aof_file = NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index + 1)), os.O_WRONLY, int(Config.AofFileBufferSize))
    err = self.aof_file.Open()
    if err != nil {
        return err
    }
    self.aof_file_index++
    self.slock.Log().Infof("Aof File Create %s.%d", "append.aof", self.aof_file_index)

    aof_filenames := make([]string, 0)
    if rewrite_file != "" {
        aof_filenames = append(aof_filenames, rewrite_file)
    }
    aof_filenames = append(aof_filenames, append_files...)
    err = self.LoadAofFiles(aof_filenames, func (filename string, aof_file *AofFile, lock *AofLock, first_lock bool) (bool, error) {
        err := self.LoadLock(lock)
        if err != nil {
            return true, err
        }
        return true, nil
    })
    if err != nil {
        return err
    }
    self.slock.Log().Infof("Aof File Load %v", aof_filenames)

    if self.actived_channel_count > 0 {
        self.unactived_channel_waiter = make(chan bool, 1)
        <- self.unactived_channel_waiter
    }
    if len(append_files) > 0 {
        go self.RewriteAofFiles()
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
            _, err := strconv.Atoi(file_name[11:])
            if err == nil {
                append_files = append(append_files, file_name)
            }
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

func (self *Aof) LoadAofFiles(filenames []string, iter_func func(string, *AofFile, *AofLock, bool) (bool, error)) error {
    lock := NewAofLock()
    now := time.Now().Unix()

    for _, filename := range filenames {
        err := self.LoadAofFile(filename, lock, now, iter_func)
        if err != nil {
            if err == io.EOF {
                return nil
            }
            return err
        }
    }
    return nil
}

func (self *Aof) LoadAofFile(filename string, lock *AofLock, now int64, iter_func func(string, *AofFile, *AofLock, bool) (bool, error)) error {
    aof_file := NewAofFile(self, filepath.Join(self.data_dir, filename), os.O_RDONLY, int(Config.AofFileBufferSize))
    err := aof_file.Open()
    if err != nil {
        return err
    }

    first_lock := true
    for {
        err := aof_file.ReadLock(lock)
        if err == io.EOF {
            err := aof_file.Close()
            if err != nil {
                return err
            }
            return nil
        }

        if err != nil {
            return err
        }

        err = lock.Decode()
        if err != nil {
            return err
        }

        if lock.ExpriedFlag & 0x4000 == 0 {
            if int64(lock.CommandTime + uint64(lock.ExpriedTime)) <= now {
                continue
            }
        }

        is_stop, iter_err := iter_func(filename, aof_file, lock, first_lock)
        if iter_err != nil {
            return iter_err
        }

        if !is_stop {
            return io.EOF
        }
        first_lock = false
    }
}

func (self *Aof) Close()  {
    self.glock.Lock()
    if self.is_stop {
        self.glock.Unlock()
        return
    }
    self.is_stop = true
    self.glock.Unlock()

    if self.channel_count > 0 {
        self.stoped_channel_waiter = make(chan bool, 1)
        <- self.stoped_channel_waiter
        self.stoped_channel_waiter = nil
    }

    if self.aof_file != nil {
        self.aof_file_glock.Lock()
        self.aof_file.Close()
        self.aof_file = nil
        self.aof_file_glock.Unlock()
    }
    self.slock.logger.Infof("Aof Closed")
}

func (self *Aof) NewAofChannel(lock_db *LockDB) *AofChannel {
    self.glock.Lock()
    server_protocol := NewMemWaiterServerProtocol(self.slock)
    aof_channel := &AofChannel{self.slock, &sync.Mutex{}, self, lock_db, make(chan *AofLock, Config.AofQueueSize),
        server_protocol, make([]*AofLock, Config.AofQueueSize), 0, int32(Config.AofQueueSize),
        make([]byte, 64), false, false}
    server_protocol.SetResultCallback(self.LockLoaded)
    self.channel_count++
    go aof_channel.Handle()
    self.glock.Unlock()
    return aof_channel
}

func (self *Aof) CloseAofChannel(aof_channel *AofChannel) *AofChannel {
    self.glock.Lock()
    aof_channel.channel <- nil
    aof_channel.closed = true
    self.glock.Unlock()
    return aof_channel
}

func (self *Aof) ChannelRun(channel *AofChannel) {
}

func (self *Aof) ChannelActive(channel *AofChannel) {
    atomic.AddUint32(&self.actived_channel_count, 1)
}

func (self *Aof) ChannelUnActive(channel *AofChannel) {
    atomic.AddUint32(&self.actived_channel_count, 0xffffffff)
    if self.actived_channel_count != 0 {
        return
    }

    if !atomic.CompareAndSwapUint32(&self.actived_channel_count, 0, 0) {
        return
    }

    self.aof_file_glock.Lock()
    self.Flush()
    if self.unactived_channel_waiter != nil {
        self.unactived_channel_waiter <- true
        self.unactived_channel_waiter = nil
    }
    self.aof_file_glock.Unlock()
}

func (self *Aof) ChannelStop(channel *AofChannel) {
    self.glock.Lock()
    self.channel_count--
    if self.channel_count <= 0 {
        if self.is_stop && self.stoped_channel_waiter != nil {
            self.stoped_channel_waiter <- true
            self.stoped_channel_waiter = nil
        }
    }
    self.glock.Unlock()
}

func (self *Aof) LoadLock(lock *AofLock) error {
    db := self.slock.dbs[lock.DbId]
    if db == nil {
        db = self.slock.GetOrNewDB(lock.DbId)
    }

    aof_channel := db.aof_channels[uint8(lock.LockKey[3] ^ lock.LockKey[15]) % uint8(db.manager_max_glocks)]
    return aof_channel.Load(lock)
}

func (self *Aof) PushLock(lock *AofLock) {
    self.aof_file_glock.Lock()
    self.aof_id++
    lock.UpdateAofIndexId(self.aof_file_index, self.aof_id)
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
    err = self.slock.replication_manager.PushLock(lock)
    if err != nil {
        self.slock.Log().Errorf("Aof File Push Error %v", err)
        if lock.AofFlag & 0x1000 != 0 && lock.lock != nil {
            lock_manager := lock.lock.manager
            lock_manager.lock_db.DoAckLock(lock.lock, false)
        }
    }
    self.aof_lock_count++

    if uint32(self.aof_file.GetSize()) >= self.rewrite_size {
        self.RewriteAofFile()
    }
    self.aof_file_glock.Unlock()
}

func (self *Aof) AppendLock(lock *AofLock) {
    self.aof_file_glock.Lock()
    if lock.AofIndex != self.aof_file_index || self.aof_file == nil {
        self.aof_file_index = lock.AofIndex - 1
        self.aof_id = lock.AofId
        self.RewriteAofFile()
    }

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
    self.aof_id = lock.AofId
    self.aof_lock_count++
    self.aof_file_glock.Unlock()
}

func (self *Aof) AofFileFlushRequested(ack_request *AofFileAckRequest, succed bool) error {
    if self.slock.state == STATE_LEADER {
        db := self.slock.replication_manager.GetAckDB(ack_request.db_id)
        if db != nil {
            return db.ProcessAofed(ack_request.request_id, succed)
        }
        return nil
    }


    db := self.slock.replication_manager.GetOrNewAckDB(ack_request.db_id)
    if db != nil {
        return db.ProcessAckAofed(ack_request.request_id, succed)
    }
    return nil
}

func (self *Aof) LockLoaded(server_protocol *MemWaiterServerProtocol, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    if self.slock.state == STATE_FOLLOWER {
        if command.TimeoutFlag & 0x1000 == 0 {
            return nil
        }

        db := self.slock.replication_manager.GetOrNewAckDB(command.DbId)
        if db != nil {
            return db.ProcessAcked(command, result, lcount, lrcount)
        }
        return nil
    }
    return nil
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

func (self *Aof) OpenAofFile(aof_index uint32) (*AofFile, error) {
    if aof_index == 0 {
        aof_file := NewAofFile(self, filepath.Join(self.data_dir, "rewrite.aof"), os.O_WRONLY, int(Config.AofFileBufferSize))
        err := aof_file.Open()
        if err != nil {
            return nil, err
        }
        return aof_file, nil
    }

    aof_file := NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", aof_index)), os.O_WRONLY, int(Config.AofFileBufferSize))
    err := aof_file.Open()
    if err != nil {
        return nil, err
    }
    return aof_file, nil
}

func (self *Aof) Reset(aof_file_index uint32) error {
    self.aof_file_glock.Lock()
    defer self.aof_file_glock.Unlock()
    if self.is_rewriting {
        return errors.New("Aof Rewriting")
    }

    if self.aof_file != nil {
        self.Flush()

        err := self.aof_file.Close()
        if err != nil {
            self.slock.Log().Errorf("Aof File Close Error %s.%d %v", "append.aof", self.aof_file_index, err)
            return err
        }
        self.aof_file = nil
    }

    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return err
    }

    if rewrite_file != "" {
        err := os.Remove(filepath.Join(self.data_dir, rewrite_file))
        if err != nil {
            self.slock.Log().Errorf("Aof Reset Rename Error %v", err)
            return err
        }
    }

    for _, append_file := range append_files {
        err := os.Remove(filepath.Join(self.data_dir, append_file))
        if err != nil {
            self.slock.Log().Errorf("Aof Reset Rename Error %v", err)
            return err
        }
    }

    self.aof_file_index = aof_file_index
    self.aof_id = 0
    self.aof_file = NewAofFile(self, filepath.Join(self.data_dir, fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index + 1)), os.O_WRONLY, int(Config.AofFileBufferSize))
    err = self.aof_file.Open()
    if err != nil {
        return err
    }
    self.aof_file_index++
    self.slock.Log().Infof("Aof File Create %s.%d", "append.aof", self.aof_file_index)
    return nil
}

func (self *Aof) RewriteAofFile() {
    if self.aof_file != nil {
        self.Flush()

        err := self.aof_file.Close()
        if err != nil {
            self.slock.Log().Errorf("Aof File Close Error %s.%d %v", "append.aof", self.aof_file_index, err)
        }
        self.aof_file = nil
    }

    for ; !self.is_stop; {
        aof_filename := "rewrite.aof"
        if self.aof_file_index > 0 {
            aof_filename = fmt.Sprintf("%s.%d", "append.aof", self.aof_file_index + 1)
        }
        aof_file := NewAofFile(self, filepath.Join(self.data_dir, aof_filename), os.O_WRONLY, int(Config.AofFileBufferSize))
        err := aof_file.Open()
        if err != nil {
            time.Sleep(1e10)
            continue
        }
        self.aof_file = aof_file
        self.aof_file_index++
        self.aof_id = 0
        self.slock.Log().Infof("Aof File Create %s.%d", "append.aof", self.aof_file_index)

        go self.RewriteAofFiles()
        break
    }
}

func (self *Aof) RewriteAofFiles() {
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
        if self.rewrited_waiter != nil {
            self.rewrited_waiter <- true
        }
        self.glock.Unlock()
    }()

    aof_filenames, err := self.FindRewriteAofFiles()
    if err != nil || len(aof_filenames) == 0 {
        return
    }

    rewrite_aof_file, aof_files, err := self.LoadRewriteAofFiles(aof_filenames)
    if err != nil {
        return
    }

    self.ClearRewriteAofFiles(aof_filenames)
    total_aof_size := len(aof_filenames) * 12 - len(aof_files) * 12
    for _, aof_file := range aof_files {
        total_aof_size += aof_file.GetSize()
    }
    self.slock.Log().Infof("Aof Rewrite %d to %d", total_aof_size, rewrite_aof_file.GetSize())
}

func (self *Aof) FindRewriteAofFiles() ([]string, error) {
    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return nil, err
    }

    aof_filenames := make([]string, 0)
    if rewrite_file != "" {
        aof_filenames = append(aof_filenames, rewrite_file)
    }
    for _, append_file := range append_files {
        aof_file_index, err := strconv.Atoi(append_file[11:])
        if err != nil {
            continue
        }

        if uint32(aof_file_index) >= self.aof_file_index {
            continue
        }
        aof_filenames = append(aof_filenames, append_file)
    }
    return aof_filenames, nil
}

func (self *Aof) LoadRewriteAofFiles(aof_filenames []string) (*AofFile, []*AofFile, error){
    rewrite_aof_file := NewAofFile(self, filepath.Join(self.data_dir, "rewrite.aof.tmp"), os.O_WRONLY, int(Config.AofFileBufferSize))
    err := rewrite_aof_file.Open()
    if err != nil {
        return nil, nil, err
    }

    lock_command := &protocol.LockCommand{}
    aof_files := make([]*AofFile, 0)
    aof_id := uint32(0)

    lerr := self.LoadAofFiles(aof_filenames, func (filename string, aof_file *AofFile, lock *AofLock, first_lock bool) (bool, error) {
        db := self.slock.GetDB(lock.DbId)
        if db == nil {
            return true, nil
        }

        lock_command.CommandType = lock.CommandType
        lock_command.DbId = lock.DbId
        lock_command.LockId = lock.LockId
        lock_command.LockKey = lock.LockKey
        if !db.HasLock(lock_command) {
            return true, nil
        }

        aof_id++
        lock.UpdateAofIndexId(0, aof_id)
        err = rewrite_aof_file.WriteLock(lock)
        if err != nil {
            return true, err
        }

        if first_lock {
            aof_files = append(aof_files, aof_file)
        }
        return true, nil
    })
    if lerr != nil {
        self.slock.Log().Errorf("Aof Rewrite Load Rewrite File Error %v", err)
    }

    err = rewrite_aof_file.Flush()
    if err != nil {
        self.slock.Log().Errorf("Aof Rewrite Flush File Error %v", err)
    }

    err = rewrite_aof_file.Close()
    if err != nil {
        self.slock.Log().Errorf("Aof Rewrite Close File Error %v", err)
    }
    return rewrite_aof_file, aof_files, lerr
}

func (self *Aof) ClearRewriteAofFiles(aof_filenames []string) {
    for _, aof_filename := range aof_filenames {
        err := os.Remove(filepath.Join(self.data_dir, aof_filename))
        if err != nil {
            self.slock.Log().Errorf("Aof Rewrite Remove File Error %s %v", aof_filename, err)
            continue
        }
        self.slock.Log().Infof("Aof Rewrite Remove File %s", aof_filename)
    }
    err := os.Rename(filepath.Join(self.data_dir, "rewrite.aof.tmp"), filepath.Join(self.data_dir, "rewrite.aof"))
    if err != nil {
        self.slock.Log().Errorf("Aof Rewrite Rename Error %v", err)
    }
}

func (self *Aof) ClearAofFiles() error {
    append_files, rewrite_file, err := self.FindAofFiles()
    if err != nil {
        return err
    }

    err = filepath.Walk(self.data_dir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        if info.IsDir() {
            return nil
        }

        file_name := info.Name()
        if file_name == rewrite_file {
            return nil
        }

        for _, append_file := range append_files {
            if append_file == file_name {
                return nil
            }
        }

        err = os.Remove(filepath.Join(self.data_dir, file_name))
        if err != nil {
            self.slock.Log().Errorf("Aof Clear Remove File Error %s %v", file_name, err)
        }
        return nil
    })
    return err
}

func (self *Aof) GetRequestId() [16]byte {
    now := uint32(time.Now().Unix())
    request_id_index := atomic.AddUint64(&request_id_index, 1)
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
    }
}