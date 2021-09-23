package protocol

import (
    "errors"
    "math/rand"
    "strings"
    "sync/atomic"
    "time"
)

const MAGIC uint8 = 0x56
const VERSION uint8 = 0x01

const (
    COMMAND_INIT    uint8 = 0
    COMMAND_LOCK    uint8 = 1
    COMMAND_UNLOCK  uint8 = 2
    COMMAND_STATE   uint8 = 3
    COMMAND_ADMIN   uint8 = 4
    COMMAND_PING    uint8 = 5
    COMMAND_QUIT    uint8 = 6
    COMMAND_CALL    uint8 = 7
)

const (
    RESULT_SUCCED = iota
    RESULT_UNKNOWN_MAGIC
    RESULT_UNKNOWN_VERSION
    RESULT_UNKNOWN_DB
    RESULT_UNKNOWN_COMMAND
    RESULT_LOCKED_ERROR
    RESULT_UNLOCK_ERROR
    RESULT_UNOWN_ERROR
    RESULT_TIMEOUT
    RESULT_EXPRIED
    RESULT_STATE_ERROR
    RESULT_ERROR
)

const (
    CALL_COMMAND_ENCODING_TEXT uint8 = 1
    CALL_COMMAND_ENCODING_JSON uint8 = 2
    CALL_COMMAND_ENCODING_PROTOCOL uint8 = 3

    CALL_COMMAND_CHARSET_UTF8 uint8 = 1
)

var ERROR_MSG []string = []string{
    "OK",
    "UNKNOWN_MAGIC",
    "UNKNOWN_VERSION",
    "UNKNOWN_DB",
    "UNKNOWN_COMMAND",
    "LOCKED_ERROR",
    "UNLOCK_ERROR",
    "UNOWN_ERROR",
    "TIMEOUT",
    "EXPRIED",
    "RESULT_STATE_ERROR",
    "UNKNOWN_ERROR",
}

var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var request_id_index uint64 = 0

func GenRequestId() [16]byte {
    now := uint32(time.Now().Unix())
    request_id_index := atomic.AddUint64(&request_id_index, 1)
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
    }
}

type ICommand interface {
    GetCommandType() uint8
    GetRequestId() [16]byte
    Encode(buf []byte) error
    Decode(buf []byte) error
}

type CommandDecode interface {
    Decode(buf []byte) error
}

type CommandEncode interface {
    Encode(buf []byte) error
}

type Command struct {
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [16]byte
}

func NewCommand(command_type uint8) *Command {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:command_type, RequestId:GenRequestId()}
    return &command
}

func (self *Command) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7], 
    self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] = 
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    return nil
}

func (self *Command) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], 
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = 
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]
        
    copy(buf[19:], make([]byte, 45))
    return nil
}

func (self *Command) GetCommandType() uint8{
    return self.CommandType
}

func (self *Command) GetRequestId() [16]byte{
    return self.RequestId
}

type ResultCommand struct {
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [16]byte
    Result    uint8
}

func NewResultCommand(command ICommand, result uint8) *ResultCommand {
    return &ResultCommand{MAGIC, VERSION, command.GetCommandType(), command.GetRequestId(), result}
}

func (self *ResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])

    return nil
}

func (self *ResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]
        
    buf[19] = uint8(self.Result)

    copy(buf[20:], make([]byte, 44))
    return nil
}

func (self *ResultCommand) GetCommandType() uint8{
    return self.CommandType
}

func (self *ResultCommand) GetRequestId() [16]byte{
    return self.RequestId
}

type InitCommand struct {
    Command
    ClientId    [16]byte
    Blank       [29]byte
}

func NewInitCommand(client_id [16]byte) *InitCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_INIT, RequestId:GenRequestId()}
    init_command := InitCommand{Command:command, ClientId:client_id, Blank:[29]byte{}}
    return &init_command
}

func (self *InitCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.ClientId[0], self.ClientId[1], self.ClientId[2], self.ClientId[3], self.ClientId[4], self.ClientId[5], self.ClientId[6], self.ClientId[7],
        self.ClientId[8], self.ClientId[9], self.ClientId[10], self.ClientId[11], self.ClientId[12], self.ClientId[13], self.ClientId[14], self.ClientId[15] =
        buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26],
        buf[27], buf[28], buf[29], buf[30], buf[31], buf[32], buf[33], buf[34]

    return nil
}

func (self *InitCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26],
        buf[27], buf[28], buf[29], buf[30], buf[31], buf[32], buf[33], buf[34] =
        self.ClientId[0], self.ClientId[1], self.ClientId[2], self.ClientId[3], self.ClientId[4], self.ClientId[5], self.ClientId[6], self.ClientId[7],
        self.ClientId[8], self.ClientId[9], self.ClientId[10], self.ClientId[11], self.ClientId[12], self.ClientId[13], self.ClientId[14], self.ClientId[15]
        
    for i :=0; i<29; i++ {
        buf[35 + i] = 0x00
    }

    return nil
}

var INIT_COMMAND_BLANK_BYTERS = [43]byte{}

type InitResultCommand struct {
    ResultCommand
    InitType  uint8
    Blank     [43]byte
}

func NewInitResultCommand(command *InitCommand, result uint8, init_type uint8) *InitResultCommand {
    result_command := ResultCommand{ MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &InitResultCommand{result_command,init_type, INIT_COMMAND_BLANK_BYTERS}
}

func (self *InitResultCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result, self.InitType = uint8(buf[19]), uint8(buf[20])

    return nil
}

func (self *InitResultCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]
        
    buf[19], buf[20] = uint8(self.Result), byte(self.InitType)

    for i :=0; i<43; i++ {
        buf[21 + i] = 0x00
    }

    return nil
}

type LockCommand struct {
    Command
    Flag            uint8
    DbId            uint8
    LockId          [16]byte
    LockKey         [16]byte
    TimeoutFlag     uint16
    /*
    |15      |                13                   |  12 |        11      |       10       |      9       | 8 |7                                  0|
    |--------|-------------------------------------|-----|----------------|----------------|--------------|---|------------------------------------|
    |        |update_no_reset_timeout_checked_count|acked|timeout_is_error|millisecond_time|unlock_to_wait|   |                                    |
    */
    Timeout         uint16
    ExpriedFlag     uint16
    /*
    |    15  |          14          |                13                   |                12         |        11      |       10       |      9      |        8         |7                                  0|
    |--------|----------------------|-------------------------------------|---------------------------|----------------|----------------|-------------|------------------|------------------------------------|
    |keeplive|unlimited_expried_time|update_no_reset_expried_checked_count|aof_time_of_expried_parcent|expried_is_error|millisecond_time|zeor_aof_time|unlimited_aof_time|                                    |
    */
    Expried         uint16
    Count           uint16
    Rcount          uint8
}

func NewLockCommand(db_id uint8, lock_key [16]byte, lock_id [16]byte, timeout uint16, expried uint16, count uint16) *LockCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_LOCK, RequestId:GenRequestId()}
    lock_command := LockCommand{Command:command, Flag:0, DbId:db_id, LockId:lock_id, LockKey:lock_key, TimeoutFlag:0,
        Timeout:timeout, ExpriedFlag:0, Expried:expried, Count:count, Rcount:0}
    return &lock_command
}

func (self *LockCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Flag, self.DbId = uint8(buf[19]), uint8(buf[20])

    self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15] =
        buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

    self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15] =
        buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
        buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]
        
    self.Timeout, self.TimeoutFlag, self.Expried, self.ExpriedFlag = uint16(buf[53]) | uint16(buf[54])<<8, uint16(buf[55]) | uint16(buf[56])<<8, uint16(buf[57]) | uint16(buf[58])<<8, uint16(buf[59]) | uint16(buf[60])<<8
    self.Count, self.Rcount = uint16(buf[61]) | uint16(buf[62])<<8, uint8(buf[63])
    return nil
}

func (self *LockCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]
        
    buf[19], buf[20] = byte(self.Flag), byte(self.DbId)

    buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
        buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36] =
        self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15]

    buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
        buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52] =
        self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15]

    buf[53], buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60] = byte(self.Timeout), byte(self.Timeout >> 8), byte(self.TimeoutFlag), byte(self.TimeoutFlag >> 8), byte(self.Expried), byte(self.Expried >> 8), byte(self.ExpriedFlag), byte(self.ExpriedFlag >> 8)

    buf[61], buf[62], buf[63] = byte(self.Count), byte(self.Count >> 8), byte(self.Rcount)

    return nil
}

var RESULT_LOCK_COMMAND_BLANK_BYTERS = [4]byte{}

type LockResultCommand struct {
    ResultCommand
    Flag      uint8
    DbId      uint8
    LockId    [16]byte
    LockKey   [16]byte
    Lcount    uint16
    Count     uint16
    Lrcount   uint8
    Rcount    uint8
    Blank     [4]byte
}

func NewLockResultCommand(command *LockCommand, result uint8, flag uint8, lcount uint16, count uint16, lrcount uint8, rcount uint8) *LockResultCommand {
    result_command := ResultCommand{ MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &LockResultCommand{result_command, flag, command.DbId, command.LockId, command.LockKey,
        lcount, count, lrcount, rcount, RESULT_LOCK_COMMAND_BLANK_BYTERS}
}

func (self *LockResultCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result, self.Flag, self.DbId = uint8(buf[19]), uint8(buf[20]), uint8(buf[21])

    self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15] =
        buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
        buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37]

    self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15] =
        buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
        buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53]

    self.Lcount, self.Count, self.Lrcount, self.Rcount = uint16(buf[54]) | uint16(buf[55])<<8, uint16(buf[56]) | uint16(buf[57])<<8, uint8(buf[58]), uint8(buf[59])

    return nil
}

func (self *LockResultCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19], buf[20], buf[21] = uint8(self.Result), byte(self.Flag), byte(self.DbId)

    buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
        buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] =
        self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
        self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15]

    buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
        buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] =
        self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
        self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15]

    buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(self.Lcount), byte(self.Lcount >> 8), byte(self.Count), byte(self.Count >> 8), byte(self.Lrcount), byte(self.Rcount), 0x00, 0x00
    buf[62], buf[63] = 0x00, 0x00
    return nil
}

type StateCommand struct {
    Command
    Flag      uint8
    DbId uint8
    Blank [43]byte
}

func NewStateCommand(db_id uint8) *StateCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_STATE, RequestId:GenRequestId()}
    state_command := StateCommand{Command:command, Flag:0, DbId:db_id, Blank:[43]byte{}}
    return &state_command
}

func (self *StateCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Flag = uint8(buf[19])
    self.DbId = uint8(buf[20])

    return nil
}

func (self *StateCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = byte(self.Flag)
    buf[20] = byte(self.DbId)

    for i :=0; i<43; i++ {
        buf[21 + i] = 0x00
    }

    return nil
}

type StateResultCommand struct {
    ResultCommand
    Flag      uint8
    DbState uint8
    DbId uint8
    State LockDBState
    Blank [1]byte
}

func NewStateResultCommand(command *StateCommand, result uint8, flag uint8, db_state uint8, state *LockDBState) *StateResultCommand {
    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    if state == nil {
        state = &LockDBState{}
    }
    return &StateResultCommand{result_command, flag, db_state, command.DbId, *state, [1]byte{}}
}

func (self *StateResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])
    self.Flag = uint8(buf[20])
    self.DbState = uint8(buf[21])
    self.DbId = uint8(buf[22])

    self.State.LockCount = uint64(buf[23]) | uint64(buf[24])<<8 | uint64(buf[25])<<16 | uint64(buf[26])<<24 | uint64(buf[27])<<32 | uint64(buf[28])<<40 | uint64(buf[29])<<48 | uint64(buf[30])<<56
    self.State.UnLockCount = uint64(buf[31]) | uint64(buf[32])<<8 | uint64(buf[33])<<16 | uint64(buf[34])<<24 | uint64(buf[35])<<32 | uint64(buf[36])<<40 | uint64(buf[37])<<48 | uint64(buf[38])<<56
    self.State.LockedCount = uint32(buf[39]) | uint32(buf[40])<<8 | uint32(buf[41])<<16 | uint32(buf[42])<<24
    self.State.WaitCount = uint32(buf[43]) | uint32(buf[44])<<8 | uint32(buf[45])<<16 | uint32(buf[46])<<24
    self.State.TimeoutedCount = uint32(buf[47]) | uint32(buf[48])<<8 | uint32(buf[49])<<16 | uint32(buf[50])<<24
    self.State.ExpriedCount = uint32(buf[51]) | uint32(buf[52])<<8 | uint32(buf[53])<<16 | uint32(buf[54])<<24
    self.State.UnlockErrorCount = uint32(buf[55]) | uint32(buf[56])<<8 | uint32(buf[57])<<16 | uint32(buf[58])<<24
    self.State.KeyCount = uint32(buf[59]) | uint32(buf[60])<<8 | uint32(buf[61])<<16 | uint32(buf[62])<<24

    return nil
}

func (self *StateResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = uint8(self.Result)
    buf[20] = byte(self.Flag)
    buf[21] = byte(self.DbState)
    buf[22] = byte(self.DbId)

    buf[23] = byte(self.State.LockCount)
    buf[24] = byte(self.State.LockCount >> 8)
    buf[25] = byte(self.State.LockCount >> 16)
    buf[26] = byte(self.State.LockCount >> 24)
    buf[27] = byte(self.State.LockCount >> 32)
    buf[28] = byte(self.State.LockCount >> 40)
    buf[29] = byte(self.State.LockCount >> 48)
    buf[30] = byte(self.State.LockCount >> 56)

    buf[31] = byte(self.State.UnLockCount)
    buf[32] = byte(self.State.UnLockCount >> 8)
    buf[33] = byte(self.State.UnLockCount >> 16)
    buf[34] = byte(self.State.UnLockCount >> 24)
    buf[35] = byte(self.State.UnLockCount >> 32)
    buf[36] = byte(self.State.UnLockCount >> 40)
    buf[37] = byte(self.State.UnLockCount >> 48)
    buf[38] = byte(self.State.UnLockCount >> 56)

    buf[39] = byte(self.State.LockedCount)
    buf[40] = byte(self.State.LockedCount >> 8)
    buf[41] = byte(self.State.LockedCount >> 16)
    buf[42] = byte(self.State.LockedCount >> 24)

    buf[43] = byte(self.State.WaitCount)
    buf[44] = byte(self.State.WaitCount >> 8)
    buf[45] = byte(self.State.WaitCount >> 16)
    buf[46] = byte(self.State.WaitCount >> 24)

    buf[47] = byte(self.State.TimeoutedCount)
    buf[48] = byte(self.State.TimeoutedCount >> 8)
    buf[49] = byte(self.State.TimeoutedCount >> 16)
    buf[50] = byte(self.State.TimeoutedCount >> 24)

    buf[51] = byte(self.State.ExpriedCount)
    buf[52] = byte(self.State.ExpriedCount >> 8)
    buf[53] = byte(self.State.ExpriedCount >> 16)
    buf[54] = byte(self.State.ExpriedCount >> 24)

    buf[55] = byte(self.State.UnlockErrorCount)
    buf[56] = byte(self.State.UnlockErrorCount >> 8)
    buf[57] = byte(self.State.UnlockErrorCount >> 16)
    buf[58] = byte(self.State.UnlockErrorCount >> 24)

    buf[59] = byte(self.State.KeyCount)
    buf[60] = byte(self.State.KeyCount >> 8)
    buf[61] = byte(self.State.KeyCount >> 16)
    buf[62] = byte(self.State.KeyCount >> 24)

    buf[63] = 0x00

    return nil
}


type AdminCommand struct {
    Command
    AdminType   uint8
    Blank       [44]byte
}

func NewAdminCommand(admin_type uint8) *AdminCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_ADMIN, RequestId:GenRequestId()}
    admin_command := AdminCommand{Command:command, AdminType:admin_type, Blank:[44]byte{}}
    return &admin_command
}

func (self *AdminCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.AdminType = uint8(buf[19])

    return nil
}

func (self *AdminCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = byte(self.AdminType)

    for i :=0; i<44; i++ {
        buf[20 + i] = 0x00
    }

    return nil
}

type AdminResultCommand struct {
    ResultCommand
    Blank [44]byte
}

func NewAdminResultCommand(command *AdminCommand, result uint8) *AdminResultCommand {
    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &AdminResultCommand{result_command, [44]byte{}}
}

func (self *AdminResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])

    return nil
}

func (self *AdminResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = uint8(self.Result)

    for i :=0; i<44; i++ {
        buf[20 + i] = 0x00
    }

    return nil
}

type PingCommand struct {
    Command
    Blank       [45]byte
}

func NewPingCommand() *PingCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_PING, RequestId:GenRequestId()}
    ping_command := PingCommand{Command:command, Blank:[45]byte{}}
    return &ping_command
}

func (self *PingCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    return nil
}

func (self *PingCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    for i :=0; i<45; i++ {
        buf[19 + i] = 0x00
    }

    return nil
}

type PingResultCommand struct {
    ResultCommand
    Blank [44]byte
}

func NewPingResultCommand(command *PingCommand, result uint8) *PingResultCommand {
    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &PingResultCommand{result_command, [44]byte{}}
}

func (self *PingResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])

    return nil
}

func (self *PingResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = uint8(self.Result)

    for i :=0; i<44; i++ {
        buf[20 + i] = 0x00
    }

    return nil
}

type QuitCommand struct {
    Command
    Blank       [45]byte
}

func NewQuitCommand() *QuitCommand {
    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_QUIT, RequestId:GenRequestId()}
    quit_command := QuitCommand{Command:command, Blank:[45]byte{}}
    return &quit_command
}

func (self *QuitCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    return nil
}

func (self *QuitCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    for i :=0; i<45; i++ {
        buf[19 + i] = 0x00
    }

    return nil
}

type QuitResultCommand struct {
    ResultCommand
    Blank [44]byte
}

func NewQuitResultCommand(command *QuitCommand, result uint8) *QuitResultCommand {
    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &QuitResultCommand{result_command, [44]byte{}}
}

func (self *QuitResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])

    return nil
}

func (self *QuitResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = uint8(self.Result)

    for i :=0; i<44; i++ {
        buf[20 + i] = 0x00
    }

    return nil
}

type CallCommand struct {
    Command
    Flag        uint8
    Encoding    uint8
    Charset     uint8
    ContentLen  uint32
    MethodName  string
    Data        []byte
}

func NewCallCommand(method_name string, data []byte) *CallCommand {
    content_len := uint32(0)
    if data != nil {
        content_len = uint32(len(data))
    }

    command := Command{Magic:MAGIC, Version:VERSION, CommandType:COMMAND_CALL, RequestId:GenRequestId()}
    call_command := CallCommand{Command:command, Flag:0, Encoding:CALL_COMMAND_ENCODING_PROTOCOL, Charset:CALL_COMMAND_CHARSET_UTF8, ContentLen:content_len, MethodName:method_name, Data:data}
    return &call_command
}

func (self *CallCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Flag = uint8(buf[19])
    self.Encoding = uint8(buf[20])
    self.Charset = uint8(buf[21])
    self.ContentLen = uint32(buf[22]) | uint32(buf[23])<<8 | uint32(buf[24])<<16 | uint32(buf[25])<<24
    self.MethodName = strings.Trim(string(buf[26:]), string([]byte{0}))
    return nil
}

func (self *CallCommand) Encode(buf []byte) error {
    if len(self.MethodName) > 38 {
        return errors.New("MethodName too long")
    }

    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = byte(self.Flag)
    buf[20] = byte(self.Encoding)
    buf[21] = byte(self.Charset)

    buf[22] = byte(self.ContentLen)
    buf[23] = byte(self.ContentLen >> 8)
    buf[24] = byte(self.ContentLen >> 16)
    buf[25] = byte(self.ContentLen >> 24)

    for i :=0; i < 38; i++ {
        if i >= len(self.MethodName) {
            buf[26 + i] = 0x00
        } else {
            buf[26 + i] = self.MethodName[i]
        }
    }
    return nil
}

type CallResultCommand struct {
    ResultCommand
    Flag        uint8
    Encoding    uint8
    Charset     uint8
    ContentLen  uint32
    ErrType     string
    Data        []byte
}

func NewCallResultCommand(command *CallCommand, result uint8, err_type string, data []byte) *CallResultCommand {
    content_len := uint32(0)
    if data != nil {
        content_len = uint32(len(data))
    }

    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &CallResultCommand{result_command, 0, CALL_COMMAND_ENCODING_PROTOCOL, CALL_COMMAND_CHARSET_UTF8, content_len, err_type, data}
}

func (self *CallResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

    self.Result = uint8(buf[19])
    self.Flag = uint8(buf[20])
    self.Encoding = uint8(buf[21])
    self.Charset = uint8(buf[22])
    self.ContentLen = uint32(buf[23]) | uint32(buf[24])<<8 | uint32(buf[25])<<16 | uint32(buf[26])<<24
    self.ErrType = strings.Trim(string(buf[27:]), string([]byte{0}))

    return nil
}

func (self *CallResultCommand) Encode(buf []byte) error {
    if len(self.ErrType) > 37 {
        return errors.New("ErrType too long")
    }

    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
        self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

    buf[19] = uint8(self.Result)

    buf[20] = byte(self.Flag)
    buf[21] = byte(self.Encoding)
    buf[22] = byte(self.Charset)

    buf[23] = byte(self.ContentLen)
    buf[24] = byte(self.ContentLen >> 8)
    buf[25] = byte(self.ContentLen >> 16)
    buf[26] = byte(self.ContentLen >> 24)

    for i :=0; i < 37; i++ {
        if i >= len(self.ErrType) {
            buf[27 + i] = 0x00
        } else {
            buf[27 + i] = self.ErrType[i]
        }
    }
    return nil
}