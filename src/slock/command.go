package slock

const MAGIC = 0x56
const VERSION = 0x01

const (
    COMMAND_LOCK = 1
    COMMAND_UNLOCK = 2
    COMMAND_STATE = 3
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
    RESULT_ERROR
)

type ICommand interface {
    GetCommandType() uint8
    GetRequestId() [16]byte
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

func NewCommand(buf []byte) *Command {
    command := Command{}
    if command.Decode(buf) != nil {
        return nil
    }

    return &command
}

func (self *Command) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }
    return nil
}

func (self *Command) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

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

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }

    self.Result = uint8(buf[19])

    return nil
}

func (self *ResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

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

type LockCommand struct {
    Command
    Flag      uint8
    DbId      uint8
    LockId    [16]byte
    LockKey   [16]byte
    Timeout   uint32
    Expried   uint32
    Count     uint16
    Blank     [1]byte
}

func NewLockCommand(buf []byte) *LockCommand {
    command := LockCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *LockCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }

    self.Flag = uint8(buf[19])
    self.DbId = uint8(buf[20])

    for i := 0; i < 16; i++{
        self.LockId[i] = buf[21 + i]
    }

    for i := 0; i < 16; i++{
        self.LockKey[i] = buf[37 + i]
    }

    self.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
    self.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
    self.Count = uint16(buf[61]) | uint16(buf[62])<<8
    return nil
}

func (self *LockCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

    buf[19] = byte(self.Flag)
    buf[20] = byte(self.DbId)

    for i := 0; i < 16; i++ {
        buf[21 + i] = self.LockId[i]
    }

    for i := 0; i < 16; i++ {
        buf[37 + i] = self.LockKey[i]
    }

    buf[53] = byte(self.Timeout)
    buf[54] = byte(self.Timeout >> 8)
    buf[55] = byte(self.Timeout >> 16)
    buf[56] = byte(self.Timeout >> 24)

    buf[57] = byte(self.Expried)
    buf[58] = byte(self.Expried >> 8)
    buf[59] = byte(self.Expried >> 16)
    buf[60] = byte(self.Expried >> 24)

    buf[61] = byte(self.Count)
    buf[62] = byte(self.Count >> 8)

    buf[63] = self.Blank[0]

    return nil
}

var RESULT_LOCK_COMMAND_BLANK_BYTERS = [10]byte{}

type LockResultCommand struct {
    ResultCommand
    Flag      uint8
    DbId      uint8
    LockId    [16]byte
    LockKey   [16]byte
    Blank [10]byte
}

func NewLockResultCommand(command *LockCommand, result uint8, flag uint8) *LockResultCommand {
    result_command := ResultCommand{ MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &LockResultCommand{result_command, flag, command.DbId, command.LockId, command.LockKey, RESULT_LOCK_COMMAND_BLANK_BYTERS}
}

func (self *LockResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }

    self.Result = uint8(buf[19])
    self.Flag = uint8(buf[20])
    self.DbId = uint8(buf[21])

    for i := 0; i < 16; i++{
        self.LockId[i] = buf[22 + i]
    }

    for i := 0; i < 16; i++{
        self.LockKey[i] = buf[38 + i]
    }

    return nil
}

func (self *LockResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

    buf[19] = uint8(self.Result)
    buf[20] = byte(self.Flag)
    buf[21] = byte(self.DbId)

    for i := 0; i < 16; i++ {
        buf[22 + i] = self.LockId[i]
    }

    for i := 0; i < 16; i++ {
        buf[38 + i] = self.LockKey[i]
    }

    for i :=0; i<10; i++ {
        buf[54 + i] = self.Blank[i]
    }

    return nil
}

type StateCommand struct {
    Command
    Flag      uint8
    DbId uint8
    Blank [43]byte
}

func NewStateCommand(buf []byte) *StateCommand {
    command := StateCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *StateCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }

    self.Flag = uint8(buf[19])
    self.DbId = uint8(buf[20])

    return nil
}

func (self *StateCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

    buf[19] = byte(self.Flag)
    buf[20] = byte(self.DbId)

    for i :=0; i<43; i++ {
        buf[21 + i] = self.Blank[i]
    }

    return nil
}

type ResultStateCommand struct {
    ResultCommand
    Flag      uint8
    DbState uint8
    DbId uint8
    State LockDBState
    Blank [1]byte
}

func NewStateResultCommand(command *StateCommand, result uint8, flag uint8, db_state uint8, state *LockDBState) *ResultStateCommand {
    result_command := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
    if state == nil {
        state = &LockDBState{}
    }
    return &ResultStateCommand{result_command, flag, db_state, command.DbId, *state, [1]byte{}}
}

func (self *ResultStateCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    for i := 0; i < 16; i++{
        self.RequestId[i] = buf[3 + i]
    }

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

func (self *ResultStateCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    for i := 0; i < 16; i++ {
        buf[3 + i] = self.RequestId[i]
    }

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

    buf[63] = self.Blank[0]

    return nil
}