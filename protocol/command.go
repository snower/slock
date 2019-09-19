package protocol

import "errors"

const MAGIC uint8 = 0x56
const VERSION uint8 = 0x01

const (
    COMMAND_INIT uint8 = 0
    COMMAND_LOCK uint8 = 1
    COMMAND_UNLOCK uint8 = 2
    COMMAND_STATE uint8 = 3
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
    GetRequestId() [2]uint64
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
    RequestId [2]uint64
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

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    return nil
}

func (self *Command) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    copy(buf[19:], make([]byte, 45))
    return nil
}

func (self *Command) GetCommandType() uint8{
    return self.CommandType
}

func (self *Command) GetRequestId() [2]uint64{
    return self.RequestId
}

type ResultCommand struct {
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [2]uint64
    Result    uint8
}

func NewResultCommand(command ICommand, result uint8) *ResultCommand {
    return &ResultCommand{MAGIC, VERSION, command.GetCommandType(), command.GetRequestId(), result}
}

func (self *ResultCommand) Decode(buf []byte) error{
    self.Magic = uint8(buf[0])
    self.Version = uint8(buf[1])
    self.CommandType = uint8(buf[2])

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Result = uint8(buf[19])

    return nil
}

func (self *ResultCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    buf[19] = uint8(self.Result)

    copy(buf[20:], make([]byte, 44))
    return nil
}

func (self *ResultCommand) GetCommandType() uint8{
    return self.CommandType
}

func (self *ResultCommand) GetRequestId() [2]uint64{
    return self.RequestId
}

type InitCommand struct {
    Command
    ClientId    [2]uint64
    Blank       [29]byte
}

func NewInitCommand(buf []byte) *InitCommand {
    command := InitCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *InitCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.ClientId[0] = uint64(buf[19]) | uint64(buf[20])<<8 | uint64(buf[21])<<16 | uint64(buf[22])<<24 | uint64(buf[23])<<32 | uint64(buf[24])<<40 | uint64(buf[25])<<48 | uint64(buf[26])<<56
    self.ClientId[1] = uint64(buf[27]) | uint64(buf[28])<<8 | uint64(buf[29])<<16 | uint64(buf[30])<<24 | uint64(buf[31])<<32 | uint64(buf[32])<<40 | uint64(buf[33])<<48 | uint64(buf[34])<<56
    return nil
}

func (self *InitCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26] = byte(self.ClientId[0]), byte(self.ClientId[0] >> 8), byte(self.ClientId[0] >> 16), byte(self.ClientId[0] >> 24), byte(self.ClientId[0] >> 32), byte(self.ClientId[0] >> 40), byte(self.ClientId[0] >> 48), byte(self.ClientId[0] >> 56)
    buf[27], buf[28], buf[29], buf[30], buf[31], buf[32], buf[33], buf[34] = byte(self.ClientId[1]), byte(self.ClientId[1] >> 8), byte(self.ClientId[1] >> 16), byte(self.ClientId[1] >> 24), byte(self.ClientId[1] >> 32), byte(self.ClientId[1] >> 40), byte(self.ClientId[1] >> 48), byte(self.ClientId[1] >> 56)

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

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Result, self.InitType = uint8(buf[19]), uint8(buf[20])

    return nil
}

func (self *InitResultCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

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
    LockId          [2]uint64
    LockKey         [2]uint64
    TimeoutFlag     uint16
    Timeout         uint16
    ExpriedFlag     uint16
    Expried         uint16
    Count           uint16
    Rcount          uint8
}

func NewLockCommand(buf []byte) *LockCommand {
    command := LockCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *LockCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Flag, self.DbId = uint8(buf[19]), uint8(buf[20])

    self.LockId[0] = uint64(buf[21]) | uint64(buf[22])<<8 | uint64(buf[23])<<16 | uint64(buf[24])<<24 | uint64(buf[25])<<32 | uint64(buf[26])<<40 | uint64(buf[27])<<48 | uint64(buf[28])<<56
    self.LockId[1] = uint64(buf[29]) | uint64(buf[30])<<8 | uint64(buf[31])<<16 | uint64(buf[32])<<24 | uint64(buf[33])<<32 | uint64(buf[34])<<40 | uint64(buf[35])<<48 | uint64(buf[36])<<56

    self.LockKey[0] = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
    self.LockKey[1] = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

    self.Timeout, self.TimeoutFlag, self.Expried, self.ExpriedFlag = uint16(buf[53]) | uint16(buf[54])<<8, uint16(buf[55]) | uint16(buf[56])<<8, uint16(buf[57]) | uint16(buf[58])<<8, uint16(buf[59]) | uint16(buf[60])<<8
    self.Count, self.Rcount = uint16(buf[61]) | uint16(buf[62])<<8, uint8(buf[63])
    return nil
}

func (self *LockCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    buf[19], buf[20] = byte(self.Flag), byte(self.DbId)

    buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28] = byte(self.LockId[0]), byte(self.LockId[0] >> 8), byte(self.LockId[0] >> 16), byte(self.LockId[0] >> 24), byte(self.LockId[0] >> 32), byte(self.LockId[0] >> 40), byte(self.LockId[0] >> 48), byte(self.LockId[0] >> 56)
    buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36] = byte(self.LockId[1]), byte(self.LockId[1] >> 8), byte(self.LockId[1] >> 16), byte(self.LockId[1] >> 24), byte(self.LockId[1] >> 32), byte(self.LockId[1] >> 40), byte(self.LockId[1] >> 48), byte(self.LockId[1] >> 56)

    buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44] = byte(self.LockKey[0]), byte(self.LockKey[0] >> 8), byte(self.LockKey[0] >> 16), byte(self.LockKey[0] >> 24), byte(self.LockKey[0] >> 32), byte(self.LockKey[0] >> 40), byte(self.LockKey[0] >> 48), byte(self.LockKey[0] >> 56)
    buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52] = byte(self.LockKey[1]), byte(self.LockKey[1] >> 8), byte(self.LockKey[1] >> 16), byte(self.LockKey[1] >> 24), byte(self.LockKey[1] >> 32), byte(self.LockKey[1] >> 40), byte(self.LockKey[1] >> 48), byte(self.LockKey[1] >> 56)


    buf[53], buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60] = byte(self.Timeout), byte(self.Timeout >> 8), byte(self.TimeoutFlag), byte(self.TimeoutFlag >> 8), byte(self.Expried), byte(self.Expried >> 8), byte(self.ExpriedFlag), byte(self.ExpriedFlag >> 8)

    buf[61], buf[62], buf[63] = byte(self.Count), byte(self.Count >> 8), byte(self.Rcount)

    return nil
}

var RESULT_LOCK_COMMAND_BLANK_BYTERS = [5]byte{}

type LockResultCommand struct {
    ResultCommand
    Flag      uint8
    DbId      uint8
    LockId    [2]uint64
    LockKey   [2]uint64
    Lcount    uint16
    Count     uint16
    Rcount    uint8
    Blank     [5]byte
}

func NewLockResultCommand(command *LockCommand, result uint8, flag uint8, lcount uint16, count uint16, rcount uint8) *LockResultCommand {
    result_command := ResultCommand{ MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &LockResultCommand{result_command, flag, command.DbId, command.LockId, command.LockKey,
        lcount, count, rcount, RESULT_LOCK_COMMAND_BLANK_BYTERS}
}

func (self *LockResultCommand) Decode(buf []byte) error{
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Result, self.Flag, self.DbId = uint8(buf[19]), uint8(buf[20]), uint8(buf[21])

    self.LockId[0] = uint64(buf[22]) | uint64(buf[23])<<8 | uint64(buf[24])<<16 | uint64(buf[25])<<24 | uint64(buf[26])<<32 | uint64(buf[27])<<40 | uint64(buf[28])<<48 | uint64(buf[29])<<56
    self.LockId[1] = uint64(buf[30]) | uint64(buf[31])<<8 | uint64(buf[32])<<16 | uint64(buf[33])<<24 | uint64(buf[34])<<32 | uint64(buf[35])<<40 | uint64(buf[36])<<48 | uint64(buf[37])<<56

    self.LockKey[0] = uint64(buf[38]) | uint64(buf[39])<<8 | uint64(buf[40])<<16 | uint64(buf[41])<<24 | uint64(buf[42])<<32 | uint64(buf[43])<<40 | uint64(buf[44])<<48 | uint64(buf[45])<<56
    self.LockKey[1] = uint64(buf[46]) | uint64(buf[47])<<8 | uint64(buf[48])<<16 | uint64(buf[49])<<24 | uint64(buf[50])<<32 | uint64(buf[51])<<40 | uint64(buf[52])<<48 | uint64(buf[53])<<56

    self.Lcount, self.Count = uint16(buf[54]) | uint16(buf[55])<<8, uint16(buf[56]) | uint16(buf[57])<<8
    self.Rcount = uint8(buf[58])

    return nil
}

func (self *LockResultCommand) Encode(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    buf[19], buf[20], buf[21] = uint8(self.Result), byte(self.Flag), byte(self.DbId)

    buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29] = byte(self.LockId[0]), byte(self.LockId[0] >> 8), byte(self.LockId[0] >> 16), byte(self.LockId[0] >> 24), byte(self.LockId[0] >> 32), byte(self.LockId[0] >> 40), byte(self.LockId[0] >> 48), byte(self.LockId[0] >> 56)
    buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] = byte(self.LockId[1]), byte(self.LockId[1] >> 8), byte(self.LockId[1] >> 16), byte(self.LockId[1] >> 24), byte(self.LockId[1] >> 32), byte(self.LockId[1] >> 40), byte(self.LockId[1] >> 48), byte(self.LockId[1] >> 56)

    buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45] = byte(self.LockKey[0]), byte(self.LockKey[0] >> 8), byte(self.LockKey[0] >> 16), byte(self.LockKey[0] >> 24), byte(self.LockKey[0] >> 32), byte(self.LockKey[0] >> 40), byte(self.LockKey[0] >> 48), byte(self.LockKey[0] >> 56)
    buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] = byte(self.LockKey[1]), byte(self.LockKey[1] >> 8), byte(self.LockKey[1] >> 16), byte(self.LockKey[1] >> 24), byte(self.LockKey[1] >> 32), byte(self.LockKey[1] >> 40), byte(self.LockKey[1] >> 48), byte(self.LockKey[1] >> 56)

    buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(self.Lcount), byte(self.Lcount >> 8), byte(self.Count), byte(self.Count >> 8), byte(self.Rcount), 0x00, 0x00, 0x00
    buf[62], buf[63] = 0x00, 0x00
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

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

    self.Flag = uint8(buf[19])
    self.DbId = uint8(buf[20])

    return nil
}

func (self *StateCommand) Encode(buf []byte) error {
    buf[0] = byte(self.Magic)
    buf[1] = byte(self.Version)
    buf[2] = byte(self.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

    buf[19] = byte(self.Flag)
    buf[20] = byte(self.DbId)

    for i :=0; i<43; i++ {
        buf[21 + i] = 0x00
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

    self.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
    self.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

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

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(self.RequestId[0]), byte(self.RequestId[0] >> 8), byte(self.RequestId[0] >> 16), byte(self.RequestId[0] >> 24), byte(self.RequestId[0] >> 32), byte(self.RequestId[0] >> 40), byte(self.RequestId[0] >> 48), byte(self.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(self.RequestId[1]), byte(self.RequestId[1] >> 8), byte(self.RequestId[1] >> 16), byte(self.RequestId[1] >> 24), byte(self.RequestId[1] >> 32), byte(self.RequestId[1] >> 40), byte(self.RequestId[1] >> 48), byte(self.RequestId[1] >> 56)

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
