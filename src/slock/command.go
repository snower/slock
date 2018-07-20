package slock

import (
    "encoding/binary"
    "bytes"
)

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
    Decode(buf *Buffer) error
}

type CommandEncode interface {
    Encode(buf *Buffer) error
}

type Command struct {
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [16]byte
}

func NewCommand(buf *Buffer) *Command {
    command := Command{}
    if command.Decode(buf) != nil {
        return nil
    }

    return &command
}

func (self *Command) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *Command) Encode(buf *Buffer) error {
    wbuf := new(bytes.Buffer)

    err := binary.Write(wbuf, binary.LittleEndian, self)
    if err != nil {
        return err
    }

    err = binary.Write(wbuf, binary.LittleEndian, make([]byte, 45))

    buf.Write(wbuf.Bytes())
    return err
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

func (self *ResultCommand) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *ResultCommand) Encode(buf *Buffer) error {
    wbuf := new(bytes.Buffer)

    err := binary.Write(wbuf, binary.LittleEndian, self)
    if err != nil {
        return err
    }

    err = binary.Write(wbuf, binary.LittleEndian, make([]byte, 44))

    buf.Write(wbuf.Bytes())
    return err
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

func NewLockCommand(buf *Buffer) *LockCommand {
    command := LockCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *LockCommand) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *LockCommand) Encode(buf *Buffer) error {
    return binary.Write(buf, binary.LittleEndian, self)
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

func (self *LockResultCommand) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *LockResultCommand) Encode(buf *Buffer) error {
    return binary.Write(buf, binary.LittleEndian, self)
}

type StateCommand struct {
    Command
    Flag      uint8
    DbId uint8
    Blank [43]byte
}

func NewStateCommand(buf *Buffer) *StateCommand {
    command := StateCommand{}
    if command.Decode(buf) != nil {
        return nil
    }
    return &command
}

func (self *StateCommand) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *StateCommand) Encode(buf *Buffer) error {
    return binary.Write(buf, binary.LittleEndian, self)
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

func (self *ResultStateCommand) Decode(buf *Buffer) error{
    return binary.Read(buf, binary.LittleEndian, self)
}

func (self *ResultStateCommand) Encode(buf *Buffer) error {
    return binary.Write(buf, binary.LittleEndian, self)
}