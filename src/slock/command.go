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
    GetProtocol() Protocol
}

type CommandDecode interface {
    Decode([]byte) error
}

type CommandEncode interface {
    Encode() ([]byte, error)
}

type Command struct {
    Protocol  Protocol
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [16]byte
}

func NewCommand(protocol *ServerProtocol, b []byte) *Command {
    command := Command{}
    command.Protocol = protocol
    command.Decode(b)
    return &command
}

func (self *Command) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    return nil
}

func (self *Command) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, make([]byte, 45))
    return buf.Bytes(), nil
}

func (self *Command) GetCommandType() uint8{
    return self.CommandType
}

func (self *Command) GetProtocol() Protocol{
    return self.Protocol
}

type ResultCommand struct {
    Protocol  Protocol
    Magic     uint8
    Version   uint8
    CommandType   uint8
    RequestId [16]byte
    Result    uint8
}

func NewResultCommand(command *Command, result uint8) *ResultCommand {
    return &ResultCommand{command.Protocol, MAGIC, VERSION, command.CommandType, command.RequestId, result}
}

func (self *ResultCommand) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    binary.Read(reader, binary.LittleEndian, &self.Result)
    return nil
}

func (self *ResultCommand) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, self.Result)
    binary.Write(buf, binary.LittleEndian, make([]byte, 44))
    return buf.Bytes(), nil
}

func (self *ResultCommand) GetCommandType() uint8{
    return self.CommandType
}

func (self *ResultCommand) GetProtocol() Protocol{
    return self.Protocol
}

type LockCommand struct {
    Command
    Flag      uint8
    DbId      uint8
    LockId    [16]byte
    LockKey   [16]byte
    Timeout   uint32
    Expried   uint32
}

func NewLockCommand(protocol *ServerProtocol, b []byte) *LockCommand {
    command := LockCommand{}
    command.Protocol = protocol
    command.Decode(b)
    return &command
}

var LOCK_COMMAND_BLANK_BYTERS = [9]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

func (self *LockCommand) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    binary.Read(reader, binary.LittleEndian, &self.Flag)
    binary.Read(reader, binary.LittleEndian, &self.DbId)
    binary.Read(reader, binary.LittleEndian, &self.LockId)
    binary.Read(reader, binary.LittleEndian, &self.LockKey)
    binary.Read(reader, binary.LittleEndian, &self.Timeout)
    binary.Read(reader, binary.LittleEndian, &self.Expried)
    return nil
}

func (self *LockCommand) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, self.Flag)
    binary.Write(buf, binary.LittleEndian, self.DbId)
    binary.Write(buf, binary.LittleEndian, self.LockId)
    binary.Write(buf, binary.LittleEndian, self.LockKey)
    binary.Write(buf, binary.LittleEndian, self.Timeout)
    binary.Write(buf, binary.LittleEndian, self.Expried)
    binary.Write(buf, binary.LittleEndian, LOCK_COMMAND_BLANK_BYTERS)
    return buf.Bytes(), nil
}

var RESULT_LOCK_COMMAND_BLANK_BYTERS = [10]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

type LockResultCommand struct {
    ResultCommand
    Flag      uint8
    DbId      uint8
    LockId    [16]byte
    LockKey   [16]byte
}

func NewLockResultCommand(command *LockCommand, result uint8, flag uint8) *LockResultCommand {
    result_command := ResultCommand{command.Protocol, MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &LockResultCommand{result_command, flag, command.DbId, command.LockId, command.LockKey}
}

func (self *LockResultCommand) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    binary.Read(reader, binary.LittleEndian, &self.Result)
    binary.Read(reader, binary.LittleEndian, &self.Flag)
    binary.Read(reader, binary.LittleEndian, &self.DbId)
    binary.Read(reader, binary.LittleEndian, &self.LockId)
    binary.Read(reader, binary.LittleEndian, &self.LockKey)
    return nil
}

func (self *LockResultCommand) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, self.Result)
    binary.Write(buf, binary.LittleEndian, self.Flag)
    binary.Write(buf, binary.LittleEndian, self.DbId)
    binary.Write(buf, binary.LittleEndian, self.LockId)
    binary.Write(buf, binary.LittleEndian, self.LockKey)
    binary.Write(buf, binary.LittleEndian, RESULT_LOCK_COMMAND_BLANK_BYTERS)
    return buf.Bytes(), nil
}

type StateCommand struct {
    Command
    Flag      uint8
    DbId uint8
}

func NewStateCommand(protocol *ServerProtocol, b []byte) *StateCommand {
    command := StateCommand{}
    command.Protocol = protocol
    command.Decode(b)
    return &command
}

func (self *StateCommand) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    binary.Read(reader, binary.LittleEndian, &self.Flag)
    binary.Read(reader, binary.LittleEndian, &self.DbId)
    return nil
}

func (self *StateCommand) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, self.Flag)
    binary.Write(buf, binary.LittleEndian, self.DbId)
    return buf.Bytes(), nil
}

type ResultStateCommand struct {
    ResultCommand
    Flag      uint8
    DbState uint8
    DbId uint8
    State *LockDBState
}

func NewStateResultCommand(command *StateCommand, result uint8, flag uint8, db_state uint8, state *LockDBState) *ResultStateCommand {
    result_command := ResultCommand{command.Protocol, MAGIC, VERSION, command.CommandType, command.RequestId, result}
    return &ResultStateCommand{result_command, flag, db_state, command.DbId, state}
}

func (self *ResultStateCommand) Decode(b []byte) error{
    reader := bytes.NewReader(b)
    self.State = &LockDBState{0, 0, 0, 0, 0, 0, 0, 0}

    binary.Read(reader, binary.LittleEndian, &self.Magic)
    binary.Read(reader, binary.LittleEndian, &self.Version)
    binary.Read(reader, binary.LittleEndian, &self.CommandType)
    binary.Read(reader, binary.LittleEndian, &self.RequestId)
    binary.Read(reader, binary.LittleEndian, &self.Result)
    binary.Read(reader, binary.LittleEndian, &self.Flag)
    binary.Read(reader, binary.LittleEndian, &self.DbState)
    binary.Read(reader, binary.LittleEndian, &self.DbId)

    binary.Read(reader, binary.LittleEndian, &self.State.LockCount)
    binary.Read(reader, binary.LittleEndian, &self.State.UnLockCount)
    binary.Read(reader, binary.LittleEndian, &self.State.LockedCount)
    binary.Read(reader, binary.LittleEndian, &self.State.WaitCount)
    binary.Read(reader, binary.LittleEndian, &self.State.TimeoutedCount)
    binary.Read(reader, binary.LittleEndian, &self.State.ExpriedCount)
    binary.Read(reader, binary.LittleEndian, &self.State.UnlockErrorCount)
    binary.Read(reader, binary.LittleEndian, &self.State.KeyCount)

    return nil
}

func (self *ResultStateCommand) Encode() (b []byte, err error) {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, self.Magic)
    binary.Write(buf, binary.LittleEndian, self.Version)
    binary.Write(buf, binary.LittleEndian, self.CommandType)
    binary.Write(buf, binary.LittleEndian, self.RequestId)
    binary.Write(buf, binary.LittleEndian, self.Result)
    binary.Write(buf, binary.LittleEndian, self.Flag)
    binary.Write(buf, binary.LittleEndian, self.DbState)
    binary.Write(buf, binary.LittleEndian, self.DbId)
    if self.State == nil {
        binary.Write(buf, binary.LittleEndian, make([]byte, 41))
    }else{
        binary.Write(buf, binary.LittleEndian, self.State.LockCount)
        binary.Write(buf, binary.LittleEndian, self.State.UnLockCount)
        binary.Write(buf, binary.LittleEndian, self.State.LockedCount)
        binary.Write(buf, binary.LittleEndian, self.State.WaitCount)
        binary.Write(buf, binary.LittleEndian, self.State.TimeoutedCount)
        binary.Write(buf, binary.LittleEndian, self.State.ExpriedCount)
        binary.Write(buf, binary.LittleEndian, self.State.UnlockErrorCount)
        binary.Write(buf, binary.LittleEndian, self.State.KeyCount)
        binary.Write(buf, binary.LittleEndian, make([]byte, 1))
    }
    return buf.Bytes(), nil
}