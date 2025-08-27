package protocol

import (
	"errors"
	"strings"
)

const MAGIC = 0x56
const VERSION = 0x01

const (
	COMMAND_INIT        = 0
	COMMAND_LOCK        = 1
	COMMAND_UNLOCK      = 2
	COMMAND_STATE       = 3
	COMMAND_ADMIN       = 4
	COMMAND_PING        = 5
	COMMAND_QUIT        = 6
	COMMAND_CALL        = 7
	COMMAND_WILL_LOCK   = 8
	COMMAND_WILL_UNLOCK = 9
	COMMAND_LEADER      = 10
	COMMAND_SUBSCRIBE   = 11
	COMMAND_PUBLISH     = 12
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
	RESULT_LOCK_ACK_WAITING
)

const (
	LOCK_FLAG_SHOW_WHEN_LOCKED   = 0x01
	LOCK_FLAG_UPDATE_WHEN_LOCKED = 0x02
	LOCK_FLAG_FROM_AOF           = 0x04
	LOCK_FLAG_CONCURRENT_CHECK   = 0x08
	LOCK_FLAG_LOCK_TREE_LOCK     = 0x10
	LOCK_FLAG_CONTAINS_DATA      = 0x20
)

const (
	UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED = 0x01
	UNLOCK_FLAG_CANCEL_WAIT_LOCK_WHEN_UNLOCKED  = 0x02
	UNLOCK_FLAG_FROM_AOF                        = 0x04
	UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT             = 0x08
	UNLOCK_FLAG_UNLOCK_TREE_LOCK                = 0x10
	UNLOCK_FLAG_CONTAINS_DATA                   = 0x20
)

const (
	TIMEOUT_FLAG_TIMEOUT_WHEN_CONTAINS_DATA            = 0x0008
	TIMEOUT_FLAG_RCOUNT_IS_PRIORITY                    = 0x0010
	TIMEOUT_FLAG_PUSH_SUBSCRIBE                        = 0x0020
	TIMEOUT_FLAG_MINUTE_TIME                           = 0x0040
	TIMEOUT_FLAG_REVERSE_KEY_LOCK_WHEN_TIMEOUT         = 0x0080
	TIMEOUT_FLAG_UNRENEW_EXPRIED_TIME_WHEN_TIMEOUT     = 0x0100
	TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK                 = 0x0200
	TIMEOUT_FLAG_MILLISECOND_TIME                      = 0x0400
	TIMEOUT_FLAG_LOG_ERROR_WHEN_TIMEOUT                = 0x0800
	TIMEOUT_FLAG_REQUIRE_ACKED                         = 0x1000
	TIMEOUT_FLAG_UPDATE_NO_RESET_TIMEOUT_CHECKED_COUNT = 0x2000
	TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED      = 0x4000
	TIMEOUT_FLAG_KEEPLIVED                             = 0x8000
)

const (
	EXPRIED_FLAG_PUSH_SUBSCRIBE                        = 0x0020
	EXPRIED_FLAG_MINUTE_TIME                           = 0x0040
	EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED         = 0x0080
	EXPRIED_FLAG_ZEOR_AOF_TIME                         = 0x0100
	EXPRIED_FLAG_UNLIMITED_AOF_TIME                    = 0x0200
	EXPRIED_FLAG_MILLISECOND_TIME                      = 0x0400
	EXPRIED_FLAG_LOG_ERROR_WHEN_EXPRIED                = 0x0800
	EXPRIED_FLAG_AOF_TIME_OF_EXPRIED_PARCENT           = 0x1000
	EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT = 0x2000
	EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME                = 0x4000
	EXPRIED_FLAG_KEEPLIVED                             = 0x8000
)

const (
	CALL_COMMAND_ENCODING_TEXT     = 1
	CALL_COMMAND_ENCODING_JSON     = 2
	CALL_COMMAND_ENCODING_PROTOBUF = 3

	CALL_COMMAND_CHARSET_UTF8 = 1
)

const (
	LOCK_DATA_STAGE_CURRENT = 0
	LOCK_DATA_STAGE_UNLOCK  = 1
	LOCK_DATA_STAGE_TIMEOUT = 2
	LOCK_DATA_STAGE_EXPRIED = 3
)

const (
	LOCK_DATA_COMMAND_TYPE_SET      = 0
	LOCK_DATA_COMMAND_TYPE_UNSET    = 1
	LOCK_DATA_COMMAND_TYPE_INCR     = 2
	LOCK_DATA_COMMAND_TYPE_APPEND   = 3
	LOCK_DATA_COMMAND_TYPE_SHIFT    = 4
	LOCK_DATA_COMMAND_TYPE_EXECUTE  = 5
	LOCK_DATA_COMMAND_TYPE_PIPELINE = 6
	LOCK_DATA_COMMAND_TYPE_PUSH     = 7
	LOCK_DATA_COMMAND_TYPE_POP      = 8
)

const (
	LOCK_DATA_FLAG_VALUE_TYPE_NUMBER     = 0x01
	LOCK_DATA_FLAG_VALUE_TYPE_ARRAY      = 0x02
	LOCK_DATA_FLAG_VALUE_TYPE_KV         = 0x04
	LOCK_DATA_FLAG_CONTAINS_PROPERTY     = 0x10
	LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST = 0x20
)

const LOCK_DATA_PROPERTY_CODE_KEY = 1

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
	Magic       uint8
	Version     uint8
	CommandType uint8
	RequestId   [16]byte
}

func NewCommand(commandType uint8) *Command {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: commandType, RequestId: GenRequestId()}
	return &command
}

func (self *Command) Decode(buf []byte) error {
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

func (self *Command) GetCommandType() uint8 {
	return self.CommandType
}

func (self *Command) GetRequestId() [16]byte {
	return self.RequestId
}

type ResultCommand struct {
	Magic       uint8
	Version     uint8
	CommandType uint8
	RequestId   [16]byte
	Result      uint8
}

func NewResultCommand(command ICommand, result uint8) *ResultCommand {
	return &ResultCommand{MAGIC, VERSION, command.GetCommandType(), command.GetRequestId(), result}
}

func (self *ResultCommand) Decode(buf []byte) error {
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

func (self *ResultCommand) GetCommandType() uint8 {
	return self.CommandType
}

func (self *ResultCommand) GetRequestId() [16]byte {
	return self.RequestId
}

type InitCommand struct {
	Command
	ClientId [16]byte
	Blank    [29]byte
}

func NewInitCommand(clientId [16]byte) *InitCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_INIT, RequestId: GenRequestId()}
	initCommand := InitCommand{Command: command, ClientId: clientId, Blank: [29]byte{}}
	return &initCommand
}

func (self *InitCommand) Decode(buf []byte) error {
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

	for i := 0; i < 29; i++ {
		buf[35+i] = 0x00
	}

	return nil
}

var INIT_COMMAND_BLANK_BYTERS = [43]byte{}

type InitResultCommand struct {
	ResultCommand
	/*
	   |7                 |     4     |    3     |    2    |      1        |      0      |
	   |------------------|-----------|----------|---------|-------------- |-------------|
	   |                  |is_shutdown|has_leader|is_leader|is_transparency|has_client_id|
	*/
	InitType uint8
	Blank    [43]byte
}

func NewInitResultCommand(command *InitCommand, result uint8, initType uint8) *InitResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &InitResultCommand{resultCommand, initType, INIT_COMMAND_BLANK_BYTERS}
}

func BuildInitResultCommand(result uint8, initType uint8) *InitResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, COMMAND_INIT, GenRequestId(), result}
	return &InitResultCommand{resultCommand, initType, INIT_COMMAND_BLANK_BYTERS}
}

func (self *InitResultCommand) Decode(buf []byte) error {
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

	for i := 0; i < 43; i++ {
		buf[21+i] = 0x00
	}

	return nil
}

type LockCommandDataProperty struct {
	Code  uint8
	Value []byte
}

func NewLockCommandDataProperty(code uint8, value []byte) *LockCommandDataProperty {
	return &LockCommandDataProperty{code, value}
}

func (self *LockCommandDataProperty) GetValueString() string {
	if self.Value == nil {
		return ""
	}
	return string(self.Value)
}

type LockCommandData struct {
	Data         []byte
	CommandStage uint8
	CommandType  uint8
	DataFlag     uint8
}

func NewLockCommandDataFromOriginBytes(data []byte) *LockCommandData {
	return &LockCommandData{data, data[4] >> 6, data[4] & 0x3f, data[5]}
}

func NewLockCommandDataFromBytes(data []byte, commandStage uint8, commandType uint8, dataFlag uint8, properties []*LockCommandDataProperty) *LockCommandData {
	dataLen, propertyLen := len(data)+2, 0
	if properties != nil {
		for _, property := range properties {
			if property.Value == nil {
				propertyLen += 3
			} else {
				propertyLen += len(property.Value) + 3
			}
		}
		dataLen += propertyLen + 2
		dataFlag |= LOCK_DATA_FLAG_CONTAINS_PROPERTY
	}
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (commandStage<<6)|(commandType&0x3f), dataFlag
	index := 6
	if properties != nil {
		buf[6], buf[7] = byte(propertyLen), byte(propertyLen>>8)
		index += 2
		for _, property := range properties {
			if property.Value == nil {
				buf[index], buf[index+1], buf[index+2] = property.Code, 0, 0
				index += 3
			} else {
				buf[index], buf[index+1], buf[index+2] = property.Code, byte(len(property.Value)), byte(len(property.Value)>>8)
				copy(buf[index+3:], property.Value)
				index += len(property.Value) + 3
			}
		}
	}
	copy(buf[index:], data)
	return &LockCommandData{buf, commandStage, commandType, dataFlag}
}

func NewLockCommandDataFromString(data string, commandStage uint8, commandType uint8, dataFlag uint8, properties []*LockCommandDataProperty) *LockCommandData {
	dataLen, propertyLen := len(data)+2, 0
	if properties != nil {
		for _, property := range properties {
			if property.Value == nil {
				propertyLen += 3
			} else {
				propertyLen += len(property.Value) + 3
			}
		}
		dataLen += propertyLen + 2
		dataFlag |= LOCK_DATA_FLAG_CONTAINS_PROPERTY
	}
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (commandStage<<6)|(commandType&0x3f), dataFlag
	index := 6
	if properties != nil {
		buf[6], buf[7] = byte(propertyLen), byte(propertyLen>>8)
		index += 2
		for _, property := range properties {
			if property.Value == nil {
				buf[index], buf[index+1], buf[index+2] = property.Code, 0, 0
				index += 3
			} else {
				buf[index], buf[index+1], buf[index+2] = property.Code, byte(len(property.Value)), byte(len(property.Value)>>8)
				copy(buf[index+3:], property.Value)
				index += len(property.Value) + 3
			}
		}
	}
	copy(buf[index:], data)
	return &LockCommandData{buf, commandStage, commandType, dataFlag}
}

func NewLockCommandDataSetData(data []byte) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, 0, nil)
}

func NewLockCommandDataSetString(data string) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, 0, nil)
}

func NewLockCommandDataSetDataWithProperty(data []byte, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, 0, properties)
}

func NewLockCommandDataSetStringWithProperty(data string, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, 0, properties)
}

func NewLockCommandDataSetArray(data [][]byte) *LockCommandData {
	size := 0
	for _, value := range data {
		size += len(value) + 4
	}
	dataLen := size + 2
	i, buf := 6, make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (LOCK_DATA_STAGE_CURRENT<<6)|LOCK_DATA_COMMAND_TYPE_SET, LOCK_DATA_FLAG_VALUE_TYPE_ARRAY
	for _, value := range data {
		valueLen := len(value)
		buf[i], buf[i+1], buf[i+2], buf[i+3] = byte(valueLen), byte(valueLen>>8), byte(valueLen>>16), byte(valueLen>>24)
		i += 4
		i += copy(buf[i:], value)
	}
	return &LockCommandData{buf, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, LOCK_DATA_FLAG_VALUE_TYPE_ARRAY}
}

func NewLockCommandDataSetKV(data map[string][]byte) *LockCommandData {
	size := 0
	for key, value := range data {
		size += len(key) + 4
		size += len(value) + 4
	}
	dataLen := size + 2
	i, buf := 6, make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (LOCK_DATA_STAGE_CURRENT<<6)|LOCK_DATA_COMMAND_TYPE_SET, LOCK_DATA_FLAG_VALUE_TYPE_KV
	for key, value := range data {
		keyLen := len(value)
		buf[i], buf[i+1], buf[i+2], buf[i+3] = byte(keyLen), byte(keyLen>>8), byte(keyLen>>16), byte(keyLen>>24)
		i += 4
		i += copy(buf[i:], key)
		valueLen := len(value)
		buf[i], buf[i+1], buf[i+2], buf[i+3] = byte(valueLen), byte(valueLen>>8), byte(valueLen>>16), byte(valueLen>>24)
		i += 4
		i += copy(buf[i:], value)
	}
	return &LockCommandData{buf, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, LOCK_DATA_FLAG_VALUE_TYPE_KV}
}

func NewLockCommandDataUnsetData() *LockCommandData {
	return &LockCommandData{[]byte{2, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_UNSET, 0}, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_UNSET, 0}
}

func NewLockCommandDataUnsetDataWithFlag(dataFlag uint8) *LockCommandData {
	return &LockCommandData{[]byte{2, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_UNSET, dataFlag}, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_UNSET, dataFlag}
}

func NewLockCommandDataIncrData(incrValue int64) *LockCommandData {
	return &LockCommandData{[]byte{10, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_INCR, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
		byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
		LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_INCR, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER}
}

func NewLockCommandDataIncrDataWithProperty(incrValue int64, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromBytes([]byte{byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
		LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_INCR, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER, properties)
}

func NewLockCommandDataAppendData(data []byte) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, 0, nil)
}

func NewLockCommandDataAppendString(data string) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, 0, nil)
}

func NewLockCommandDataAppendDataWithProperty(data []byte, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, 0, properties)
}

func NewLockCommandDataAppendStringWithProperty(data string, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, 0, properties)
}

func NewLockCommandDataShiftData(lengthValue uint32) *LockCommandData {
	return &LockCommandData{[]byte{6, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_SHIFT, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
		byte(lengthValue), byte(lengthValue >> 8), byte(lengthValue >> 16), byte(lengthValue >> 24)},
		LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SHIFT, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER}
}

func NewLockCommandDataExecuteData(lockCommand *LockCommand, commandStage uint8) *LockCommandData {
	return NewLockCommandDataExecuteDataWithFlag(lockCommand, commandStage, 0)
}

func NewLockCommandDataExecuteDataWithFlag(lockCommand *LockCommand, commandStage uint8, dataFlag uint8) *LockCommandData {
	dataLen := 66
	if lockCommand.Data != nil {
		lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		dataLen += len(lockCommand.Data.Data)
	}
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (commandStage<<6)|LOCK_DATA_COMMAND_TYPE_EXECUTE, dataFlag
	err := lockCommand.Encode(buf[6:70])
	if err != nil {
		return nil
	}
	if lockCommand.Data != nil {
		copy(buf[70:], lockCommand.Data.Data)
	}
	return &LockCommandData{buf, commandStage, LOCK_DATA_COMMAND_TYPE_EXECUTE, dataFlag}
}

func NewLockCommandDataPipelineData(lockCommandDatas []*LockCommandData) *LockCommandData {
	dataLen := 2
	for _, lockCommandData := range lockCommandDatas {
		dataLen += len(lockCommandData.Data)
	}
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = LOCK_DATA_COMMAND_TYPE_PIPELINE, 0
	index := 6
	for _, lockCommandData := range lockCommandDatas {
		copy(buf[index:], lockCommandData.Data)
		index += len(lockCommandData.Data)
	}
	return &LockCommandData{buf, 0, LOCK_DATA_COMMAND_TYPE_PIPELINE, 0}
}

func NewLockCommandDataPushData(data []byte) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, 0, nil)
}

func NewLockCommandDataPushString(data string) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, 0, nil)
}

func NewLockCommandDataPushDataWithProperty(data []byte, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromBytes(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, 0, properties)
}

func NewLockCommandDataPushStringWithProperty(data string, properties []*LockCommandDataProperty) *LockCommandData {
	return NewLockCommandDataFromString(data, LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, 0, properties)
}

func NewLockCommandDataPopData(popValue uint32) *LockCommandData {
	return &LockCommandData{[]byte{6, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_POP, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
		byte(popValue), byte(popValue >> 8), byte(popValue >> 16), byte(popValue >> 24)},
		LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_POP, LOCK_DATA_FLAG_VALUE_TYPE_NUMBER}
}

func (self *LockCommandData) GetValueOffset() int {
	if self.DataFlag&LOCK_DATA_FLAG_CONTAINS_PROPERTY != 0 {
		return (int(self.Data[6]) | (int(self.Data[7]) << 8)) + 8
	}
	return 6
}

func (self *LockCommandData) GetValueSize() int {
	return len(self.Data) - self.GetValueOffset()
}

func (self *LockCommandData) GetBytesValue() []byte {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return nil
	}
	return self.Data[self.GetValueOffset():]
}

func (self *LockCommandData) GetStringValue() string {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return ""
	}
	return string(self.Data[self.GetValueOffset():])
}

func (self *LockCommandData) GetIncrValue() int64 {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return 0
	}
	valueOffset, value := self.GetValueOffset(), int64(0)
	for i := 0; i < 8; i++ {
		if i+valueOffset >= len(self.Data) {
			break
		}
		if i > 0 {
			value |= int64(self.Data[i+valueOffset]) << (i * 8)
		} else {
			value |= int64(self.Data[i+valueOffset])
		}
	}
	return value
}

func (self *LockCommandData) GetShiftLengthValue() uint32 {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return 0
	}
	valueOffset, value := self.GetValueOffset(), uint32(0)
	for i := 0; i < 4; i++ {
		if i+valueOffset >= len(self.Data) {
			break
		}
		if i > 0 {
			value |= uint32(self.Data[i+valueOffset]) << (i * 8)
		} else {
			value |= uint32(self.Data[i+valueOffset])
		}
	}
	return value
}

func (self *LockCommandData) GetPopCountValue() uint32 {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return 0
	}
	valueOffset, value := self.GetValueOffset(), uint32(0)
	for i := 0; i < 4; i++ {
		if i+valueOffset >= len(self.Data) {
			break
		}
		if i > 0 {
			value |= uint32(self.Data[i+valueOffset]) << (i * 8)
		} else {
			value |= uint32(self.Data[i+valueOffset])
		}
	}
	return value
}

func (self *LockCommandData) DecodeLockCommand(lockCommand *LockCommand) error {
	valueOffset := self.GetValueOffset()
	if len(self.Data) < valueOffset+64 {
		return errors.New("data size error")
	}
	err := lockCommand.Decode(self.Data[valueOffset : valueOffset+64])
	if err != nil {
		return err
	}
	if lockCommand.Flag&LOCK_FLAG_CONTAINS_DATA != 0 {
		if len(self.Data) < valueOffset+68 {
			return errors.New("data size error")
		}
		dataLen := int(uint32(self.Data[valueOffset+64]) | uint32(self.Data[valueOffset+65])<<8 | uint32(self.Data[valueOffset+66])<<16 | uint32(self.Data[valueOffset+67])<<24)
		buf := make([]byte, dataLen+4)
		buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
		if dataLen <= 0 {
			return nil
		}
		if len(self.Data) < valueOffset+dataLen+68 {
			return errors.New("data size error")
		}
		copy(buf[4:], self.Data[valueOffset+68:valueOffset+dataLen+68])
		lockCommand.Data = NewLockCommandDataFromOriginBytes(buf)
	}
	return nil
}

type LockCommand struct {
	Command
	Flag        uint8
	DbId        uint8
	LockId      [16]byte
	LockKey     [16]byte
	TimeoutFlag uint16
	/*
	   |    15  |              14              |                13                   |  12 |        11      |       10       |      9       |           8        |             7          |      6    |       5      |        4         |            3             |2        0|
	   |--------|------------------------------|-------------------------------------|-----|----------------|----------------|--------------|--------------------|------------------------|-----------|--------------|------------------|--------------------------|----------|
	   |keeplive|less_request_id_is_lock_succed|update_no_reset_timeout_checked_count|acked|timeout_is_error|millisecond_time|unlock_to_wait|unrenew_expried_time|timeout_reverse_key_lock|minute_time|push_subscribe|rcount_is_priority|timeout_when_contains_data|          |
	*/
	Timeout     uint16
	ExpriedFlag uint16
	/*
	   |    15  |          14          |                13                   |                12         |        11      |       10       |         9        |        8    |            7           |     6     |    5         |4            0|
	   |--------|----------------------|-------------------------------------|---------------------------|----------------|----------------|------------------|-------------|------------------------|-----------|--------------|--------------|
	   |keeplive|unlimited_expried_time|update_no_reset_expried_checked_count|aof_time_of_expried_parcent|expried_is_error|millisecond_time|unlimited_aof_time|zeor_aof_time|expried_reverse_key_lock|minute_time|push_subscribe|              |
	*/
	Expried uint16
	Count   uint16
	Rcount  uint8
	Data    *LockCommandData
}

func NewLockCommand(dbId uint8, lockKey [16]byte, lockId [16]byte, timeout uint16, expried uint16, count uint16) *LockCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_LOCK, RequestId: GenRequestId()}
	lockCommand := LockCommand{Command: command, Flag: 0, DbId: dbId, LockId: lockId, LockKey: lockKey, TimeoutFlag: 0,
		Timeout: timeout, ExpriedFlag: 0, Expried: expried, Count: count, Rcount: 0, Data: nil}
	return &lockCommand
}

func (self *LockCommand) Decode(buf []byte) error {
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

	self.Timeout, self.TimeoutFlag, self.Expried, self.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
	self.Count, self.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])
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

	buf[53], buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60] = byte(self.Timeout), byte(self.Timeout>>8), byte(self.TimeoutFlag), byte(self.TimeoutFlag>>8), byte(self.Expried), byte(self.Expried>>8), byte(self.ExpriedFlag), byte(self.ExpriedFlag>>8)

	buf[61], buf[62], buf[63] = byte(self.Count), byte(self.Count>>8), byte(self.Rcount)

	return nil
}

func (self *LockCommand) GetLockData() *LockCommandData {
	return self.Data
}

var RESULT_LOCK_COMMAND_BLANK_BYTERS = [4]byte{}

type LockResultCommandData struct {
	Data         []byte
	CommandStage uint8
	CommandType  uint8
	DataFlag     uint8
}

func NewLockResultCommandDataFromOriginBytes(data []byte) *LockResultCommandData {
	return &LockResultCommandData{data, data[4] >> 6, data[4] & 0x3f, data[5]}
}

func NewLockResultCommandDataFromBytes(data []byte, commandStage uint8, commandType uint8, dataFlag uint8) *LockResultCommandData {
	dataLen := len(data) + 2
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (commandStage<<6)|(commandType&0x3f), dataFlag
	copy(buf[6:], data)
	return &LockResultCommandData{buf, commandStage, commandType, dataFlag}
}

func NewLockResultCommandDataFromString(data string, commandStage uint8, commandType uint8, dataFlag uint8) *LockResultCommandData {
	dataLen := len(data) + 2
	buf := make([]byte, dataLen+4)
	buf[0], buf[1], buf[2], buf[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
	buf[4], buf[5] = (commandStage<<6)|(commandType&0x3f), dataFlag
	copy(buf[6:], data)
	return &LockResultCommandData{buf, commandStage, commandType, dataFlag}
}

func (self *LockResultCommandData) GetValueOffset() int {
	if self.DataFlag&LOCK_DATA_FLAG_CONTAINS_PROPERTY != 0 {
		return (int(self.Data[6]) | (int(self.Data[7]) << 8)) + 8
	}
	return 6
}

func (self *LockResultCommandData) GetValueSize() int {
	return len(self.Data) - self.GetValueOffset()
}

func (self *LockResultCommandData) GetBytesValue() []byte {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return nil
	}
	return self.Data[self.GetValueOffset():]
}

func (self *LockResultCommandData) GetStringValue() string {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return ""
	}
	return string(self.Data[self.GetValueOffset():])
}

func (self *LockResultCommandData) GetIncrValue() int64 {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET {
		return 0
	}
	valueOffset, value := self.GetValueOffset(), int64(0)
	for i := 0; i < 8; i++ {
		if i+valueOffset >= len(self.Data) {
			break
		}
		if i > 0 {
			value |= int64(self.Data[i+valueOffset]) << (i * 8)
		} else {
			value |= int64(self.Data[i+valueOffset])
		}
	}
	return value
}

func (self *LockResultCommandData) GetArrayValue() [][]byte {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET || self.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_ARRAY == 0 {
		return nil
	}
	values := make([][]byte, 0)
	index := self.GetValueOffset()
	for index+4 < len(self.Data) {
		valueLen := int(uint32(self.Data[index]) | uint32(self.Data[index+1])<<8 | uint32(self.Data[index+2])<<16 | uint32(self.Data[index+3])<<24)
		if valueLen == 0 {
			index += 4
			continue
		}
		values = append(values, self.Data[index+4:index+4+valueLen])
		index += valueLen + 4
	}
	return values
}

func (self *LockResultCommandData) GetKVValue() map[string][]byte {
	if self.Data == nil || self.CommandType == LOCK_DATA_COMMAND_TYPE_UNSET || self.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_KV == 0 {
		return nil
	}
	values := make(map[string][]byte)
	index := self.GetValueOffset()
	for index+4 < len(self.Data) {
		keyLen := int(uint32(self.Data[index]) | uint32(self.Data[index+1])<<8 | uint32(self.Data[index+2])<<16 | uint32(self.Data[index+3])<<24)
		if keyLen == 0 {
			index += 4
			continue
		}
		key := string(self.Data[index+4 : index+4+keyLen])
		index += keyLen + 4

		valueLen := int(uint32(self.Data[index]) | uint32(self.Data[index+1])<<8 | uint32(self.Data[index+2])<<16 | uint32(self.Data[index+3])<<24)
		if valueLen == 0 {
			index += 4
			continue
		}
		values[key] = self.Data[index+4 : index+4+valueLen]
		index += valueLen + 4
	}
	return values
}

func (self *LockResultCommandData) GetDataProperties() []*LockCommandDataProperty {
	if self.DataFlag&LOCK_DATA_FLAG_CONTAINS_PROPERTY == 0 {
		return nil
	}
	properties := make([]*LockCommandDataProperty, 0)
	propertyLen, index := int(self.Data[6])|int(self.Data[7])<<8, 0
	for index < propertyLen {
		propertyCode, valueLen := self.Data[8+index], int(self.Data[9+index])|int(self.Data[10+index])<<8
		if valueLen > 0 {
			properties = append(properties, NewLockCommandDataProperty(propertyCode, self.Data[11+index:11+index+valueLen]))
		} else {
			properties = append(properties, NewLockCommandDataProperty(propertyCode, nil))
		}
		index += valueLen + 3
	}
	return properties
}

func (self *LockResultCommandData) GetDataProperty(code uint8) *LockCommandDataProperty {
	if self.DataFlag&LOCK_DATA_FLAG_CONTAINS_PROPERTY == 0 {
		return nil
	}
	propertyLen, index := int(self.Data[6])|int(self.Data[7])<<8, 0
	for index < propertyLen {
		propertyCode, valueLen := self.Data[8+index], int(self.Data[9+index])|int(self.Data[10+index])<<8
		if code == propertyCode {
			if valueLen > 0 {
				return NewLockCommandDataProperty(code, self.Data[11+index:11+index+valueLen])
			}
			return NewLockCommandDataProperty(code, nil)
		}
		index += valueLen + 3
	}
	return nil
}

type LockResultCommand struct {
	ResultCommand
	Flag    uint8
	DbId    uint8
	LockId  [16]byte
	LockKey [16]byte
	Lcount  uint16
	Count   uint16
	Lrcount uint8
	Rcount  uint8
	Blank   [4]byte
	Data    *LockResultCommandData
}

func NewLockResultCommand(command *LockCommand, result uint8, flag uint8, lcount uint16, count uint16, lrcount uint8, rcount uint8, data []byte) *LockResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	var lockResultCommandData *LockResultCommandData = nil
	if data != nil {
		lockResultCommandData = NewLockResultCommandDataFromOriginBytes(data)
		flag |= LOCK_FLAG_CONTAINS_DATA
	}
	return &LockResultCommand{resultCommand, flag, command.DbId, command.LockId, command.LockKey,
		lcount, count, lrcount, rcount, RESULT_LOCK_COMMAND_BLANK_BYTERS, lockResultCommandData}
}

func (self *LockResultCommand) Decode(buf []byte) error {
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

	self.Lcount, self.Count, self.Lrcount, self.Rcount = uint16(buf[54])|uint16(buf[55])<<8, uint16(buf[56])|uint16(buf[57])<<8, uint8(buf[58]), uint8(buf[59])

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

	buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(self.Lcount), byte(self.Lcount>>8), byte(self.Count), byte(self.Count>>8), byte(self.Lrcount), byte(self.Rcount), 0x00, 0x00
	buf[62], buf[63] = 0x00, 0x00
	return nil
}

func (self *LockResultCommand) GetLockData() *LockResultCommandData {
	return self.Data
}

type StateCommand struct {
	Command
	Flag  uint8
	DbId  uint8
	Blank [43]byte
}

func NewStateCommand(dbId uint8) *StateCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_STATE, RequestId: GenRequestId()}
	stateCommand := StateCommand{Command: command, Flag: 0, DbId: dbId, Blank: [43]byte{}}
	return &stateCommand
}

func (self *StateCommand) Decode(buf []byte) error {
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

	for i := 0; i < 43; i++ {
		buf[21+i] = 0x00
	}

	return nil
}

type StateResultCommand struct {
	ResultCommand
	Flag    uint8
	DbState uint8
	DbId    uint8
	State   LockDBState
	Blank   [1]byte
}

func NewStateResultCommand(command *StateCommand, result uint8, flag uint8, dbState uint8, state *LockDBState) *StateResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	if state == nil {
		state = &LockDBState{}
	}
	return &StateResultCommand{resultCommand, flag, dbState, command.DbId, *state, [1]byte{}}
}

func (self *StateResultCommand) Decode(buf []byte) error {
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
	AdminType uint8
	Blank     [44]byte
}

func NewAdminCommand(adminType uint8) *AdminCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_ADMIN, RequestId: GenRequestId()}
	adminCommand := AdminCommand{Command: command, AdminType: adminType, Blank: [44]byte{}}
	return &adminCommand
}

func (self *AdminCommand) Decode(buf []byte) error {
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

	for i := 0; i < 44; i++ {
		buf[20+i] = 0x00
	}

	return nil
}

type AdminResultCommand struct {
	ResultCommand
	Blank [44]byte
}

func NewAdminResultCommand(command *AdminCommand, result uint8) *AdminResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &AdminResultCommand{resultCommand, [44]byte{}}
}

func (self *AdminResultCommand) Decode(buf []byte) error {
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

	for i := 0; i < 44; i++ {
		buf[20+i] = 0x00
	}

	return nil
}

type PingCommand struct {
	Command
	Blank [45]byte
}

func NewPingCommand() *PingCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_PING, RequestId: GenRequestId()}
	pingCommand := PingCommand{Command: command, Blank: [45]byte{}}
	return &pingCommand
}

func (self *PingCommand) Decode(buf []byte) error {
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

	for i := 0; i < 45; i++ {
		buf[19+i] = 0x00
	}

	return nil
}

type PingResultCommand struct {
	ResultCommand
	Blank [44]byte
}

func NewPingResultCommand(command *PingCommand, result uint8) *PingResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &PingResultCommand{resultCommand, [44]byte{}}
}

func (self *PingResultCommand) Decode(buf []byte) error {
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

	for i := 0; i < 44; i++ {
		buf[20+i] = 0x00
	}

	return nil
}

type QuitCommand struct {
	Command
	Blank [45]byte
}

func NewQuitCommand() *QuitCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_QUIT, RequestId: GenRequestId()}
	quitCommand := QuitCommand{Command: command, Blank: [45]byte{}}
	return &quitCommand
}

func (self *QuitCommand) Decode(buf []byte) error {
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

	for i := 0; i < 45; i++ {
		buf[19+i] = 0x00
	}

	return nil
}

type QuitResultCommand struct {
	ResultCommand
	Blank [44]byte
}

func NewQuitResultCommand(command *QuitCommand, result uint8) *QuitResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &QuitResultCommand{resultCommand, [44]byte{}}
}

func (self *QuitResultCommand) Decode(buf []byte) error {
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

	for i := 0; i < 44; i++ {
		buf[20+i] = 0x00
	}

	return nil
}

type CallCommand struct {
	Command
	Flag       uint8
	Encoding   uint8
	Charset    uint8
	ContentLen uint32
	MethodName string
	Data       []byte
}

func NewCallCommand(methodName string, data []byte) *CallCommand {
	contentLen := uint32(0)
	if data != nil {
		contentLen = uint32(len(data))
	}

	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_CALL, RequestId: GenRequestId()}
	callCommand := CallCommand{Command: command, Flag: 0, Encoding: CALL_COMMAND_ENCODING_PROTOBUF, Charset: CALL_COMMAND_CHARSET_UTF8, ContentLen: contentLen, MethodName: methodName, Data: data}
	return &callCommand
}

func (self *CallCommand) Decode(buf []byte) error {
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
	self.MethodName = strings.Trim(string(buf[26:64]), string([]byte{0}))
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

	for i := 0; i < 38; i++ {
		if i >= len(self.MethodName) {
			buf[26+i] = 0x00
		} else {
			buf[26+i] = self.MethodName[i]
		}
	}
	return nil
}

type CallResultCommand struct {
	ResultCommand
	Flag       uint8
	Encoding   uint8
	Charset    uint8
	ContentLen uint32
	ErrType    string
	Data       []byte
}

func NewCallResultCommand(command *CallCommand, result uint8, errType string, data []byte) *CallResultCommand {
	contentLen := uint32(0)
	if data != nil {
		contentLen = uint32(len(data))
	}

	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &CallResultCommand{resultCommand, 0, CALL_COMMAND_ENCODING_PROTOBUF, CALL_COMMAND_CHARSET_UTF8, contentLen, errType, data}
}

func (self *CallResultCommand) Decode(buf []byte) error {
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
	self.ErrType = strings.Trim(string(buf[27:64]), string([]byte{0}))

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

	for i := 0; i < 37; i++ {
		if i >= len(self.ErrType) {
			buf[27+i] = 0x00
		} else {
			buf[27+i] = self.ErrType[i]
		}
	}
	return nil
}

type LeaderCommand struct {
	Command
	Flag  uint8
	Blank [44]byte
}

func NewLeaderCommand() *LeaderCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_LEADER, RequestId: GenRequestId()}
	leaderCommand := LeaderCommand{Command: command, Flag: 0, Blank: [44]byte{}}
	return &leaderCommand
}

func (self *LeaderCommand) Decode(buf []byte) error {
	self.Magic = uint8(buf[0])
	self.Version = uint8(buf[1])
	self.CommandType = uint8(buf[2])

	self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
		buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

	self.Flag = uint8(buf[19])
	return nil
}

func (self *LeaderCommand) Encode(buf []byte) error {
	buf[0] = byte(self.Magic)
	buf[1] = byte(self.Version)
	buf[2] = byte(self.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

	buf[19] = byte(self.Flag)

	for i := 0; i < 44; i++ {
		buf[20+i] = 0x00
	}
	return nil
}

type LeaderResultCommand struct {
	ResultCommand
	HostLen uint8
	Host    string
}

func NewLeaderResultCommand(command *LeaderCommand, result uint8, host string) *LeaderResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &LeaderResultCommand{resultCommand, uint8(len(host)), host}
}

func (self *LeaderResultCommand) Decode(buf []byte) error {
	self.Magic = uint8(buf[0])
	self.Version = uint8(buf[1])
	self.CommandType = uint8(buf[2])

	self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
		buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

	self.Result, self.HostLen = uint8(buf[19]), uint8(buf[20])
	self.Host = string(buf[21 : 21+self.HostLen])
	return nil
}

func (self *LeaderResultCommand) Encode(buf []byte) error {
	if len(self.Host) > 43 {
		return errors.New("Host too long")
	}

	buf[0] = byte(self.Magic)
	buf[1] = byte(self.Version)
	buf[2] = byte(self.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

	buf[19] = uint8(self.Result)
	buf[20] = byte(self.HostLen)

	for i := 0; i < 43; i++ {
		if i >= len(self.Host) {
			buf[21+i] = 0x00
		} else {
			buf[21+i] = self.Host[i]
		}
	}
	return nil
}

type SubscribeCommand struct {
	Command
	Flag          uint8
	ClientId      uint32
	SubscribeId   uint32
	SubscribeType uint8
	LockKeyMask   [16]byte
	Expried       uint32
	MaxSize       uint32
	Blank         [11]byte
}

func NewSubscribeCommand(clientId uint32, subscribeId uint32, subscribeType uint8, lockKeyMask [16]byte, expried uint32, maxSize uint32) *SubscribeCommand {
	command := Command{Magic: MAGIC, Version: VERSION, CommandType: COMMAND_SUBSCRIBE, RequestId: GenRequestId()}
	subscribeCommand := SubscribeCommand{Command: command, Flag: 0, ClientId: clientId, SubscribeId: subscribeId,
		SubscribeType: subscribeType, LockKeyMask: lockKeyMask, Expried: expried, MaxSize: maxSize, Blank: [11]byte{}}
	return &subscribeCommand
}

func (self *SubscribeCommand) Decode(buf []byte) error {
	self.Magic = uint8(buf[0])
	self.Version = uint8(buf[1])
	self.CommandType = uint8(buf[2])

	self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
		buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

	self.Flag = uint8(buf[19])
	self.ClientId = uint32(buf[20]) | uint32(buf[21])<<8 | uint32(buf[22])<<16 | uint32(buf[23])<<24
	self.SubscribeId = uint32(buf[24]) | uint32(buf[25])<<8 | uint32(buf[26])<<16 | uint32(buf[27])<<24
	self.SubscribeType = uint8(buf[28])

	self.LockKeyMask[0], self.LockKeyMask[1], self.LockKeyMask[2], self.LockKeyMask[3], self.LockKeyMask[4], self.LockKeyMask[5], self.LockKeyMask[6], self.LockKeyMask[7],
		self.LockKeyMask[8], self.LockKeyMask[9], self.LockKeyMask[10], self.LockKeyMask[11], self.LockKeyMask[12], self.LockKeyMask[13], self.LockKeyMask[14], self.LockKeyMask[15] =
		buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36],
		buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44]

	self.Expried = uint32(buf[45]) | uint32(buf[46])<<8 | uint32(buf[47])<<16 | uint32(buf[48])<<24
	self.MaxSize = uint32(buf[49]) | uint32(buf[50])<<8 | uint32(buf[51])<<16 | uint32(buf[52])<<24
	return nil
}

func (self *SubscribeCommand) Encode(buf []byte) error {
	buf[0] = byte(self.Magic)
	buf[1] = byte(self.Version)
	buf[2] = byte(self.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

	buf[19] = byte(self.Flag)

	buf[20] = byte(self.ClientId)
	buf[21] = byte(self.ClientId >> 8)
	buf[22] = byte(self.ClientId >> 16)
	buf[23] = byte(self.ClientId >> 24)
	buf[24] = byte(self.SubscribeId)
	buf[25] = byte(self.SubscribeId >> 8)
	buf[26] = byte(self.SubscribeId >> 16)
	buf[27] = byte(self.SubscribeId >> 24)

	buf[28] = byte(self.SubscribeType)

	buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36],
		buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44] =
		self.LockKeyMask[0], self.LockKeyMask[1], self.LockKeyMask[2], self.LockKeyMask[3], self.LockKeyMask[4], self.LockKeyMask[5], self.LockKeyMask[6], self.LockKeyMask[7],
		self.LockKeyMask[8], self.LockKeyMask[9], self.LockKeyMask[10], self.LockKeyMask[11], self.LockKeyMask[12], self.LockKeyMask[13], self.LockKeyMask[14], self.LockKeyMask[15]

	buf[45] = byte(self.Expried)
	buf[46] = byte(self.Expried >> 8)
	buf[47] = byte(self.Expried >> 16)
	buf[48] = byte(self.Expried >> 24)

	buf[49] = byte(self.MaxSize)
	buf[50] = byte(self.MaxSize >> 8)
	buf[51] = byte(self.MaxSize >> 16)
	buf[52] = byte(self.MaxSize >> 24)

	for i := 0; i < 11; i++ {
		buf[53+i] = 0x00
	}
	return nil
}

type SubscribeResultCommand struct {
	ResultCommand
	Flag        uint8
	ClientId    uint32
	SubscribeId uint32
	Blank       [35]byte
}

func NewSubscribeResultCommand(command *SubscribeCommand, result uint8, subscribeId uint32) *SubscribeResultCommand {
	resultCommand := ResultCommand{MAGIC, VERSION, command.CommandType, command.RequestId, result}
	return &SubscribeResultCommand{resultCommand, 0, command.ClientId, subscribeId, [35]byte{}}
}

func (self *SubscribeResultCommand) Decode(buf []byte) error {
	self.Magic = uint8(buf[0])
	self.Version = uint8(buf[1])
	self.CommandType = uint8(buf[2])

	self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
		buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

	self.Result, self.Flag = uint8(buf[19]), uint8(buf[20])
	self.ClientId = uint32(buf[21]) | uint32(buf[22])<<8 | uint32(buf[23])<<16 | uint32(buf[24])<<24
	self.SubscribeId = uint32(buf[25]) | uint32(buf[26])<<8 | uint32(buf[27])<<16 | uint32(buf[28])<<24
	return nil
}

func (self *SubscribeResultCommand) Encode(buf []byte) error {
	buf[0] = byte(self.Magic)
	buf[1] = byte(self.Version)
	buf[2] = byte(self.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

	buf[19] = uint8(self.Result)
	buf[20] = byte(self.Flag)

	buf[21] = byte(self.ClientId)
	buf[22] = byte(self.ClientId >> 8)
	buf[23] = byte(self.ClientId >> 16)
	buf[24] = byte(self.ClientId >> 24)
	buf[25] = byte(self.SubscribeId)
	buf[26] = byte(self.SubscribeId >> 8)
	buf[27] = byte(self.SubscribeId >> 16)
	buf[28] = byte(self.SubscribeId >> 24)

	for i := 0; i < 35; i++ {
		buf[29+i] = 0x00
	}
	return nil
}
