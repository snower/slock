package protocol

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ITextProtocol interface {
	GetDBId() uint8
	GetLockId() [16]byte
	GetLockCommand() *LockCommand
	FreeLockCommand(lockCommand *LockCommand) error
	GetParser() *TextParser
}

type ConvertTextCommand func(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error)
type WriteTextCommandResultFunc func(textProtocol ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error

type TextCommandConverter struct {
	handlers map[string]ConvertTextCommand
}

func NewTextCommandConverter() *TextCommandConverter {
	return &TextCommandConverter{nil}
}

func (self *TextCommandConverter) FindHandler(name string) (ConvertTextCommand, error) {
	if self.handlers == nil {
		self.handlers = make(map[string]ConvertTextCommand, 64)
		self.handlers["LOCK"] = self.ConvertTextLockAndUnLockCommand
		self.handlers["UNLOCK"] = self.ConvertTextLockAndUnLockCommand
		self.handlers["DEL"] = self.ConvertTextDelCommand
		self.handlers["SET"] = self.ConvertTextSetCommand
		self.handlers["APPEND"] = self.ConvertTextAppendCommand
		self.handlers["GETSET"] = self.ConvertTextGetSetCommand
		self.handlers["SETEX"] = self.ConvertTextSetEXCommand
		self.handlers["PSETEX"] = self.ConvertTextSetEXCommand
		self.handlers["SETNX"] = self.ConvertTextSetNXCommand
		self.handlers["GET"] = self.ConvertTextGetCommand
		self.handlers["INCR"] = self.ConvertTextIncrCommand
		self.handlers["INCRBY"] = self.ConvertTextIncrCommand
		self.handlers["DECR"] = self.ConvertTextDecrCommand
		self.handlers["DECRBY"] = self.ConvertTextDecrCommand
		self.handlers["STRLEN"] = self.ConvertTextStrlenCommand
		self.handlers["EXISTS"] = self.ConvertTextExistsCommand
		self.handlers["EXPIRE"] = self.ConvertTextExpireCommand
		self.handlers["PEXPIREAT"] = self.ConvertTextExpireCommand
		self.handlers["PEXPIRE"] = self.ConvertTextExpireCommand
		self.handlers["PEXPIREAT"] = self.ConvertTextExpireCommand
		self.handlers["PERSIST"] = self.ConvertTextExpireCommand
		self.handlers["TYPE"] = self.ConvertTextTypeCommand
		self.handlers["DUMP"] = self.ConvertTextDumpCommand
	}
	if handler, ok := self.handlers[name]; ok {
		return handler, nil
	}
	return nil, errors.New("unknown command")
}

func (self *TextCommandConverter) ConvertArgId2LockId(argId string, lockId *[16]byte) {
	argLen := len(argId)
	if argLen == 16 {
		lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
			lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
			byte(argId[0]), byte(argId[1]), byte(argId[2]), byte(argId[3]), byte(argId[4]), byte(argId[5]), byte(argId[6]),
			byte(argId[7]), byte(argId[8]), byte(argId[9]), byte(argId[10]), byte(argId[11]), byte(argId[12]), byte(argId[13]), byte(argId[14]), byte(argId[15])
	} else if argLen > 16 {
		if argLen == 32 {
			v, err := hex.DecodeString(argId)
			if err == nil {
				lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
					lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			} else {
				v := md5.Sum([]byte(argId))
				lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
					lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			}
		} else {
			v := md5.Sum([]byte(argId))
			lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
				lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
				v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
				v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
		}
	} else {
		argIndex := 16 - argLen
		for i := 0; i < 16; i++ {
			if i < argIndex {
				lockId[i] = 0
			} else {
				lockId[i] = argId[i-argIndex]
			}
		}
	}
}

func (self *TextCommandConverter) ConvertArgs2Flag(lockCommand *LockCommand, args []string) error {
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX":
			if i+i >= len(args) {
				return errors.New("Command Parse Args Count Error")
			}
			expried, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errors.New("Command Parse EX Value Error")
			}
			if expried > 65535 {
				if expried%60 == 0 {
					lockCommand.Expried = uint16(expried / 60)
				} else {
					lockCommand.Expried = uint16(expried/60) + 1
				}
				lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
			} else {
				lockCommand.Expried = uint16(expried)
			}
			i++
		case "PX":
			if i+i >= len(args) {
				return errors.New("Command Parse Args Count Error")
			}
			expried, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errors.New("Command Parse PX Value Error")
			}
			if expried > 65535000 {
				if expried%60000 == 0 {
					lockCommand.Expried = uint16(expried / 60000)
				} else {
					lockCommand.Expried = uint16(expried/60000) + 1
				}
				lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
			} else if expried <= 3000 {
				lockCommand.Expried = uint16(expried)
				lockCommand.ExpriedFlag |= EXPRIED_FLAG_MILLISECOND_TIME
			} else {
				lockCommand.Expried = uint16(expried)
			}
			i++
		case "TX":
			if i+i >= len(args) {
				return errors.New("Command Parse Args Count Error")
			}
			timeout, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errors.New("Command Parse TX Value Error")
			}
			if timeout > 65535 {
				if timeout%60 == 0 {
					lockCommand.Timeout = uint16(timeout / 60)
				} else {
					lockCommand.Timeout = uint16(timeout/60) + 1
				}
				lockCommand.TimeoutFlag |= TIMEOUT_FLAG_MINUTE_TIME
			} else {
				lockCommand.Timeout = uint16(timeout)
			}
			i++
		case "PTX":
			if i+i >= len(args) {
				return errors.New("Command Parse Args Count Error")
			}
			timeout, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errors.New("Command Parse TX Value Error")
			}
			if timeout > 65535000 {
				if (timeout/1000)%60 == 0 {
					lockCommand.Timeout = uint16(timeout / 60000)
				} else {
					lockCommand.Timeout = uint16(timeout/60000) + 1
				}
				lockCommand.TimeoutFlag |= TIMEOUT_FLAG_MINUTE_TIME
			} else if timeout <= 3000 {
				lockCommand.Timeout = uint16(timeout)
				lockCommand.TimeoutFlag |= TIMEOUT_FLAG_MILLISECOND_TIME
			} else {
				lockCommand.Timeout = uint16(timeout)
			}
			i++
		case "NX":
			lockCommand.Flag = LOCK_FLAG_CONTAINS_DATA
			lockCommand.LockId = GenLockId()
		case "XX":
			lockCommand.TimeoutFlag |= TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK
		case "ACK":
			lockCommand.TimeoutFlag |= TIMEOUT_FLAG_REQUIRE_ACKED
		case "NAOF":
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UNLIMITED_AOF_TIME
		}
	}
	return nil
}

func (self *TextCommandConverter) GetAndResetLockCommand(textProtocol ITextProtocol) *LockCommand {
	lockCommand := textProtocol.GetLockCommand()
	lockCommand.Magic = MAGIC
	lockCommand.Version = VERSION
	lockCommand.RequestId = GenRequestId()
	lockCommand.DbId = textProtocol.GetDBId()
	lockCommand.Flag = 0
	lockCommand.Timeout = 0
	lockCommand.TimeoutFlag = 0
	lockCommand.Expried = 0
	lockCommand.ExpriedFlag = 0
	lockCommand.Count = 0
	lockCommand.Rcount = 0
	return lockCommand
}

func (self *TextCommandConverter) ConvertTextLockAndUnLockCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	commandName := strings.ToUpper(args[0])
	if commandName == "UNLOCK" {
		lockCommand.CommandType = COMMAND_UNLOCK
	} else {
		lockCommand.CommandType = COMMAND_LOCK
	}
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.Timeout = 15
	lockCommand.Expried = 120

	hasLockId := false
	for i := 2; i < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "LOCK_ID":
			self.ConvertArgId2LockId(args[i+1], &lockCommand.LockId)
			hasLockId = true
		case "FLAG":
			flag, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse FLAG Error")
			}
			lockCommand.Flag = uint8(flag)
		case "TIMEOUT":
			timeout, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse TIMEOUT Error")
			}
			lockCommand.Timeout = uint16(timeout & 0xffff)
			lockCommand.TimeoutFlag = uint16(timeout >> 16 & 0xffff)
		case "EXPRIED":
			expried, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse EXPRIED Error")
			}
			lockCommand.Expried = uint16(expried & 0xffff)
			lockCommand.ExpriedFlag = uint16(expried >> 16 & 0xffff)
		case "COUNT":
			count, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse COUNT Error")
			}
			if count > 0 {
				lockCommand.Count = uint16(count) - 1
			} else {
				lockCommand.Count = uint16(count)
			}
		case "RCOUNT":
			rcount, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse RCOUNT Error")
			}
			if rcount > 0 {
				lockCommand.Rcount = uint8(rcount) - 1
			} else {
				lockCommand.Rcount = uint8(rcount)
			}
		case "WILL":
			willType, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse WILL Error")
			}
			if willType > 0 && commandName != "PUSH" {
				lockCommand.CommandType += 7
			}
		case "SET":
			lockCommand.Data = NewLockCommandDataSetString(args[i+1])
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		case "UNSET":
			lockCommand.Data = NewLockCommandDataUnsetData()
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		case "INCR":
			incrValue, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse INCR Error")
			}
			lockCommand.Data = NewLockCommandDataIncrData(int64(incrValue))
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		case "APPEND":
			lockCommand.Data = NewLockCommandDataAppendString(args[i+1])
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		case "SHIFT":
			lengthValue, err := strconv.Atoi(args[i+1])
			if err != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, errors.New("Command Parse SHIFT Error")
			}
			lockCommand.Data = NewLockCommandDataShiftData(uint32(lengthValue))
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		case "EXECUTE":
			commandStage := uint8(LOCK_DATA_STAGE_LOCK)
			switch strings.ToUpper(args[i+1]) {
			case "UNLOCK":
				commandStage = LOCK_DATA_STAGE_UNLOCK
			case "TIMEOUT":
				commandStage = LOCK_DATA_STAGE_TIMEOUT
			case "EXPRIED":
				commandStage = LOCK_DATA_STAGE_EXPRIED
			}
			executeCommand, _, cerr := self.ConvertTextLockAndUnLockCommand(textProtocol, args[i+2:])
			if cerr != nil {
				_ = textProtocol.FreeLockCommand(lockCommand)
				return nil, nil, cerr
			}
			lockCommand.Data = NewLockCommandDataExecuteData(executeCommand, commandStage)
			lockCommand.Flag |= LOCK_FLAG_CONTAINS_DATA
		}
	}

	if !hasLockId {
		if commandName == "LOCK" {
			lockCommand.LockId = lockCommand.RequestId
		} else {
			lockCommand.LockId = textProtocol.GetLockId()
		}
	}
	return lockCommand, self.WriteTextLockAndUnLockCommandResult, nil
}

func (self *TextCommandConverter) WriteTextLockAndUnLockCommandResult(textProtocol ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	bufIndex := 0
	tr := ""
	wbuf := textProtocol.GetParser().GetWriteBuf()
	if lockCommandResult.Flag&UNLOCK_FLAG_CONTAINS_DATA != 0 {
		bufIndex += copy(wbuf[bufIndex:], []byte("*14\r\n"))
	} else {
		bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))
	}

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommandResult.LockId)))
	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Count+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lrcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Rcount+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n"))

	err := stream.WriteBytes(wbuf[:bufIndex])
	if err == nil {
		if lockCommandResult.Flag&UNLOCK_FLAG_CONTAINS_DATA != 0 {
			data := lockCommandResult.Data.GetStringValue()
			err = stream.WriteBytes([]byte(fmt.Sprintf("$4\r\nDATA\r\n$%d\r\n%s\r\n", len(data), data)))
		}
	}
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextKeyOperateValueCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	handler, err := self.FindHandler(strings.ToUpper(args[0]))
	if err != nil {
		return nil, nil, err
	}
	return handler(textProtocol, args)
}

func (self *TextCommandConverter) ConvertTextDelCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_UNLOCK
	lockCommand.Flag = UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	return lockCommand, self.WriteTextDelCommandResult, nil
}

func (self *TextCommandConverter) WriteTextDelCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != 0 {
		err := stream.WriteBytes([]byte(":0\r\n"))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte(":1\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextSetCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 3 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
	lockCommand.Data = NewLockCommandDataSetStringWithProperty(args[2], []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})
	if len(args) > 3 {
		err := self.ConvertArgs2Flag(lockCommand, args[3:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.Flag&LOCK_FLAG_UPDATE_WHEN_LOCKED == 0 && lockCommand.Timeout == 0 && lockCommand.TimeoutFlag == 0 {
		lockCommand.Timeout = 15
	}
	if lockCommand.Expried == 0 && lockCommand.ExpriedFlag == 0 {
		lockCommand.Expried = 0x7fff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME | EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		} else {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		}
	}
	return lockCommand, self.WriteTextSetCommandResult, nil
}

func (self *TextCommandConverter) WriteTextSetCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
		if lockCommandResult.Result == RESULT_TIMEOUT {
			err := stream.WriteBytes([]byte("$-1\r\n"))
			lockCommandResult.Data = nil
			return err
		}
		err := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte("+OK\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextAppendCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 3 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
	lockCommand.Data = NewLockCommandDataAppendStringWithProperty(args[2], []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})

	if len(args) > 3 {
		err := self.ConvertArgs2Flag(lockCommand, args[3:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.Expried == 0 && lockCommand.ExpriedFlag == 0 {
		lockCommand.Expried = 0xffff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME | EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		} else {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		}
	}
	return lockCommand, func(textProtocol ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
		if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
			if lockCommandResult.Result == RESULT_TIMEOUT {
				werr := stream.WriteBytes([]byte("$-1\r\n"))
				lockCommandResult.Data = nil
				return werr
			}
			werr := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
			lockCommandResult.Data = nil
			return werr
		}
		werr := stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", lockCommandResult.Data.GetValueSize()+len(args[2]))))
		lockCommandResult.Data = nil
		return werr
	}, nil
}

func (self *TextCommandConverter) ConvertTextGetSetCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	lockCommand, _, err := self.ConvertTextSetCommand(textProtocol, args)
	if err != nil {
		return nil, nil, err
	}
	return lockCommand, self.WriteTextGetCommandResult, nil
}

func (self *TextCommandConverter) ConvertTextSetNXCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 3 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = GenLockId()
	lockCommand.Flag = LOCK_FLAG_CONTAINS_DATA
	lockCommand.Data = NewLockCommandDataSetStringWithProperty(args[2], []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})
	lockCommand.Timeout = 15
	if len(args) > 3 {
		err := self.ConvertArgs2Flag(lockCommand, args[3:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.Expried == 0 && lockCommand.ExpriedFlag == 0 {
		lockCommand.Expried = 0xffff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME | EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		} else {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		}
	}
	return lockCommand, self.WriteTextSetNXCommandResult, nil
}

func (self *TextCommandConverter) WriteTextSetNXCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
		if lockCommandResult.Result == RESULT_TIMEOUT {
			err := stream.WriteBytes([]byte(":0\r\n"))
			lockCommandResult.Data = nil
			return err
		}
		err := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte(":1\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextSetEXCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 3 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
	lockCommand.Data = NewLockCommandDataSetStringWithProperty(args[3], []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})
	expried, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, nil, errors.New("Command Parse EX Value Error")
	}
	lockCommand.Expried = uint16(expried & 0xffff)
	if strings.ToUpper(args[0]) == "PSETEX" {
		if expried > 65535000 {
			if (expried/1000)%60 == 0 {
				lockCommand.Expried = uint16(expried / 60000)
			} else {
				lockCommand.Expried = uint16(expried/60000) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else if expried <= 3000 {
			lockCommand.Expried = uint16(expried)
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MILLISECOND_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	} else {
		if expried > 65535 {
			if expried%60 == 0 {
				lockCommand.Expried = uint16(expried / 60)
			} else {
				lockCommand.Expried = uint16(expried/60) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	}
	if len(args) > 4 {
		err = self.ConvertArgs2Flag(lockCommand, args[4:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
		lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	}
	return lockCommand, self.WriteTextSetCommandResult, nil
}

func (self *TextCommandConverter) ConvertTextGetCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_SHOW_WHEN_LOCKED
	return lockCommand, self.WriteTextGetCommandResult, nil
}

func (self *TextCommandConverter) WriteTextGetCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if (lockCommandResult.Result != RESULT_UNOWN_ERROR && lockCommandResult.Result != RESULT_LOCKED_ERROR) || lockCommandResult.Data == nil {
		err := stream.WriteBytes([]byte("$-1\r\n"))
		lockCommandResult.Data = nil
		return err
	}

	lockResultCommandData := lockCommandResult.Data
	lockCommandResult.Data = nil
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_NUMBER != 0 {
		return stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", lockResultCommandData.GetIncrValue())))
	}
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_ARRAY != 0 {
		values := lockResultCommandData.GetArrayValue()
		if values == nil || len(values) == 0 {
			return stream.WriteBytes([]byte("$-1\r\n"))
		}
		value := string(values[0])
		return stream.WriteBytes([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
	}
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_KV != 0 {
		values := lockResultCommandData.GetKVValue()
		if values == nil || len(values) == 0 {
			return stream.WriteBytes([]byte("$-1\r\n"))
		}
		var value string
		for _, v := range values {
			value = string(v)
			break
		}
		return stream.WriteBytes([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
	}

	if len(lockResultCommandData.Data) <= 6 {
		return stream.WriteBytes([]byte("$-1\r\n"))
	}
	value := lockResultCommandData.GetStringValue()
	return stream.WriteBytes([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
}

func (self *TextCommandConverter) ConvertTextIncrCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA

	incrValue, index := int64(1), 3
	if len(args) > 2 {
		v, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, errors.New("Command Parse Increment Value Error")
		}
		incrValue = v
		index++
	}
	lockCommand.Data = NewLockCommandDataIncrDataWithProperty(incrValue, []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})
	if len(args) > index {
		err := self.ConvertArgs2Flag(lockCommand, args[index:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.Expried == 0 && lockCommand.ExpriedFlag == 0 {
		lockCommand.Expried = 0xffff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME | EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		} else {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		}
	}
	return lockCommand, func(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
		if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
			werr := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
			lockCommandResult.Data = nil
			return werr
		}
		if lockCommandResult.Data == nil {
			werr := stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", incrValue)))
			lockCommandResult.Data = nil
			return werr
		}
		werr := stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", lockCommandResult.Data.GetIncrValue()+incrValue)))
		lockCommandResult.Data = nil
		return werr
	}, nil
}

func (self *TextCommandConverter) ConvertTextDecrCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA

	incrValue, index := int64(-1), 3
	if len(args) > 2 {
		v, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, errors.New("Command Parse Increment Value Error")
		}
		incrValue = -v
		index++
	}
	lockCommand.Data = NewLockCommandDataIncrDataWithProperty(incrValue, []*LockCommandDataProperty{NewLockCommandDataProperty(LOCK_DATA_PROPERTY_CODE_KEY, []byte(args[1]))})
	if len(args) > index {
		err := self.ConvertArgs2Flag(lockCommand, args[index:])
		if err != nil {
			_ = textProtocol.FreeLockCommand(lockCommand)
			return nil, nil, err
		}
	}
	if lockCommand.Expried == 0 && lockCommand.ExpriedFlag == 0 {
		lockCommand.Expried = 0xffff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME | EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	} else {
		if lockCommand.ExpriedFlag&EXPRIED_FLAG_UNLIMITED_AOF_TIME != 0 {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		} else {
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
		}
	}
	return lockCommand, func(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
		if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
			werr := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
			lockCommandResult.Data = nil
			return werr
		}
		if lockCommandResult.Data == nil {
			werr := stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", incrValue)))
			lockCommandResult.Data = nil
			return werr
		}
		werr := stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", lockCommandResult.Data.GetIncrValue()+incrValue)))
		lockCommandResult.Data = nil
		return werr
	}, nil
}

func (self *TextCommandConverter) ConvertTextStrlenCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_SHOW_WHEN_LOCKED
	return lockCommand, self.WriteTextStrlenCommandResult, nil
}

func (self *TextCommandConverter) WriteTextStrlenCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != RESULT_UNOWN_ERROR || lockCommandResult.Data == nil {
		err := stream.WriteBytes([]byte(":0\r\n"))
		lockCommandResult.Data = nil
		return err
	}

	lockResultCommandData := lockCommandResult.Data
	lockCommandResult.Data = nil
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_NUMBER != 0 {
		return stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", len(fmt.Sprintf("%d", lockResultCommandData.GetIncrValue())))))
	}
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_ARRAY != 0 {
		values := lockResultCommandData.GetArrayValue()
		if values == nil || len(values) == 0 {
			return stream.WriteBytes([]byte(":0\r\n"))
		}
		value := string(values[0])
		return stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", len(value))))
	}
	if lockResultCommandData.DataFlag&LOCK_DATA_FLAG_VALUE_TYPE_KV != 0 {
		values := lockResultCommandData.GetKVValue()
		if values == nil || len(values) == 0 {
			return stream.WriteBytes([]byte(":0\r\n"))
		}
		var value string
		for _, v := range values {
			value = string(v)
			break
		}
		return stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", len(value))))
	}

	if len(lockResultCommandData.Data) <= 6 {
		return stream.WriteBytes([]byte(":0\r\n"))
	}
	value := lockResultCommandData.GetStringValue()
	return stream.WriteBytes([]byte(fmt.Sprintf(":%d\r\n", len(value))))
}

func (self *TextCommandConverter) ConvertTextExistsCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_SHOW_WHEN_LOCKED
	return lockCommand, self.WriteTextExistsCommandResult, nil
}

func (self *TextCommandConverter) WriteTextExistsCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != RESULT_UNOWN_ERROR || lockCommandResult.Data == nil {
		err := stream.WriteBytes([]byte(":0\r\n"))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte(":1\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextExpireCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 3 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_UPDATE_WHEN_LOCKED
	expried, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, nil, errors.New("Command Parse EX Value Error")
	}
	switch strings.ToUpper(args[0]) {
	case "EXPIRE":
		if expried > 65535 {
			if expried%60 == 0 {
				lockCommand.Expried = uint16(expried / 60)
			} else {
				lockCommand.Expried = uint16(expried/60) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	case "EXPIREAT":
		expried = expried - time.Now().Unix()
		if expried > 65535 {
			if expried%60 == 0 {
				lockCommand.Expried = uint16(expried / 60)
			} else {
				lockCommand.Expried = uint16(expried/60) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	case "PEXPIRE":
		if expried > 65535000 {
			if (expried/1000)%60 == 0 {
				lockCommand.Expried = uint16(expried / 60000)
			} else {
				lockCommand.Expried = uint16(expried/60000) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else if expried <= 3000 {
			lockCommand.Expried = uint16(expried)
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MILLISECOND_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	case "PEXPIREAT":
		expried = expried - time.Now().UnixMilli()
		if expried > 65535000 {
			if (expried/1000)%60 == 0 {
				lockCommand.Expried = uint16(expried / 60000)
			} else {
				lockCommand.Expried = uint16(expried/60000) + 1
			}
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MINUTE_TIME
		} else if expried <= 3000 {
			lockCommand.Expried = uint16(expried)
			lockCommand.ExpriedFlag |= EXPRIED_FLAG_MILLISECOND_TIME
		} else {
			lockCommand.Expried = uint16(expried)
		}
	case "PERSIST":
		lockCommand.Expried = 0x7fff
		lockCommand.ExpriedFlag = EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME
	}
	lockCommand.ExpriedFlag |= EXPRIED_FLAG_ZEOR_AOF_TIME | EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT
	return lockCommand, self.WriteTextExpireCommandResult, nil
}

func (self *TextCommandConverter) WriteTextExpireCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != 0 && lockCommandResult.Result != RESULT_LOCKED_ERROR {
		if lockCommandResult.Result == RESULT_TIMEOUT {
			err := stream.WriteBytes([]byte(":0\r\n"))
			lockCommandResult.Data = nil
			return err
		}
		err := stream.WriteBytes([]byte(fmt.Sprintf("-ERR %d\r\n", lockCommandResult.Result)))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte(":1\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextTypeCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_SHOW_WHEN_LOCKED
	return lockCommand, self.WriteTextTypeCommandResult, nil
}

func (self *TextCommandConverter) WriteTextTypeCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != RESULT_UNOWN_ERROR || lockCommandResult.Data == nil {
		err := stream.WriteBytes([]byte("+none\r\n"))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte("+string\r\n"))
	lockCommandResult.Data = nil
	return err
}

func (self *TextCommandConverter) ConvertTextDumpCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 {
		return nil, nil, errors.New("Command Parse Args Count Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	lockCommand.CommandType = COMMAND_LOCK
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)
	lockCommand.LockId = lockCommand.LockKey
	lockCommand.Flag = LOCK_FLAG_SHOW_WHEN_LOCKED
	return lockCommand, self.WriteTextDumpCommandResult, nil
}

func (self *TextCommandConverter) WriteTextDumpCommandResult(_ ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error {
	if lockCommandResult.Result != RESULT_UNOWN_ERROR || lockCommandResult.Data == nil {
		err := stream.WriteBytes([]byte("$-1\r\n"))
		lockCommandResult.Data = nil
		return err
	}
	err := stream.WriteBytes([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(lockCommandResult.Data.Data), lockCommandResult.Data.Data)))
	lockCommandResult.Data = nil
	return err
}
