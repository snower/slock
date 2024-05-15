package protocol

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ITextProtocol interface {
	GetDBId() uint8
	GetLockId() [16]byte
	GetLockCommand() *LockCommand
	FreeLockCommand(lockCommand *LockCommand) error
	GetParser() *TextParser
}

type WriteTextCommandResultFunc func(textProtocol ITextProtocol, stream ISteam, lockCommandResult *LockResultCommand) error

type TextCommandConverter struct {
}

func NewTextCommandConverter() *TextCommandConverter {
	return &TextCommandConverter{}
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

func (self *TextCommandConverter) GetAndResetLockCommand(textProtocol ITextProtocol) *LockCommand {
	lockCommand := textProtocol.GetLockCommand()
	lockCommand.Magic = MAGIC
	lockCommand.Version = VERSION
	lockCommand.RequestId = GenRequestId()
	lockCommand.DbId = textProtocol.GetDBId()
	lockCommand.Flag = 0
	lockCommand.Timeout = 3
	lockCommand.TimeoutFlag = 0
	lockCommand.Expried = 60
	lockCommand.ExpriedFlag = 0
	lockCommand.Count = 0
	lockCommand.Rcount = 0
	return lockCommand
}

func (self *TextCommandConverter) ConvertTextLockAndUnLockCommand(textProtocol ITextProtocol, args []string) (*LockCommand, WriteTextCommandResultFunc, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, nil, errors.New("Command Parse Len Error")
	}

	lockCommand := self.GetAndResetLockCommand(textProtocol)
	commandName := strings.ToUpper(args[0])
	if commandName == "UNLOCK" {
		lockCommand.CommandType = COMMAND_UNLOCK
	} else {
		lockCommand.CommandType = COMMAND_LOCK
	}
	self.ConvertArgId2LockId(args[1], &lockCommand.LockKey)

	hasLockId := false
	for i := 2; i < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "LOCK_ID":
			self.ConvertArgId2LockId(args[i+1], &lockCommand.LockId)
			hasLockId = true
		case "FLAG":
			flag, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, nil, errors.New("Command Parse FLAG Error")
			}
			lockCommand.Flag = uint8(flag)
		case "TIMEOUT":
			timeout, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, nil, errors.New("Command Parse TIMEOUT Error")
			}
			lockCommand.Timeout = uint16(timeout & 0xffff)
			lockCommand.TimeoutFlag = uint16(timeout >> 16 & 0xffff)
		case "EXPRIED":
			expried, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, nil, errors.New("Command Parse EXPRIED Error")
			}
			lockCommand.Expried = uint16(expried & 0xffff)
			lockCommand.ExpriedFlag = uint16(expried >> 16 & 0xffff)
		case "COUNT":
			count, err := strconv.Atoi(args[i+1])
			if err != nil {
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
