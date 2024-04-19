package client

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ClientProtocol interface {
	Close() error
	Read() (protocol.CommandDecode, error)
	Write(protocol.CommandEncode) error
	ReadCommand() (protocol.CommandDecode, error)
	WriteCommand(protocol.CommandEncode) error
	GetStream() *Stream
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
}

type BinaryClientProtocol struct {
	stream *Stream
	rglock *sync.Mutex
	wglock *sync.Mutex
	wbuf   []byte
}

func NewBinaryClientProtocol(stream *Stream) *BinaryClientProtocol {
	return &BinaryClientProtocol{stream, &sync.Mutex{}, &sync.Mutex{}, make([]byte, 64)}
}

func (self *BinaryClientProtocol) Close() error {
	return self.stream.Close()
}

func (self *BinaryClientProtocol) Read() (protocol.CommandDecode, error) {
	defer self.rglock.Unlock()
	self.rglock.Lock()

	buf, err := self.stream.ReadBytesSize(64)
	if err != nil {
		return nil, err
	}
	if buf == nil || len(buf) != 64 {
		return nil, errors.New("command data too short")
	}
	if uint8(buf[0]) != protocol.MAGIC {
		return nil, errors.New("unknown magic")
	}
	if uint8(buf[1]) != protocol.VERSION {
		return nil, errors.New("unknown version")
	}

	switch uint8(buf[2]) {
	case protocol.COMMAND_LOCK:
		command := protocol.LockResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		if command.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			lockData, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				return nil, derr
			}
			command.Data = protocol.NewLockResultCommandDataFromOriginBytes(lockData)
		}
		return &command, nil
	case protocol.COMMAND_UNLOCK:
		command := protocol.LockResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		if command.Flag&protocol.UNLOCK_FLAG_CONTAINS_DATA != 0 {
			lockData, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				return nil, derr
			}
			command.Data = protocol.NewLockResultCommandDataFromOriginBytes(lockData)
		}
		return &command, nil
	case protocol.COMMAND_STATE:
		command := protocol.StateResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_INIT:
		command := protocol.InitResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_ADMIN:
		command := protocol.AdminResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_PING:
		command := protocol.PingResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_QUIT:
		command := protocol.QuitResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_CALL:
		command := protocol.CallResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		command.Data = make([]byte, command.ContentLen)
		if command.ContentLen > 0 {
			_, derr := self.stream.ReadBytes(command.Data)
			if derr != nil {
				return nil, derr
			}
		}
		return &command, nil
	case protocol.COMMAND_WILL_LOCK:
		command := protocol.LockResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		if command.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			lockData, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				return nil, derr
			}
			command.Data = protocol.NewLockResultCommandDataFromOriginBytes(lockData)
		}
		return &command, nil
	case protocol.COMMAND_WILL_UNLOCK:
		command := protocol.LockResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		if command.Flag&protocol.UNLOCK_FLAG_CONTAINS_DATA != 0 {
			lockData, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				return nil, derr
			}
			command.Data = protocol.NewLockResultCommandDataFromOriginBytes(lockData)
		}
		return &command, nil
	case protocol.COMMAND_LEADER:
		command := protocol.LeaderResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_SUBSCRIBE:
		command := protocol.SubscribeResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_PUBLISH:
		command := protocol.LockResultCommand{}
		err = command.Decode(buf)
		if err != nil {
			return nil, err
		}
		return &command, nil
	default:
		return nil, errors.New("unknown command")
	}
}

func (self *BinaryClientProtocol) Write(command protocol.CommandEncode) error {
	self.wglock.Lock()
	wbuf := self.wbuf
	err := command.Encode(wbuf)
	if err != nil {
		self.wglock.Unlock()
		return err
	}
	err = self.stream.WriteBytes(wbuf)
	if err != nil {
		self.wglock.Unlock()
		return err
	}

	switch command.(type) {
	case *protocol.LockCommand:
		lockCommand := command.(*protocol.LockCommand)
		if lockCommand.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			err = self.stream.WriteBytes(lockCommand.Data.Data)
			if err != nil {
				self.wglock.Unlock()
				return err
			}
		}
	case *protocol.CallCommand:
		callCommand := command.(*protocol.CallCommand)
		if callCommand.ContentLen > 0 {
			err = self.stream.WriteBytes(callCommand.Data)
			if err != nil {
				self.wglock.Unlock()
				return err
			}
		}
	}
	self.wglock.Unlock()
	return err
}

func (self *BinaryClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.Read()
}

func (self *BinaryClientProtocol) WriteCommand(command protocol.CommandEncode) error {
	return self.Write(command)
}

func (self *BinaryClientProtocol) GetStream() *Stream {
	return self.stream
}

func (self *BinaryClientProtocol) RemoteAddr() net.Addr {
	return self.stream.RemoteAddr()
}

func (self *BinaryClientProtocol) LocalAddr() net.Addr {
	return self.stream.LocalAddr()
}

type TextClientProtocol struct {
	stream *Stream
	rglock *sync.Mutex
	wglock *sync.Mutex
	parser *protocol.TextParser
}

func NewTextClientProtocol(stream *Stream) *TextClientProtocol {
	parser := protocol.NewTextParser(make([]byte, 1024), make([]byte, 1024))
	clientProtocol := &TextClientProtocol{stream, &sync.Mutex{}, &sync.Mutex{}, parser}
	return clientProtocol
}

func (self *TextClientProtocol) Close() error {
	return self.stream.Close()
}

func (self *TextClientProtocol) GetParser() *protocol.TextParser {
	return self.parser
}

func (self *TextClientProtocol) ArgsToLockComandResultParseId(argId string, lockId *[16]byte) {
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

func (self *TextClientProtocol) ArgsToLockComandResult(args []string) (*protocol.LockResultCommand, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("Response Parse Len Error")
	}

	lockCommandResult := protocol.LockResultCommand{}
	lockCommandResult.Magic = protocol.MAGIC
	lockCommandResult.Version = protocol.VERSION
	result, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, errors.New("Response Parse Result Error")
	}
	lockCommandResult.Result = uint8(result)

	for i := 2; i < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "LOCK_ID":
			self.ArgsToLockComandResultParseId(args[i+1], &lockCommandResult.LockId)
		case "LCOUNT":
			lcount, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Response Parse LCOUNT Error")
			}
			lockCommandResult.Lcount = uint16(lcount)
		case "COUNT":
			count, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Response Parse COUNT Error")
			}
			lockCommandResult.Count = uint16(count)
		case "LRCOUNT":
			lrcount, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Response Parse LRCOUNT Error")
			}
			lockCommandResult.Lrcount = uint8(lrcount)
		case "RCOUNT":
			rcount, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Response Parse RCOUNT Error")
			}
			lockCommandResult.Rcount = uint8(rcount)
		case "DATA":
			lockCommandResult.Data = protocol.NewLockResultCommandDataFromString(args[i+1], 0, 0)
			lockCommandResult.Flag |= protocol.LOCK_FLAG_CONTAINS_DATA
		}
	}
	return &lockCommandResult, nil
}

func (self *TextClientProtocol) Read() (protocol.CommandDecode, error) {
	defer self.rglock.Unlock()
	self.rglock.Lock()

	rbuf := self.parser.GetReadBuf()
	for {
		if self.parser.IsBufferEnd() {
			n, err := self.stream.ReadFromConn(rbuf)
			if err != nil {
				return nil, err
			}

			self.parser.BufferUpdate(n)
		}

		err := self.parser.ParseResponse()
		if err != nil {
			return nil, err
		}

		if self.parser.IsParseFinish() {
			command, err := self.parser.GetResponseCommand()
			self.parser.Reset()
			return command, err
		}
	}
}

func (self *TextClientProtocol) Write(result protocol.CommandEncode) error {
	switch result.(type) {
	case *protocol.LockResultCommand:
		return self.WriteCommand(result)
	case *protocol.TextRequestCommand:
		self.wglock.Lock()
		err := self.stream.WriteBytes(self.parser.BuildRequest(result.(*protocol.TextRequestCommand).Args))
		self.wglock.Unlock()
		return err
	}
	return errors.New("unknown command")
}

func (self *TextClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
	command, err := self.Read()
	if err != nil {
		return nil, err
	}

	textClientCommand := command.(*protocol.TextResponseCommand)
	if textClientCommand.ErrorType != "" {
		if textClientCommand.Message != "" {
			return nil, errors.New(textClientCommand.ErrorType + " " + textClientCommand.Message)
		}
		return nil, errors.New("unknown result")
	}

	lockCommandResult, err := self.ArgsToLockComandResult(textClientCommand.Results)
	return lockCommandResult, err
}

func (self *TextClientProtocol) WriteCommand(result protocol.CommandEncode) error {
	defer self.wglock.Unlock()
	self.wglock.Lock()

	lockCommandResult, ok := result.(*protocol.LockResultCommand)
	if !ok {
		return errors.New("unknown result")
	}

	bufIndex := 0
	tr := ""

	wbuf := self.parser.GetWriteBuf()
	if lockCommandResult.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
		bufIndex += copy(wbuf[bufIndex:], []byte("*14\r\n"))
	} else {
		bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))
	}

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommandResult.LockId)))
	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Count)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lrcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Rcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n"))

	err := self.stream.WriteBytes(wbuf[:bufIndex])
	if err != nil {
		lockCommandResult.Data = nil
		return err
	}
	if lockCommandResult.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
		data := lockCommandResult.Data.GetStringData()
		err = self.stream.WriteBytes([]byte(fmt.Sprintf("$4\r\nDATA\r\n$%d\r\n%s\r\n", len(data), data)))
	}
	lockCommandResult.Data = nil
	return err
}

func (self *TextClientProtocol) GetStream() *Stream {
	return self.stream
}

func (self *TextClientProtocol) RemoteAddr() net.Addr {
	return self.stream.RemoteAddr()
}

func (self *TextClientProtocol) LocalAddr() net.Addr {
	return self.stream.LocalAddr()
}
