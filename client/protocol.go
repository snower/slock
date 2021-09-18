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
)

type ClientProtocol interface {
    Close() error
    Read() (protocol.CommandDecode, error)
    Write(protocol.CommandEncode) error
    ReadCommand() (protocol.CommandDecode, error)
    WriteCommand(protocol.CommandEncode) error
    RemoteAddr() net.Addr
}

type BinaryClientProtocol struct {
    stream *Stream
    rbuf []byte
}

func NewBinaryClientProtocol(stream *Stream) *BinaryClientProtocol {
    return &BinaryClientProtocol{stream, make([]byte, 64)}
}

func (self *BinaryClientProtocol) Close() error {
    return self.stream.Close()
}

func (self *BinaryClientProtocol) Read() (protocol.CommandDecode, error) {
    n, err := self.stream.ReadBytes(self.rbuf)
    if err != nil {
        return nil, err
    }

    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf[0]) != protocol.MAGIC {
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf[1]) != protocol.VERSION {
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf[2]) {
    case protocol.COMMAND_LOCK:
        command := protocol.LockResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_UNLOCK:
        command := protocol.LockResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_STATE:
        command := protocol.StateResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_INIT:
        command := protocol.InitResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_ADMIN:
        command := protocol.AdminResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_PING:
        command := protocol.PingResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_QUIT:
        command := protocol.QuitResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_CALL:
        command := protocol.CallResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        command.Data = make([]byte, command.ContentLen)
        if command.ContentLen > 0 {
            _, err := self.stream.ReadBytes(command.Data)
            if err != nil {
                return nil, err
            }
        }
        return &command, nil
    default:
        return nil, errors.New("unknown command")
    }
}

func (self *BinaryClientProtocol) Write(command protocol.CommandEncode) error {
    wbuf := make([]byte, 64)
    err := command.Encode(wbuf)
    if err != nil {
        return err
    }

    err = self.stream.WriteBytes(wbuf)
    if err != nil {
        return err
    }

    switch command.(type) {
    case *protocol.CallCommand:
        call_command := command.(*protocol.CallCommand)
        if call_command.ContentLen > 0 {
            err = self.stream.WriteBytes(call_command.Data)
        }
    }
    return err
}

func (self *BinaryClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
    return self.Read()
}

func (self *BinaryClientProtocol) WriteCommand(command protocol.CommandEncode) error {
    return self.Write(command)
}

func (self *BinaryClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}

type TextClientProtocol struct {
    stream *Stream
    parser *protocol.TextParser
}

func NewTextClientProtocol(stream *Stream) *TextClientProtocol {
    parser := protocol.NewTextParser(make([]byte, 1024), make([]byte, 1024))
    client_protocol := &TextClientProtocol{stream, parser}
    return client_protocol
}

func (self *TextClientProtocol) Close() error {
    return self.stream.Close()
}

func (self *TextClientProtocol) GetParser() *protocol.TextParser {
    return self.parser
}

func (self *TextClientProtocol) ArgsToLockComandResultParseId(arg_id string, lock_id *[16]byte) {
    arg_len := len(arg_id)
    if arg_len == 16 {
        lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
            lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
            byte(arg_id[0]), byte(arg_id[1]), byte(arg_id[2]), byte(arg_id[3]), byte(arg_id[4]), byte(arg_id[5]), byte(arg_id[6]),
            byte(arg_id[7]), byte(arg_id[8]), byte(arg_id[9]), byte(arg_id[10]), byte(arg_id[11]), byte(arg_id[12]), byte(arg_id[13]), byte(arg_id[14]), byte(arg_id[15])
    } else if arg_len > 16 {
        if arg_len == 32 {
            v, err := hex.DecodeString(arg_id)
            if err == nil {
                lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                    lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
            } else {
                v := md5.Sum([]byte(arg_id))
                lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                    lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
            }
        } else {
            v := md5.Sum([]byte(arg_id))
            lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
        }
    } else {
        arg_index := 16 - arg_len
        for i := 0; i < 16; i++ {
            if i < arg_index {
                lock_id[i] = 0
            } else {
                lock_id[i] = arg_id[i - arg_index]
            }
        }
    }
}

func (self *TextClientProtocol) ArgsToLockComandResult(args []string) (*protocol.LockResultCommand, error) {
    if len(args) < 2 || len(args) % 2 != 0 {
        return nil, errors.New("Response Parse Len Error")
    }

    lock_command_result := protocol.LockResultCommand{}
    lock_command_result.Magic = protocol.MAGIC
    lock_command_result.Version = protocol.VERSION
    result, err := strconv.Atoi(args[0])
    if err != nil {
        return nil, errors.New("Response Parse Result Error")
    }
    lock_command_result.Result = uint8(result)

    for i := 2; i < len(args); i+= 2 {
        switch strings.ToUpper(args[i]) {
        case "LOCK_ID":
            self.ArgsToLockComandResultParseId(args[i+1], &lock_command_result.LockId)
        case "LCOUNT":
            lcount, err := strconv.Atoi(args[i+1])
            if err != nil {
                return nil, errors.New("Response Parse LCOUNT Error")
            }
            lock_command_result.Lcount = uint16(lcount)
        case "COUNT":
            count, err := strconv.Atoi(args[i+1])
            if err != nil {
                return nil, errors.New("Response Parse COUNT Error")
            }
            lock_command_result.Count = uint16(count)
        case "LRCOUNT":
            lrcount, err := strconv.Atoi(args[i+1])
            if err != nil {
                return nil, errors.New("Response Parse LRCOUNT Error")
            }
            lock_command_result.Lrcount = uint8(lrcount)
        case "RCOUNT":
            rcount, err := strconv.Atoi(args[i+1])
            if err != nil {
                return nil, errors.New("Response Parse RCOUNT Error")
            }
            lock_command_result.Rcount = uint8(rcount)
        }
    }
    return &lock_command_result, nil
}

func (self *TextClientProtocol) Read() (protocol.CommandDecode, error) {
    rbuf := self.parser.GetReadBuf()
    for ;; {
        if self.parser.IsBufferEnd() {
            n, err := self.stream.Read(rbuf)
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
        return self.stream.WriteBytes(self.parser.BuildRequest(result.(*protocol.TextRequestCommand).Args))
    }
    return errors.New("unknown command")
}

func (self *TextClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
    command, err := self.Read()
    if err != nil {
        return nil, err
    }

    text_client_command := command.(*protocol.TextResponseCommand)
    if text_client_command.ErrorType != "" {
        if text_client_command.Message != "" {
            return nil, errors.New(text_client_command.ErrorType + " " + text_client_command.Message)
        }
        return nil, errors.New("unknown result")
    }

    lock_command_result, err := self.ArgsToLockComandResult(text_client_command.Results)
    return lock_command_result, err
}

func (self *TextClientProtocol) WriteCommand(result protocol.CommandEncode) error {
    lock_command_result, ok := result.(*protocol.LockResultCommand)
    if !ok {
        return errors.New("unknown result")
    }

    buf_index := 0
    tr := ""

    wbuf := self.parser.GetWriteBuf()
    buf_index += copy(wbuf[buf_index:], []byte("*12\r\n"))

    tr = fmt.Sprintf("%d", lock_command_result.Result)
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    tr = protocol.ERROR_MSG[lock_command_result.Result]
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    buf_index += copy(wbuf[buf_index:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command_result.LockId)))
    buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lcount)
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    buf_index += copy(wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Count)
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    buf_index += copy(wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Rcount)
    buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(wbuf[buf_index:], []byte(tr))

    buf_index += copy(wbuf[buf_index:], []byte("\r\n"))

    return self.stream.WriteBytes(wbuf[:buf_index])
}

func (self *TextClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}