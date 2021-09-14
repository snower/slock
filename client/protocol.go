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
    client_protocol := &BinaryClientProtocol{stream, make([]byte, 64)}
    return client_protocol
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
    default:
        return nil, errors.New("unknown command")
    }
}

func (self *BinaryClientProtocol) Write(result protocol.CommandEncode) error {
    wbuf := make([]byte, 64)
    err := result.Encode(wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(wbuf)
}

func (self *BinaryClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
    return self.Read()
}

func (self *BinaryClientProtocol) WriteCommand(result protocol.CommandEncode) error {
    return self.Write(result)
}

func (self *BinaryClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}

type TextClientProtocolParser struct {
    buf         []byte
    wbuf        []byte
    args        []string
    carg        []byte
    arg_type    int
    buf_index   int
    buf_len     int
    stage       int
    args_count  int
    carg_index  int
    carg_len    int
}

func (self *TextClientProtocolParser) Parse() error {
    for ; self.buf_index < self.buf_len; {
        switch self.stage {
        case 0:
            switch self.buf[self.buf_index] {
            case '+':
                self.args = append(self.args, "")
                self.args_count = 0
                self.arg_type = 1
                self.buf_index++
                self.stage = 5
            case '-':
                self.args = append(self.args, "")
                self.args = append(self.args, "")
                self.args_count = 0
                self.arg_type = 2
                self.buf_index++
                self.stage = 6
            case '$':
                self.arg_type = 3
                self.buf_index++
                self.stage = 3
            case '*':
                self.arg_type = 4
                self.buf_index++
                self.stage = 1
            default:
                return errors.New("Response first byte must by -+$*")
            }
        case 1:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Response parse args count error")
                    }

                    args_count, err := strconv.Atoi(string(self.carg[:self.carg_index]))
                    if err != nil {
                        return err
                    }
                    self.args_count = args_count
                    self.carg_index = 0
                    self.buf_index++
                    self.stage = 2
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    if self.carg_index >= 64 {
                        return errors.New("Response parse args count error")
                    }
                    self.carg[self.carg_index] = self.buf[self.buf_index]
                    self.carg_index++
                }
            }

            if self.stage == 1 {
                return nil
            }
        case 2:
            if self.buf[self.buf_index] != '$' {
                return errors.New("Response first byte must by $")
            }
            self.buf_index++
            self.stage = 3
        case 3:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Response parse arg len error")
                    }

                    carg_len, err := strconv.Atoi(string(self.carg[:self.carg_index]))
                    if err != nil {
                        return errors.New("Response parse args count error")
                    }
                    self.carg_len = carg_len
                    self.carg_index = 0
                    self.buf_index++
                    self.stage = 4
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    if self.carg_index >= 64 {
                        return errors.New("Response parse args count error")
                    }
                    self.carg[self.carg_index] = self.buf[self.buf_index]
                    self.carg_index++
                }
            }

            if self.stage == 3 {
                return nil
            }
        case 4:
            carg_len := self.carg_len - self.carg_index
            if carg_len > 0 {
                if self.buf_len - self.buf_index < carg_len {
                    if self.carg_index == 0 {
                        self.args = append(self.args, string(self.buf[self.buf_index: self.buf_len]))
                    } else {
                        self.args[len(self.args) - 1] += string(self.buf[self.buf_index: self.buf_len])
                    }
                    self.carg_index += self.buf_len - self.buf_index
                    self.buf_index = self.buf_len
                    return nil
                }

                if self.carg_index == 0 {
                    self.args = append(self.args, string(self.buf[self.buf_index: self.buf_index + carg_len]))
                } else {
                    self.args[len(self.args) - 1] += string(self.buf[self.buf_index: self.buf_index + carg_len])
                }
                self.carg_index = carg_len
                self.buf_index += carg_len
            }

            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Response parse arg error")
                    }

                    self.carg_index = 0
                    self.carg_len = 0
                    self.buf_index++
                    if len(self.args) < self.args_count {
                        self.stage = 2
                    } else {
                        self.stage = 0
                        return nil
                    }
                    break
                }
            }

            if self.stage == 4 {
                return nil
            }
        case 5:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Response parse msg error")
                    }

                    self.buf_index++
                    self.stage = 0
                    return nil
                } else if self.buf[self.buf_index] != '\r' {
                    if self.arg_type == 1 {
                        self.args[0] += string(self.buf[self.buf_index])
                    } else {
                        self.args[1] += string(self.buf[self.buf_index])
                    }
                }
            }
        case 6:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == ' ' {
                    self.buf_index++
                    self.stage = 5
                    return nil
                } else if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Response parse msg error")
                    }

                    self.buf_index++
                    self.stage = 0
                    return nil
                } else if self.buf[self.buf_index] != '\r' {
                    self.args[0] += string(self.buf[self.buf_index])
                }
            }
        }
    }
    return nil
}

func (self *TextClientProtocolParser) Build(args []string) []byte {
    buf := make([]byte, 0)
    buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(args)))...)
    for _, arg := range args {
        buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))...)
    }
    return buf
}

type TextClientCommand struct {
    Parser      *TextClientProtocolParser
    CommandType int
    ErrorType   string
    Args        []string
    Message     string
}

func (self *TextClientCommand) Decode(buf []byte) error  {
    if len(buf) > len(self.Parser.buf) || self.Parser.buf_index != self.Parser.buf_len {
        return errors.New("buf error")
    }

    copy(self.Parser.buf, buf)
    self.Parser.buf_len = len(buf)
    self.Parser.buf_index = 0
    return self.Parser.Parse()
}

func (self *TextClientCommand) Encode(buf []byte) error  {
    build_buf := self.Parser.Build(self.Args)
    if len(build_buf) > len(buf)|| self.Parser.buf_index == self.Parser.buf_len {
        return errors.New("buf error")
    }

    copy(buf, build_buf)
    return nil
}

func (self *TextClientCommand) GetCommandType() int {
    return self.CommandType
}


func (self *TextClientCommand) GetErrorType() string {
    return self.ErrorType
}

func (self *TextClientCommand) GetArgs() []string {
    return self.Args
}

func (self *TextClientCommand) GetMessage() string {
    return self.Message
}


type TextClientProtocol struct {
    stream *Stream
    parser            *TextClientProtocolParser
}

func NewTextClientProtocol(stream *Stream) *TextClientProtocol {
    parser := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}
    client_protocol := &TextClientProtocol{stream, parser}
    return client_protocol
}

func (self *TextClientProtocol) Close() error {
    return self.stream.Close()
}

func (self *TextClientProtocol) GetParser() *TextClientProtocolParser {
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
    for ;; {
        if self.parser.buf_index == self.parser.buf_len {
            n, err := self.stream.Read(self.parser.buf)
            if err != nil {
                return nil, err
            }

            self.parser.buf_len = n
            self.parser.buf_index = 0
        }

        err := self.parser.Parse()
        if err != nil {
            return nil, err
        }

        if self.parser.stage == 0 {
            var command *TextClientCommand
            if self.parser.arg_type == 2 {
                command = &TextClientCommand{self.parser, self.parser.arg_type, self.parser.args[0], make([]string, 0), self.parser.args[1]}
            } else {
                command = &TextClientCommand{self.parser, self.parser.arg_type, self.parser.args[0], make([]string, len(self.parser.args)), self.parser.args[1]}
                copy(command.Args, self.parser.args)
            }
            self.parser.args = self.parser.args[:0]
            self.parser.args_count = 0
            return command, err
        }
    }
}

func (self *TextClientProtocol) Write(result protocol.CommandEncode) error {
    switch result.(type) {
    case *protocol.LockResultCommand:
        return self.WriteCommand(result)
    case *TextClientCommand:
        return self.stream.WriteBytes(self.parser.Build(result.(*TextClientCommand).Args))
    }
    return errors.New("unknown command")
}

func (self *TextClientProtocol) ReadCommand() (protocol.CommandDecode, error) {
    command, err := self.Read()
    if err != nil {
        return nil, err
    }

    text_client_command := command.(*TextClientCommand)
    if text_client_command.ErrorType != "" {
        if text_client_command.Message != "" {
            return nil, errors.New(text_client_command.ErrorType + " " + text_client_command.Message)
        }
        return nil, errors.New("unknown result")
    }

    lock_command_result, err := self.ArgsToLockComandResult(text_client_command.Args)
    return lock_command_result, err
}

func (self *TextClientProtocol) WriteCommand(result protocol.CommandEncode) error {
    lock_command_result, ok := result.(*protocol.LockResultCommand)
    if !ok {
        return errors.New("unknown result")
    }

    buf_index := 0
    tr := ""

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("*12\r\n"))

    tr = fmt.Sprintf("%d", lock_command_result.Result)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    tr = protocol.ERROR_MSG[lock_command_result.Result]
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command_result.LockId)))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Count)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Rcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n"))

    return self.stream.WriteBytes(self.parser.wbuf[:buf_index])
}

func (self *TextClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}