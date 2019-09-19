package server

import (
    "errors"
    "fmt"
    "io"
    "strconv"
)

type AdminParser struct {
    buf         []byte
    buf_index   int
    buf_len     int
    stage       uint8
    args        []string
    args_count  int
    carg        []byte
    carg_len    int
}

type AdminCommandHandle func(*Stream, []string) error

type Admin struct {
    slock *SLock
    commands map[string]AdminCommandHandle
    is_stop bool
}

func NewAdmin() *Admin{
    admin := &Admin{nil, make(map[string]AdminCommandHandle, 64),false}

    admin.commands["shutdown"] = admin.CommandHandleShutdownCommand
    admin.commands["quit"] = admin.CommandHandleQuitCommand
    return admin
}

func (self *Admin) Close() {
    self.is_stop = true
}

func (self *Admin) Handle(stream *Stream) error {
    admin_parser := &AdminParser{make([]byte, 4096), 0, 0, 0,make([]string, 0), 0, make([]byte, 0), 0}

    for ; !self.is_stop; {
        if admin_parser.buf_index == admin_parser.buf_len {
            n, err := stream.Read(admin_parser.buf)
            if err != nil {
                return err
            }

            admin_parser.buf_len = n
            admin_parser.buf_index = 0
        }

        err := self.Parse(admin_parser)
        if err != nil {
            return err
        }

        if admin_parser.args_count > 0 && admin_parser.args_count == len(admin_parser.args) {
            var err error

            if command_handle, ok := self.commands[admin_parser.args[0]]; ok {
                err = command_handle(stream, admin_parser.args)
            } else {
                err = self.CommandHandleUnknownCommand(stream, admin_parser.args)
            }

            if err != nil {
                if err == io.EOF {
                    return nil
                }
                return err
            }

            admin_parser.args = admin_parser.args[:0]
            admin_parser.args_count = 0
        }
    }
    return nil
}

func (self *Admin) Parse(admin_parser *AdminParser) error {
    for ; admin_parser.buf_index < admin_parser.buf_len; {
        switch admin_parser.stage {
        case 0:
            if admin_parser.buf[admin_parser.buf_index] != '*' {
                return errors.New("Command first byte must by *")
            }
            admin_parser.buf_index++
            admin_parser.stage = 1
        case 1:
            for ; admin_parser.buf_index < admin_parser.buf_len; admin_parser.buf_index++ {
                if admin_parser.buf[admin_parser.buf_index] == '\n' {
                    if admin_parser.buf_index > 0 && admin_parser.buf[admin_parser.buf_index-1] != '\r' {
                        return errors.New("Command parse error")
                    }

                    args_count, err := strconv.Atoi(string(admin_parser.carg))
                    if err != nil {
                        return err
                    }
                    admin_parser.args_count = args_count
                    admin_parser.carg = admin_parser.carg[:0]
                    admin_parser.buf_index++
                    admin_parser.stage = 2
                    break
                } else if admin_parser.buf[admin_parser.buf_index] != '\r' {
                    admin_parser.carg = append(admin_parser.carg, admin_parser.buf[admin_parser.buf_index])
                }
            }

            if admin_parser.stage == 1 {
                return nil
            }
        case 2:
            if admin_parser.buf[admin_parser.buf_index] != '$' {
                return errors.New("Command first byte must by $")
            }
            admin_parser.buf_index++
            admin_parser.stage = 3
        case 3:
            for ; admin_parser.buf_index < admin_parser.buf_len; admin_parser.buf_index++ {
                if admin_parser.buf[admin_parser.buf_index] == '\n' {
                    if admin_parser.buf_index > 0 && admin_parser.buf[admin_parser.buf_index-1] != '\r' {
                        return errors.New("Command parse error")
                    }

                    carg_len, err := strconv.Atoi(string(admin_parser.carg))
                    if err != nil {
                        return errors.New("Command parse args count error")
                    }
                    admin_parser.carg_len = carg_len
                    admin_parser.carg = admin_parser.carg[:0]
                    admin_parser.buf_index++
                    admin_parser.stage = 4
                    break
                } else if admin_parser.buf[admin_parser.buf_index] != '\r' {
                    admin_parser.carg = append(admin_parser.carg, admin_parser.buf[admin_parser.buf_index])
                }
            }

            if admin_parser.stage == 3 {
                return nil
            }
        case 4:
            for ; admin_parser.buf_index < admin_parser.buf_len; admin_parser.buf_index++ {
                if len(admin_parser.carg) < admin_parser.carg_len {
                    admin_parser.carg = append(admin_parser.carg, admin_parser.buf[admin_parser.buf_index])
                } else {
                    if admin_parser.buf[admin_parser.buf_index] == '\n' {
                        if admin_parser.buf_index > 0 && admin_parser.buf[admin_parser.buf_index-1] != '\r' {
                            return errors.New("Command parse error")
                        }

                        admin_parser.args = append(admin_parser.args, string(admin_parser.carg))
                        admin_parser.carg = admin_parser.carg[:0]
                        admin_parser.carg_len = 0
                        admin_parser.buf_index++
                        if len(admin_parser.args) < admin_parser.args_count {
                            admin_parser.stage = 2
                        } else {
                            admin_parser.stage = 0
                            return nil
                        }
                        break
                    }
                }
            }

            if admin_parser.stage == 4 {
                return nil
            }
        }
    }
    return nil
}

func (self *Admin) BuildResult(is_success bool, err_msg string, results []string) []byte {
    if !is_success {
        return []byte(fmt.Sprintf("-ERR %s\r\n", err_msg))
    }

    if results == nil || len(results) == 0 {
        return []byte(fmt.Sprintf("+%s\r\n", err_msg))
    }

    buf := make([]byte, 0)
    if len(results) == 1 {
        buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(results[0]), results[0]))...)
        return buf
    }

    buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(results)))...)
    for _, result := range results {
        buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(result), result))...)
    }
    return buf
}


func (self *Admin) CommandHandleUnknownCommand(stream *Stream, args []string) error {
    return stream.WriteAllBytes(self.BuildResult(false, "Unknown Command", nil))
}

func (self *Admin) CommandHandleShutdownCommand(stream *Stream, args []string) error {
    err := stream.WriteAllBytes(self.BuildResult(true, "OK", nil))
    if err != nil {
        return err
    }

    go self.slock.Close()
    self.slock.Log().Infof("Admin Shutdown Server")
    return io.EOF
}

func (self *Admin) CommandHandleQuitCommand(stream *Stream, args []string) error {
    err := stream.WriteAllBytes(self.BuildResult(true, "OK", nil))
    if err != nil {
        return err
    }
    return io.EOF
}