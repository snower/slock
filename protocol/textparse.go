package protocol

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type TextRequestCommand struct {
	Parser      *TextParser
	Args        []string
}

func (self *TextRequestCommand) Decode(buf []byte) error {
	if len(buf) > len(self.Parser.rbuf) || self.Parser.buf_index != self.Parser.buf_len {
		return errors.New("buf error")
	}

	copy(self.Parser.rbuf, buf)
	self.Parser.buf_len = len(buf)
	self.Parser.buf_index = 0
	return self.Parser.ParseRequest()
}

func (self *TextRequestCommand) Encode(buf []byte) error {
	build_buf := self.Parser.BuildRequest(self.Args)
	if len(build_buf) > len(buf) || self.Parser.buf_index == self.Parser.buf_len {
		return errors.New("buf error")
	}

	copy(buf, build_buf)
	return nil
}

func (self *TextRequestCommand) GetArgs() []string {
	return self.Args
}

type TextResponseCommand struct {
	Parser      *TextParser
	ErrorType   string
	Message     string
	Results     []string
}

func (self *TextResponseCommand) Decode(buf []byte) error  {
	if len(buf) > len(self.Parser.rbuf) || self.Parser.buf_index != self.Parser.buf_len {
		return errors.New("buf error")
	}

	copy(self.Parser.rbuf, buf)
	self.Parser.buf_len = len(buf)
	self.Parser.buf_index = 0
	return self.Parser.ParseResponse()
}

func (self *TextResponseCommand) Encode(buf []byte) error  {
	is_success := false
	message := self.Message
	if self.ErrorType == "" {
		is_success = true
	} else {
		message = fmt.Sprintf("%s %s", self.ErrorType, self.Message)
	}

	build_buf := self.Parser.BuildResponse(is_success, message, self.Results)
	if len(build_buf) > len(buf)|| self.Parser.buf_index == self.Parser.buf_len {
		return errors.New("buf error")
	}

	copy(buf, build_buf)
	return nil
}

func (self *TextResponseCommand) GetErrorType() string {
	return self.ErrorType
}

func (self *TextResponseCommand) GetMessage() string {
	return self.Message
}

func (self *TextResponseCommand) GetResults() []string {
	return self.Results
}

type TextParser struct {
	rbuf        []byte
	wbuf        []byte
	args        []string
	carg        []byte
	args_type   int
	buf_index   int
	buf_len     int
	stage       int
	args_count  int
	carg_index  int
	carg_len    int
}

func NewTextParser(rbuf []byte, wbuf []byte) *TextParser {
	return &TextParser{rbuf, wbuf, make([]string, 0), make([]byte, 64),
		0, 0, 0, 0, 0, 0, 0}
}

func (self *TextParser) GetReadBuf() []byte {
	return self.rbuf
}

func (self *TextParser) CopyToReadBuf(buf []byte) {
	copy(self.rbuf[self.buf_index:], buf)
	self.buf_len += len(buf)
}

func (self *TextParser) GetWriteBuf() []byte {
	return self.wbuf
}

func (self *TextParser) CopyToWriteBuf(buf []byte) {
	copy(self.wbuf[self.buf_index:], buf)
	self.buf_len += len(buf)
}

func (self *TextParser) GetArgsType() int {
	return self.args_type
}

func (self *TextParser) GetArgs() []string {
	return self.args
}

func (self *TextParser) GetArgsCount() int {
	return self.args_count
}

func (self *TextParser) GetCommandType() string {
	if self.args_type == 0 && len(self.args) >= 1 {
		return strings.ToUpper(self.args[0])
	}
	return ""
}

func (self *TextParser) GetErrorType() string {
	if self.args_type == 2 && len(self.args) >= 1 {
		return strings.ToUpper(self.args[0])
	}
	return ""
}

func (self *TextParser) GetErrorMessage() string {
	if self.args_type == 2 && len(self.args) >= 2 {
		return self.args[1]
	}
	return ""
}

func (self *TextParser) GetRequestCommand() (*TextRequestCommand, error) {
	if self.args_type != 0 {
		return nil, errors.New("not request")
	}

	command := TextRequestCommand{self, make([]string, 0)}
	command.Args = append(command.Args, self.args...)
	return &command, nil
}

func (self *TextParser) GetResponseCommand() (*TextResponseCommand, error) {
	if self.args_type == 0 {
		return nil, errors.New("not response")
	}

	command := TextResponseCommand{self, "", "", make([]string, 0)}
	if self.args_type == 2 {
		if len(self.args) >= 1 {
			command.ErrorType = self.args[0]
		}

		if len(self.args) >= 2 {
			command.Message = self.args[1]
		}
	} else {
		command.Results = append(command.Results, self.args...)
	}
	return &command, nil
}

func (self *TextParser) IsBufferEnd() bool {
	return self.buf_len == self.buf_index
}

func (self *TextParser) IsParseFinish() bool {
	return self.stage == 0
}

func (self *TextParser) BufferUpdate(n int) {
	self.buf_len = n
	self.buf_index = 0
}

func (self *TextParser) Reset() {
	self.args = self.args[:0]
	self.args_count = 0
}

func (self *TextParser) ParseRequest() error {
	for ; self.buf_index < self.buf_len; {
		switch self.stage {
		case 0:
			if self.rbuf[self.buf_index] != '*' {
				return errors.New("Command first byte must by *")
			}
			self.buf_index++
			self.args_type = 0
			self.stage = 1
		case 1:
			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Command parse args count error")
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
				} else if self.rbuf[self.buf_index] != '\r' {
					if self.carg_index >= 64 {
						return errors.New("Command parse args count error")
					}
					self.carg[self.carg_index] = self.rbuf[self.buf_index]
					self.carg_index++
				}
			}

			if self.stage == 1 {
				return nil
			}
		case 2:
			if self.rbuf[self.buf_index] != '$' {
				return errors.New("Command first byte must by $")
			}
			self.buf_index++
			self.stage = 3
		case 3:
			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Command parse arg len error")
					}

					carg_len, err := strconv.Atoi(string(self.carg[:self.carg_index]))
					if err != nil {
						return errors.New("Command parse args count error")
					}
					self.carg_len = carg_len
					self.carg_index = 0
					self.buf_index++
					self.stage = 4
					break
				} else if self.rbuf[self.buf_index] != '\r' {
					if self.carg_index >= 64 {
						return errors.New("Command parse args count error")
					}
					self.carg[self.carg_index] = self.rbuf[self.buf_index]
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
						self.args = append(self.args, string(self.rbuf[self.buf_index: self.buf_len]))
					} else {
						self.args[len(self.args) - 1] += string(self.rbuf[self.buf_index: self.buf_len])
					}
					self.carg_index += self.buf_len - self.buf_index
					self.buf_index = self.buf_len
					return nil
				}

				if self.carg_index == 0 {
					self.args = append(self.args, string(self.rbuf[self.buf_index: self.buf_index + carg_len]))
				} else {
					self.args[len(self.args) - 1] += string(self.rbuf[self.buf_index: self.buf_index + carg_len])
				}
				self.carg_index = carg_len
				self.buf_index += carg_len
			}

			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Command parse arg error")
					}

					if self.carg_len == 0 {
						self.args = append(self.args, "")
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
		}
	}
	return nil
}

func (self *TextParser) ParseResponse() error {
	for ; self.buf_index < self.buf_len; {
		switch self.stage {
		case 0:
			switch self.rbuf[self.buf_index] {
			case '+':
				self.args = append(self.args, "")
				self.args_count = 0
				self.args_type = 1
				self.buf_index++
				self.stage = 5
			case '-':
				self.args = append(self.args, "")
				self.args = append(self.args, "")
				self.args_count = 0
				self.args_type = 2
				self.buf_index++
				self.stage = 6
			case '$':
				self.args_type = 3
				self.buf_index++
				self.stage = 3
			case '*':
				self.args_type = 4
				self.buf_index++
				self.stage = 1
			default:
				return errors.New("Response first byte must by -+$*")
			}
		case 1:
			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
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
				} else if self.rbuf[self.buf_index] != '\r' {
					if self.carg_index >= 64 {
						return errors.New("Response parse args count error")
					}
					self.carg[self.carg_index] = self.rbuf[self.buf_index]
					self.carg_index++
				}
			}

			if self.stage == 1 {
				return nil
			}
		case 2:
			if self.rbuf[self.buf_index] != '$' {
				return errors.New("Response first byte must by $")
			}
			self.buf_index++
			self.stage = 3
		case 3:
			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
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
				} else if self.rbuf[self.buf_index] != '\r' {
					if self.carg_index >= 64 {
						return errors.New("Response parse args count error")
					}
					self.carg[self.carg_index] = self.rbuf[self.buf_index]
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
						self.args = append(self.args, string(self.rbuf[self.buf_index: self.buf_len]))
					} else {
						self.args[len(self.args) - 1] += string(self.rbuf[self.buf_index: self.buf_len])
					}
					self.carg_index += self.buf_len - self.buf_index
					self.buf_index = self.buf_len
					return nil
				}

				if self.carg_index == 0 {
					self.args = append(self.args, string(self.rbuf[self.buf_index: self.buf_index + carg_len]))
				} else {
					self.args[len(self.args) - 1] += string(self.rbuf[self.buf_index: self.buf_index + carg_len])
				}
				self.carg_index = carg_len
				self.buf_index += carg_len
			}

			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Response parse arg error")
					}

					if self.carg_len == 0 {
						self.args = append(self.args, "")
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
				if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Response parse msg error")
					}

					self.buf_index++
					self.stage = 0
					return nil
				} else if self.rbuf[self.buf_index] != '\r' {
					if self.args_type == 2 {
						self.args[1] += string(self.rbuf[self.buf_index])
					} else {
						self.args[0] += string(self.rbuf[self.buf_index])
					}
				}
			}
		case 6:
			for ; self.buf_index < self.buf_len; self.buf_index++ {
				if self.rbuf[self.buf_index] == ' ' {
					self.buf_index++
					self.stage = 5
					break
				} else if self.rbuf[self.buf_index] == '\n' {
					if self.buf_index > 0 && self.rbuf[self.buf_index-1] != '\r' {
						return errors.New("Response parse msg error")
					}

					self.buf_index++
					self.stage = 0
					return nil
				} else if self.rbuf[self.buf_index] != '\r' {
					self.args[0] += string(self.rbuf[self.buf_index])
				}
			}
		}
	}
	return nil
}

func (self *TextParser) BuildRequest(args []string) []byte {
	buf := make([]byte, 0)
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(args)))...)
	for _, arg := range args {
		buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))...)
	}
	return buf
}

func (self *TextParser) BuildResponse(is_success bool, message string, results []string) []byte {
	if !is_success {
		return []byte(fmt.Sprintf("-%s\r\n", message))
	}

	if results == nil || len(results) == 0 {
		return []byte(fmt.Sprintf("+%s\r\n", message))
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