package protocol

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type TextRequestCommand struct {
	Parser *TextParser
	Args   []string
}

func (self *TextRequestCommand) Decode(buf []byte) error {
	if len(buf) > len(self.Parser.rbuf) || self.Parser.bufIndex != self.Parser.bufLen {
		return errors.New("buf error")
	}

	copy(self.Parser.rbuf, buf)
	self.Parser.bufLen = len(buf)
	self.Parser.bufIndex = 0
	return self.Parser.ParseRequest()
}

func (self *TextRequestCommand) Encode(buf []byte) error {
	buildBuf := self.Parser.BuildRequest(self.Args)
	if len(buildBuf) > len(buf) || self.Parser.bufIndex == self.Parser.bufLen {
		return errors.New("buf error")
	}

	copy(buf, buildBuf)
	return nil
}

func (self *TextRequestCommand) GetArgs() []string {
	return self.Args
}

type TextResponseCommand struct {
	Parser    *TextParser
	ErrorType string
	Message   string
	Results   []string
}

func (self *TextResponseCommand) Decode(buf []byte) error {
	if len(buf) > len(self.Parser.rbuf) || self.Parser.bufIndex != self.Parser.bufLen {
		return errors.New("buf error")
	}

	copy(self.Parser.rbuf, buf)
	self.Parser.bufLen = len(buf)
	self.Parser.bufIndex = 0
	return self.Parser.ParseResponse()
}

func (self *TextResponseCommand) Encode(buf []byte) error {
	isSuccess := false
	message := self.Message
	if self.ErrorType == "" {
		isSuccess = true
	} else {
		message = fmt.Sprintf("%s %s", self.ErrorType, self.Message)
	}

	buildBuf := self.Parser.BuildResponse(isSuccess, message, self.Results)
	if len(buildBuf) > len(buf) || self.Parser.bufIndex == self.Parser.bufLen {
		return errors.New("buf error")
	}

	copy(buf, buildBuf)
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
	rbuf      []byte
	wbuf      []byte
	args      []string
	carg      []byte
	argsType  int
	bufIndex  int
	bufLen    int
	stage     int
	argsCount int
	cargIndex int
	cargLen   int
}

const MAX_CARG_LEN = 128

func NewTextParser(rbuf []byte, wbuf []byte) *TextParser {
	return &TextParser{rbuf, wbuf, make([]string, 0), make([]byte, MAX_CARG_LEN),
		0, 0, 0, 0, 0, 0, 0}
}

func (self *TextParser) GetReadBuf() []byte {
	return self.rbuf
}

func (self *TextParser) CopyToReadBuf(buf []byte) {
	copy(self.rbuf[self.bufIndex:], buf)
	self.bufLen += len(buf)
}

func (self *TextParser) GetRemainingReadBuffer() []byte {
	buf := self.rbuf[self.bufIndex:self.bufLen]
	self.bufLen = 0
	self.bufIndex = 0
	return buf
}

func (self *TextParser) GetWriteBuf() []byte {
	return self.wbuf
}

func (self *TextParser) CopyToWriteBuf(buf []byte) {
	copy(self.wbuf[self.bufIndex:], buf)
	self.bufLen += len(buf)
}

func (self *TextParser) GetArgsType() int {
	return self.argsType
}

func (self *TextParser) GetArgs() []string {
	return self.args
}

func (self *TextParser) GetArgsCount() int {
	return self.argsCount
}

func (self *TextParser) GetCommandType() string {
	if self.argsType == 0 && len(self.args) >= 1 {
		return strings.ToUpper(self.args[0])
	}
	return ""
}

func (self *TextParser) GetErrorType() string {
	if self.argsType == 2 && len(self.args) >= 1 {
		return strings.ToUpper(self.args[0])
	}
	return ""
}

func (self *TextParser) GetErrorMessage() string {
	if self.argsType == 2 && len(self.args) >= 2 {
		return self.args[1]
	}
	return ""
}

func (self *TextParser) GetRequestCommand() (*TextRequestCommand, error) {
	if self.argsType != 0 {
		return nil, errors.New("not request")
	}

	command := TextRequestCommand{self, make([]string, 0)}
	command.Args = append(command.Args, self.args...)
	return &command, nil
}

func (self *TextParser) GetResponseCommand() (*TextResponseCommand, error) {
	if self.argsType == 0 {
		return nil, errors.New("not response")
	}

	command := TextResponseCommand{self, "", "", make([]string, 0)}
	if self.argsType == 2 {
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
	return self.bufLen == self.bufIndex
}

func (self *TextParser) IsParseFinish() bool {
	return self.stage == 0
}

func (self *TextParser) BufferUpdate(n int) {
	self.bufLen = n
	self.bufIndex = 0
}

func (self *TextParser) Reset() {
	self.args = self.args[:0]
	self.argsCount = 0
}

func (self *TextParser) ParseRequest() error {
	for self.bufIndex < self.bufLen {
		switch self.stage {
		case 0:
			if self.rbuf[self.bufIndex] != '*' {
				return errors.New("Command first byte must by *")
			}
			self.bufIndex++
			self.argsType = 0
			self.stage = 1
		case 1:
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Command parse args count error")
					}

					argsCount, err := strconv.Atoi(string(self.carg[:self.cargIndex]))
					if err != nil {
						return err
					}
					self.argsCount = argsCount
					self.cargIndex = 0
					self.bufIndex++
					self.stage = 2
					break
				} else if self.rbuf[self.bufIndex] != '\r' {
					if self.cargIndex >= MAX_CARG_LEN {
						return errors.New("Command parse args count error")
					}
					self.carg[self.cargIndex] = self.rbuf[self.bufIndex]
					self.cargIndex++
				}
			}

			if self.stage == 1 {
				return nil
			}
		case 2:
			if self.rbuf[self.bufIndex] != '$' {
				return errors.New("Command first byte must by $")
			}
			self.bufIndex++
			self.stage = 3
		case 3:
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Command parse arg len error")
					}

					cargLen, err := strconv.Atoi(string(self.carg[:self.cargIndex]))
					if err != nil {
						return errors.New("Command parse args count error")
					}
					self.cargLen = cargLen
					self.cargIndex = 0
					self.bufIndex++
					self.stage = 4
					break
				} else if self.rbuf[self.bufIndex] != '\r' {
					if self.cargIndex >= MAX_CARG_LEN {
						return errors.New("Command parse args count error")
					}
					self.carg[self.cargIndex] = self.rbuf[self.bufIndex]
					self.cargIndex++
				}
			}

			if self.stage == 3 {
				return nil
			}
		case 4:
			cargLen := self.cargLen - self.cargIndex
			if cargLen > 0 {
				if self.bufLen-self.bufIndex < cargLen {
					if self.cargIndex == 0 {
						self.args = append(self.args, string(self.rbuf[self.bufIndex:self.bufLen]))
					} else {
						self.args[len(self.args)-1] += string(self.rbuf[self.bufIndex:self.bufLen])
					}
					self.cargIndex += self.bufLen - self.bufIndex
					self.bufIndex = self.bufLen
					return nil
				}

				if self.cargIndex == 0 {
					self.args = append(self.args, string(self.rbuf[self.bufIndex:self.bufIndex+cargLen]))
				} else {
					self.args[len(self.args)-1] += string(self.rbuf[self.bufIndex : self.bufIndex+cargLen])
				}
				self.cargIndex = cargLen
				self.bufIndex += cargLen
			}

			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Command parse arg error")
					}

					if self.cargLen == 0 {
						self.args = append(self.args, "")
					}
					self.cargIndex = 0
					self.cargLen = 0
					self.bufIndex++
					if len(self.args) < self.argsCount {
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
	for self.bufIndex < self.bufLen {
		switch self.stage {
		case 0:
			switch self.rbuf[self.bufIndex] {
			case '+':
				self.args = append(self.args, "")
				self.argsCount = 0
				self.argsType = 1
				self.bufIndex++
				self.stage = 5
			case '-':
				self.args = append(self.args, "")
				self.args = append(self.args, "")
				self.argsCount = 0
				self.argsType = 2
				self.bufIndex++
				self.stage = 6
			case '$':
				self.argsType = 3
				self.bufIndex++
				self.stage = 3
			case '*':
				self.argsType = 4
				self.bufIndex++
				self.stage = 1
			default:
				return errors.New("Response first byte must by -+$*")
			}
		case 1:
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Response parse args count error")
					}

					argsCount, err := strconv.Atoi(string(self.carg[:self.cargIndex]))
					if err != nil {
						return err
					}
					self.argsCount = argsCount
					self.cargIndex = 0
					self.bufIndex++
					self.stage = 2
					break
				} else if self.rbuf[self.bufIndex] != '\r' {
					if self.cargIndex >= MAX_CARG_LEN {
						return errors.New("Response parse args count error")
					}
					self.carg[self.cargIndex] = self.rbuf[self.bufIndex]
					self.cargIndex++
				}
			}

			if self.stage == 1 {
				return nil
			}
		case 2:
			if self.rbuf[self.bufIndex] != '$' {
				return errors.New("Response first byte must by $")
			}
			self.bufIndex++
			self.stage = 3
		case 3:
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Response parse arg len error")
					}

					cargLen, err := strconv.Atoi(string(self.carg[:self.cargIndex]))
					if err != nil {
						return errors.New("Response parse args count error")
					}
					self.cargLen = cargLen
					self.cargIndex = 0
					self.bufIndex++
					self.stage = 4
					break
				} else if self.rbuf[self.bufIndex] != '\r' {
					if self.cargIndex >= MAX_CARG_LEN {
						return errors.New("Response parse args count error")
					}
					self.carg[self.cargIndex] = self.rbuf[self.bufIndex]
					self.cargIndex++
				}
			}

			if self.stage == 3 {
				return nil
			}
		case 4:
			cargLen := self.cargLen - self.cargIndex
			if cargLen > 0 {
				if self.bufLen-self.bufIndex < cargLen {
					if self.cargIndex == 0 {
						self.args = append(self.args, string(self.rbuf[self.bufIndex:self.bufLen]))
					} else {
						self.args[len(self.args)-1] += string(self.rbuf[self.bufIndex:self.bufLen])
					}
					self.cargIndex += self.bufLen - self.bufIndex
					self.bufIndex = self.bufLen
					return nil
				}

				if self.cargIndex == 0 {
					self.args = append(self.args, string(self.rbuf[self.bufIndex:self.bufIndex+cargLen]))
				} else {
					self.args[len(self.args)-1] += string(self.rbuf[self.bufIndex : self.bufIndex+cargLen])
				}
				self.cargIndex = cargLen
				self.bufIndex += cargLen
			}

			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Response parse arg error")
					}

					if self.cargLen == 0 {
						self.args = append(self.args, "")
					}
					self.cargIndex = 0
					self.cargLen = 0
					self.bufIndex++
					if len(self.args) < self.argsCount {
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
			startBufIndex, endBufIndex := self.bufIndex, self.bufIndex
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == '\n' {
					if self.argsType == 2 {
						self.args[1] += string(self.rbuf[startBufIndex : endBufIndex+1])
					} else {
						self.args[0] += string(self.rbuf[startBufIndex : endBufIndex+1])
					}
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Response parse msg error")
					}

					self.bufIndex++
					self.stage = 0
					return nil
				} else if self.rbuf[self.bufIndex] != '\r' {
					endBufIndex = self.bufIndex
				}
			}

			if self.argsType == 2 {
				self.args[1] += string(self.rbuf[startBufIndex : endBufIndex+1])
			} else {
				self.args[0] += string(self.rbuf[startBufIndex : endBufIndex+1])
			}
			return nil
		case 6:
			startBufIndex, endBufIndex := self.bufIndex, self.bufIndex
			for ; self.bufIndex < self.bufLen; self.bufIndex++ {
				if self.rbuf[self.bufIndex] == ' ' {
					self.args[0] += string(self.rbuf[startBufIndex : endBufIndex+1])
					self.bufIndex++
					self.stage = 5
					break
				} else if self.rbuf[self.bufIndex] == '\n' {
					self.args[0] += string(self.rbuf[startBufIndex : endBufIndex+1])
					if self.bufIndex > 0 && self.rbuf[self.bufIndex-1] != '\r' {
						return errors.New("Response parse msg error")
					}

					self.bufIndex++
					self.stage = 0
					return nil
				} else if self.rbuf[self.bufIndex] != '\r' {
					endBufIndex = self.bufIndex
				}
			}

			if self.stage == 6 {
				self.args[0] += string(self.rbuf[startBufIndex : endBufIndex+1])
				return nil
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

func (self *TextParser) BuildResponse(isSuccess bool, message string, results []string) []byte {
	if !isSuccess {
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
