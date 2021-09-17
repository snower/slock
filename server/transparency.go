package server

import (
	"errors"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"io"
	"net"
	"sync"
	"time"
)

type TransparencyBinaryClientProtocol struct {
	slock                       *SLock
	stream 						*client.Stream
	protocol 					*client.BinaryClientProtocol
	closed						bool
}

func (self *TransparencyBinaryClientProtocol) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", addr, 5 * time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	client_protocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.protocol = client_protocol
	self.closed = false
	return nil
}

func (self *TransparencyBinaryClientProtocol) IsOpened() bool {
	return self.stream != nil
}

func (self *TransparencyBinaryClientProtocol) Close() error {
	if self.protocol != nil {
		self.protocol.Close()
	}

	self.stream = nil
	self.protocol = nil
	self.closed = true
	return nil
}

func (self *TransparencyBinaryClientProtocol) Lock(command *protocol.LockCommand) error {
	return self.protocol.Write(command)
}

func (self *TransparencyBinaryClientProtocol) Unlock(command *protocol.LockCommand) error {
	return self.protocol.Write(command)
}

func (self *TransparencyBinaryClientProtocol) Process(server_protocol *TransparencyBinaryServerProtocol) {
	for ; !self.closed; {
		command, err := self.protocol.Read()
		if err != nil {
			if err == io.EOF {
				if self.slock.state == STATE_LEADER {
					self.Close()
				} else {
					server_protocol.Close()
				}
				return
			}
			continue
		}

		switch command.(type) {
		case *protocol.LockResultCommand:
			err = server_protocol.Write(command.(*protocol.LockResultCommand))
			if err != nil {
				if self.slock.state == STATE_LEADER {
					self.Close()
				} else {
					server_protocol.Close()
				}
				return
			}
		case *protocol.InitResultCommand:
			err = server_protocol.Write(command.(*protocol.LockResultCommand))
			if err != nil {
				if self.slock.state == STATE_LEADER {
					self.Close()
				} else {
					server_protocol.Close()
				}
				return
			}
		}
	}
}

func (self *TransparencyBinaryClientProtocol) ProcessWaiter(server_protocol *TransparencyTextServerProtocol) {
	for ; !self.closed; {
		command, err := self.protocol.Read()
		if err != nil {
			if err == io.EOF {
				if self.slock.state == STATE_LEADER {
					self.Close()
				} else {
					server_protocol.Close()
				}
				return
			}
			continue
		}

		switch command.(type) {
		case *protocol.LockResultCommand:
			lock_command := command.(*protocol.LockResultCommand)
			if lock_command.RequestId == server_protocol.lock_request_id {
				server_protocol.lock_waiter <- lock_command
			}
		}
	}
}

type TransparencyBinaryServerProtocol struct {
	slock                       *SLock
	glock                       *sync.Mutex
	stream 						*Stream
	server_protocol				*BinaryServerProtocol
	client_protocol 			*TransparencyBinaryClientProtocol
	closed                      bool
}

func NewTransparencyBinaryServerProtocol(slock *SLock, stream *Stream, server_protocol *BinaryServerProtocol) *TransparencyBinaryServerProtocol {
	client_protocol := &TransparencyBinaryClientProtocol{slock, nil, nil, false}
	return &TransparencyBinaryServerProtocol{slock, &sync.Mutex{}, stream, server_protocol, client_protocol, false}
}

func (self *TransparencyBinaryServerProtocol) Init(client_id [16]byte) error {
	return self.server_protocol.Init(client_id)
}

func (self *TransparencyBinaryServerProtocol) Close() error {
	self.glock.Lock()
	defer self.glock.Unlock()

	if self.closed {
		return nil
	}

	self.closed = true
	self.client_protocol.Close()
	return self.server_protocol.Close()
}

func (self *TransparencyBinaryServerProtocol) Lock() {
	self.server_protocol.Lock()
}

func (self *TransparencyBinaryServerProtocol) Unlock() {
	self.server_protocol.Unlock()
}

func (self *TransparencyBinaryServerProtocol) Read() (protocol.CommandDecode, error) {
	return self.server_protocol.Read()
}

func (self *TransparencyBinaryServerProtocol) Write(result protocol.CommandEncode) error {
	return self.server_protocol.Write(result)
}

func (self *TransparencyBinaryServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.Read()
}

func (self *TransparencyBinaryServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	return self.Write(result)
}

func (self *TransparencyBinaryServerProtocol) Process() error {
	buf := self.server_protocol.rbuf
	for ; !self.closed; {
		n, err := self.stream.conn.Read(buf)
		if err != nil {
			return err
		}

		if n < 64 {
			for ; n < 64; {
				nn, nerr := self.stream.conn.Read(buf[n:])
				if nerr != nil {
					return nerr
				}
				n += nn
			}
		}

		err = self.ProcessParse(buf)
		if err != nil {
			return err
		}
	}
	return io.EOF
}

func (self *TransparencyBinaryServerProtocol) ProcessParse(buf []byte) error {
	if self.slock.state != STATE_FOLLOWER {
		if self.slock.state == STATE_SYNC {
			waiter := make(chan bool, 1)
			self.slock.replication_manager.WaitInitSynced(waiter)
			succed := <- waiter
			if succed {
				return io.EOF
			}
			return nil
		}
		return self.server_protocol.ProcessParse(buf)
	}

	if !self.client_protocol.IsOpened() {
		leader_addrss := self.slock.replication_manager.leader_address
		if leader_addrss == "" {
			return self.server_protocol.ProcessParse(buf)
		}

		err := self.client_protocol.Open(leader_addrss)
		if err != nil {
			return err
		}
		go self.client_protocol.Process(self)
	}

	if len(buf) < 64 {
		return errors.New("command data too short")
	}

	mv := uint16(buf[0]) | uint16(buf[1])<<8
	if mv != 0x0156 {
		if mv&0xff != uint16(protocol.MAGIC) {
			command := protocol.NewCommand(buf)
			self.server_protocol.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_MAGIC))
			return errors.New("Unknown Magic")
		}

		if (mv>>8)&0xff != uint16(protocol.VERSION) {
			command := protocol.NewCommand(buf)
			self.server_protocol.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_VERSION))
			return errors.New("Unknown Version")
		}
	}

	self.server_protocol.total_command_count++
	command_type := uint8(buf[2])
	switch command_type {
	case protocol.COMMAND_LOCK:
		lock_command := self.server_protocol.free_commands.PopRight()
		if lock_command == nil {
			lock_command = self.server_protocol.GetLockCommandLocked()
		}

		lock_command.CommandType = command_type

		lock_command.RequestId[0], lock_command.RequestId[1], lock_command.RequestId[2], lock_command.RequestId[3], lock_command.RequestId[4], lock_command.RequestId[5], lock_command.RequestId[6], lock_command.RequestId[7],
			lock_command.RequestId[8], lock_command.RequestId[9], lock_command.RequestId[10], lock_command.RequestId[11], lock_command.RequestId[12], lock_command.RequestId[13], lock_command.RequestId[14], lock_command.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

		lock_command.LockId[0], lock_command.LockId[1], lock_command.LockId[2], lock_command.LockId[3], lock_command.LockId[4], lock_command.LockId[5], lock_command.LockId[6], lock_command.LockId[7],
			lock_command.LockId[8], lock_command.LockId[9], lock_command.LockId[10], lock_command.LockId[11], lock_command.LockId[12], lock_command.LockId[13], lock_command.LockId[14], lock_command.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lock_command.LockKey[0], lock_command.LockKey[1], lock_command.LockKey[2], lock_command.LockKey[3], lock_command.LockKey[4], lock_command.LockKey[5], lock_command.LockKey[6], lock_command.LockKey[7],
			lock_command.LockKey[8], lock_command.LockKey[9], lock_command.LockKey[10], lock_command.LockKey[11], lock_command.LockKey[12], lock_command.LockKey[13], lock_command.LockKey[14], lock_command.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if self.slock.state != STATE_LEADER {
			return self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
		}

		if lock_command.DbId == 0xff {
			return self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
		}

		err := self.client_protocol.Lock(lock_command)
		if err != nil {
			return err
		}
		return nil
	case protocol.COMMAND_UNLOCK:
		lock_command := self.server_protocol.free_commands.PopRight()
		if lock_command == nil {
			lock_command = self.server_protocol.GetLockCommandLocked()
		}

		lock_command.CommandType = command_type

		lock_command.RequestId[0], lock_command.RequestId[1], lock_command.RequestId[2], lock_command.RequestId[3], lock_command.RequestId[4], lock_command.RequestId[5], lock_command.RequestId[6], lock_command.RequestId[7],
			lock_command.RequestId[8], lock_command.RequestId[9], lock_command.RequestId[10], lock_command.RequestId[11], lock_command.RequestId[12], lock_command.RequestId[13], lock_command.RequestId[14], lock_command.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

		lock_command.LockId[0], lock_command.LockId[1], lock_command.LockId[2], lock_command.LockId[3], lock_command.LockId[4], lock_command.LockId[5], lock_command.LockId[6], lock_command.LockId[7],
			lock_command.LockId[8], lock_command.LockId[9], lock_command.LockId[10], lock_command.LockId[11], lock_command.LockId[12], lock_command.LockId[13], lock_command.LockId[14], lock_command.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lock_command.LockKey[0], lock_command.LockKey[1], lock_command.LockKey[2], lock_command.LockKey[3], lock_command.LockKey[4], lock_command.LockKey[5], lock_command.LockKey[6], lock_command.LockKey[7],
			lock_command.LockKey[8], lock_command.LockKey[9], lock_command.LockKey[10], lock_command.LockKey[11], lock_command.LockKey[12], lock_command.LockKey[13], lock_command.LockKey[14], lock_command.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if self.slock.state != STATE_LEADER {
			return self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
		}

		if lock_command.DbId == 0xff {
			return self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
		}

		err := self.client_protocol.Unlock(lock_command)
		if err != nil {
			return err
		}
		return nil
	default:
		var command protocol.ICommand
		switch command_type {
		case protocol.COMMAND_INIT:
			command = &protocol.InitCommand{}
		case protocol.COMMAND_STATE:
			command = &protocol.StateCommand{}
		case protocol.COMMAND_ADMIN:
			command = &protocol.AdminCommand{}
		case protocol.COMMAND_PING:
			command = &protocol.PingCommand{}
		case protocol.COMMAND_QUIT:
			command = &protocol.QuitCommand{}
		default:
			command = &protocol.Command{}
		}
		err := command.Decode(buf)
		if err != nil {
			return err
		}
		err = self.ProcessCommad(command)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *TransparencyBinaryServerProtocol) ProcessBuild(command protocol.ICommand) error {
	return self.server_protocol.ProcessBuild(command)
}

func (self *TransparencyBinaryServerProtocol) ProcessCommad(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_INIT:
		init_command := command.(*protocol.InitCommand)
		if self.Init(init_command.ClientId) != nil {
			return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_ERROR, 0))
		}
		self.slock.glock.Lock()
		self.slock.streams[init_command.ClientId] = self
		self.slock.glock.Unlock()
		return self.client_protocol.protocol.Write(init_command)
	}
	return self.server_protocol.ProcessCommad(command)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
	return self.server_protocol.ProcessLockCommand(lock_command)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.server_protocol.ProcessLockResultCommand(command, result, lcount, lrcount)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.server_protocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
}

func (self *TransparencyBinaryServerProtocol) GetStream() *Stream {
	return self.stream
}


func (self *TransparencyBinaryServerProtocol) RemoteAddr() net.Addr {
	if self.stream == nil {
		return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
	}
	return self.stream.RemoteAddr()
}

func (self *TransparencyBinaryServerProtocol) InitLockCommand() {
	self.server_protocol.InitLockCommand()
}

func (self *TransparencyBinaryServerProtocol) UnInitLockCommand() {
	self.server_protocol.UnInitLockCommand()
}

func (self *TransparencyBinaryServerProtocol) GetLockCommand() *protocol.LockCommand {
	return self.server_protocol.GetLockCommand()
}

func (self *TransparencyBinaryServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
	return self.server_protocol.GetLockCommandLocked()
}

func (self *TransparencyBinaryServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.server_protocol.FreeLockCommand(command)
}

func (self *TransparencyBinaryServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.server_protocol.FreeLockCommandLocked(command)
}

type TransparencyTextServerProtocol struct {
	slock                       *SLock
	glock                       *sync.Mutex
	stream                      *Stream
	server_protocol				*TextServerProtocol
	client_protocol 			*TransparencyBinaryClientProtocol
	lock_waiter                 chan *protocol.LockResultCommand
	lock_request_id             [16]byte
	lock_id                     [16]byte
	closed                      bool
}

func NewTransparencyTextServerProtocol(slock *SLock, stream *Stream, server_protocol *TextServerProtocol) *TransparencyTextServerProtocol {
	client_protocol := &TransparencyBinaryClientProtocol{slock, nil, nil, false}
	transparency_protocol := &TransparencyTextServerProtocol{slock, &sync.Mutex{}, stream, server_protocol, client_protocol,
		make(chan *protocol.LockResultCommand, 4),[16]byte{}, [16]byte{}, false}
	server_protocol.handlers["LOCK"] = transparency_protocol.CommandHandlerLock
	server_protocol.handlers["UNLOCK"] = transparency_protocol.CommandHandlerUnlock
	return transparency_protocol
}

func (self *TransparencyTextServerProtocol) Init(client_id [16]byte) error{
	return nil
}

func (self *TransparencyTextServerProtocol) Lock() {
	self.server_protocol.Lock()
}

func (self *TransparencyTextServerProtocol) Unlock() {
	self.server_protocol.Unlock()
}

func (self *TransparencyTextServerProtocol) Close() error {
	self.glock.Lock()
	defer self.glock.Unlock()

	if self.closed {
		return nil
	}

	self.closed = true
	return self.server_protocol.Close()
}

func (self *TransparencyTextServerProtocol) GetParser() *protocol.TextParser {
	return self.server_protocol.GetParser()
}

func (self *TransparencyTextServerProtocol) Read() (protocol.CommandDecode, error) {
	return self.server_protocol.Read()
}

func (self *TransparencyTextServerProtocol) Write(result protocol.CommandEncode) error {
	return self.server_protocol.Write(result)
}

func (self *TransparencyTextServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.server_protocol.ReadCommand()
}

func (self *TransparencyTextServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	return self.server_protocol.WriteCommand(result)
}

func (self *TransparencyTextServerProtocol) Process() error {
	rbuf := self.server_protocol.parser.GetReadBuf()
	for ; !self.closed; {
		if self.server_protocol.parser.IsBufferEnd() {
			n, err := self.stream.Read(rbuf)
			if err != nil {
				return err
			}

			self.server_protocol.parser.BufferUpdate(n)
		}

		err := self.server_protocol.parser.ParseRequest()
		if err != nil {
			return err
		}

		if self.server_protocol.parser.IsParseFinish() {
			if self.slock.state == STATE_SYNC {
				waiter := make(chan bool, 1)
				self.slock.replication_manager.WaitInitSynced(waiter)
				succed := <- waiter
				if succed {
					return io.EOF
				}
			} else if self.slock.state == STATE_FOLLOWER {
				if !self.client_protocol.IsOpened() {
					leader_addrss := self.slock.replication_manager.leader_address
					if leader_addrss == "" {
						return errors.New("unknown leader address")
					}

					err := self.client_protocol.Open(leader_addrss)
					if err != nil {
						return err
					}
					go self.client_protocol.ProcessWaiter(self)
				}
			}

			self.server_protocol.total_command_count++
			command_name := self.server_protocol.parser.GetCommandType()
			if command_handler, ok := self.server_protocol.handlers[command_name]; ok {
				err := command_handler(self.server_protocol, self.server_protocol.parser.GetArgs())
				if err != nil {
					return err
				}
			} else {
				err := self.CommandHandlerUnknownCommand(self.server_protocol, self.server_protocol.parser.GetArgs())
				if err != nil {
					return err
				}
			}

			self.server_protocol.parser.Reset()
		}
	}
	return nil
}

func (self *TransparencyTextServerProtocol) ProcessParse(buf []byte) error {
	if self.slock.state == STATE_SYNC {
		waiter := make(chan bool, 1)
		self.slock.replication_manager.WaitInitSynced(waiter)
		succed := <- waiter
		if succed {
			return io.EOF
		}
	} else if self.slock.state == STATE_FOLLOWER {
		if !self.client_protocol.IsOpened() {
			leader_addrss := self.slock.replication_manager.leader_address
			if leader_addrss == "" {
				return errors.New("unknown leader address")
			}

			err := self.client_protocol.Open(leader_addrss)
			if err != nil {
				return err
			}
			go self.client_protocol.ProcessWaiter(self)
		}
	}

	return self.server_protocol.ProcessParse(buf)
}

func (self *TransparencyTextServerProtocol) ProcessBuild(command protocol.ICommand) error {
	return self.server_protocol.ProcessBuild(command)
}

func (self *TransparencyTextServerProtocol) ProcessCommad(command protocol.ICommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.ProcessCommad(command)
}

func (self *TransparencyTextServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.ProcessLockCommand(lock_command)
}

func (self *TransparencyTextServerProtocol) ProcessLockResultCommand(lock_command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.ProcessLockResultCommand(lock_command, result, lcount, lrcount)
}

func (self *TransparencyTextServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.server_protocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
}

func (self *TransparencyTextServerProtocol) GetStream() *Stream {
	return self.stream
}

func (self *TransparencyTextServerProtocol) RemoteAddr() net.Addr {
	if self.stream == nil {
		return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
	}
	return self.stream.RemoteAddr()
}

func (self *TransparencyTextServerProtocol) InitLockCommand() {
	self.InitLockCommand()
}

func (self *TransparencyTextServerProtocol) UnInitLockCommand() {
	self.UnInitLockCommand()
}

func (self *TransparencyTextServerProtocol) GetLockCommand() *protocol.LockCommand {
	return self.GetLockCommand()
}

func (self *TransparencyTextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.FreeLockCommand(command)
}

func (self *TransparencyTextServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.FreeLockCommandLocked(command)
}

func (self *TransparencyTextServerProtocol) CommandHandlerUnknownCommand(server_protocol *TextServerProtocol, args []string) error {
	return self.server_protocol.CommandHandlerUnknownCommand(server_protocol, args)
}

func (self *TransparencyTextServerProtocol) CommandHandlerLock(server_protocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.server_protocol.CommandHandlerLock(server_protocol, args)
	}

	lock_command, err := self.server_protocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR " + err.Error(), nil))
	}

	if self.slock.state != STATE_FOLLOWER {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR State Error", nil))
	}

	if lock_command.DbId == 0xff {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	self.lock_request_id = lock_command.RequestId
	err = self.client_protocol.Lock(lock_command)
	if err != nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Lock Error", nil))
	}
	lock_command_result := <- self.lock_waiter
	if lock_command_result.Result == 0 {
		self.lock_id = lock_command.LockId
	}

	buf_index := 0
	tr := ""

	wbuf := self.server_protocol.parser.GetWriteBuf()
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
	self.server_protocol.free_command_result = lock_command_result
	return self.stream.WriteBytes(wbuf[:buf_index])
}

func (self *TransparencyTextServerProtocol) CommandHandlerUnlock(server_protocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.server_protocol.CommandHandlerUnlock(server_protocol, args)
	}

	lock_command, err := self.server_protocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR " + err.Error(), nil))
	}

	if self.slock.state != STATE_FOLLOWER {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR State Error", nil))
	}

	if lock_command.DbId == 0xff {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	db := self.slock.dbs[lock_command.DbId]
	if db == nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}
	self.lock_request_id = lock_command.RequestId
	err = self.client_protocol.Unlock(lock_command)
	if err != nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR UnLock Error", nil))
	}
	lock_command_result := <- self.lock_waiter
	if lock_command_result.Result == 0 {
		self.lock_id[0], self.lock_id[1], self.lock_id[2], self.lock_id[3], self.lock_id[4], self.lock_id[5], self.lock_id[6], self.lock_id[7],
			self.lock_id[8], self.lock_id[9], self.lock_id[10], self.lock_id[11], self.lock_id[12], self.lock_id[13], self.lock_id[14], self.lock_id[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
	}

	buf_index := 0
	tr := ""

	wbuf := self.server_protocol.parser.GetWriteBuf()
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

	self.server_protocol.free_command_result = lock_command_result
	return self.stream.WriteBytes(wbuf[:buf_index])
}
