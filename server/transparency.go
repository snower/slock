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
	manager						*TransparencyManager
	glock 						*sync.Mutex
	stream 						*client.Stream
	client_protocol 			*client.BinaryClientProtocol
	server_protocol				ServerProtocol
	next_client					*TransparencyBinaryClientProtocol
	init_command				*protocol.LockResultCommand
	latest_command_type			uint8
	latest_request_id			[16]byte
	leader_address				string
	local_address				string
	closed						bool
	idle_time					time.Time
}

func NewTransparencyBinaryClientProtocol(manager *TransparencyManager) *TransparencyBinaryClientProtocol {
	return &TransparencyBinaryClientProtocol{manager, &sync.Mutex{}, nil, nil,
		nil, nil, nil, 0xff, [16]byte{}, "", "",false, time.Now()}
}

func (self *TransparencyBinaryClientProtocol) Open(leader_address string) error {
	conn, err := net.DialTimeout("tcp", leader_address, 5 * time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	client_protocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.client_protocol = client_protocol
	self.leader_address = leader_address
	self.local_address = conn.LocalAddr().String()
	if self.init_command != nil {
		err := self.Write(self.init_command)
		if err != nil {
			return err
		}
	}
	self.manager.slock.Log().Infof("Transparency client connected %s, leader %s", self.local_address, self.leader_address)
	return nil
}

func (self *TransparencyBinaryClientProtocol) RetryOpen(leader_address string) error {
	self.manager.slock.Log().Infof("Transparency client reconnect %s, leader %s", self.local_address, self.leader_address)
	if self.client_protocol != nil {
		_ = self.client_protocol.Close()
		self.client_protocol = nil
		self.stream = nil
	}

	err := self.Open(leader_address)
	if err != nil {
		return err
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) Close() error {
	if self.client_protocol != nil {
		_ = self.client_protocol.Close()
	}

	self.closed = true
	self.manager.slock.Log().Infof("Transparency client close %s, leader %s", self.local_address, self.leader_address)
	return nil
}

func (self *TransparencyBinaryClientProtocol) Write(command protocol.ICommand) error {
	if self.client_protocol == nil {
		return errors.New("client not open")
	}

	err := self.client_protocol.Write(command)
	if err != nil {
		return err
	}
	self.latest_command_type = command.GetCommandType()
	self.latest_request_id = command.GetRequestId()
	return nil
}

func (self *TransparencyBinaryClientProtocol) Process() {
	defer func() {
		_ = self.Close()
		_ = self.manager.CloseClient(self)
		self.stream = nil
		self.client_protocol = nil
		self.manager.slock.Log().Infof("Transparency client closed %s, leader %s", self.local_address, self.leader_address)
	}()

	for ; !self.closed; {
		command, err := self.client_protocol.Read()
		if err != nil {
			self.RollbackLatestCommand()
			err := self.manager.ProcessFinish(self)
			if err != nil {
				return
			}
			continue
		}

		if self.server_protocol == nil {
			continue
		}

		switch self.server_protocol.(type) {
		case *TransparencyBinaryServerProtocol:
			err := self.ProcessBinaryProcotol(command)
			if err != nil {
				return
			}
		case *TransparencyTextServerProtocol:
			err := self.ProcessTextProcotol(command)
			if err != nil {
				return
			}
		}
	}
}

func (self *TransparencyBinaryClientProtocol) ProcessBinaryProcotol(command protocol.CommandDecode) error {
	server_protocol := self.server_protocol.(*TransparencyBinaryServerProtocol)
	switch command.(type) {
	case *protocol.LockResultCommand:
		lock_result_command := command.(*protocol.LockResultCommand)
		if self.latest_request_id == lock_result_command.RequestId {
			self.latest_command_type = 0xff
		}
		return server_protocol.Write(lock_result_command)
	case *protocol.InitResultCommand:
		init_result_command := command.(*protocol.InitResultCommand)
		if self.latest_request_id == init_result_command.RequestId {
			self.latest_command_type = 0xff
		}
		init_result_command.InitType = 2
		return server_protocol.Write(init_result_command)
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) ProcessTextProcotol(command protocol.CommandDecode) error {
	server_protocol := self.server_protocol.(*TransparencyTextServerProtocol)
	switch command.(type) {
	case *protocol.LockResultCommand:
		lock_result_command := command.(*protocol.LockResultCommand)
		if self.latest_request_id == lock_result_command.RequestId {
			self.latest_command_type = 0xff
		}

		text_protocol := server_protocol.server_protocol
		if lock_result_command.RequestId == text_protocol.lock_request_id {
			text_protocol.lock_request_id[0], text_protocol.lock_request_id[1], text_protocol.lock_request_id[2], text_protocol.lock_request_id[3], text_protocol.lock_request_id[4], text_protocol.lock_request_id[5], text_protocol.lock_request_id[6], text_protocol.lock_request_id[7],
				text_protocol.lock_request_id[8], text_protocol.lock_request_id[9], text_protocol.lock_request_id[10], text_protocol.lock_request_id[11], text_protocol.lock_request_id[12], text_protocol.lock_request_id[13], text_protocol.lock_request_id[14], text_protocol.lock_request_id[15] =
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0

			server_protocol.lock_waiter <- lock_result_command
		}
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) RollbackLatestCommand()  {
	var result_command protocol.CommandDecode = nil
	if self.latest_command_type != 0xff {
		command := protocol.ResultCommand{Magic:protocol.MAGIC, Version:protocol.VERSION, CommandType:self.latest_command_type,
			RequestId:self.latest_request_id, Result:protocol.RESULT_ERROR}
		if self.latest_command_type == protocol.COMMAND_INIT {
			result_command = &protocol.LockResultCommand{ResultCommand:command}
		} else {
			result_command = &protocol.InitResultCommand{ResultCommand: command}
		}
	}

	if self.server_protocol != nil {
		switch self.server_protocol.(type) {
		case *TransparencyBinaryServerProtocol:
			if result_command != nil {
				_ = self.ProcessBinaryProcotol(result_command)
			}
			server_protocol := self.server_protocol.(*TransparencyBinaryServerProtocol)
			server_protocol.client_protocol = nil
		case *TransparencyTextServerProtocol:
			if result_command != nil {
				_ = self.ProcessTextProcotol(result_command)
			}
			server_protocol := self.server_protocol.(*TransparencyTextServerProtocol)
			server_protocol.client_protocol = nil
		}
		self.server_protocol = nil
	}
	self.latest_command_type = 0xff
}

type TransparencyBinaryServerProtocol struct {
	slock                       *SLock
	manager						*TransparencyManager
	glock                       *sync.Mutex
	stream 						*Stream
	server_protocol				*BinaryServerProtocol
	client_protocol 			*TransparencyBinaryClientProtocol
	closed                      bool
}

func NewTransparencyBinaryServerProtocol(slock *SLock, stream *Stream, server_protocol *BinaryServerProtocol) *TransparencyBinaryServerProtocol {
	return &TransparencyBinaryServerProtocol{slock, slock.replication_manager.transparency_manager, &sync.Mutex{}, stream,
		server_protocol, nil, false}
}

func (self *TransparencyBinaryServerProtocol) Init(client_id [16]byte) error {
	return self.server_protocol.Init(client_id)
}

func (self *TransparencyBinaryServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	will_commands := self.server_protocol.will_commands
	if will_commands != nil {
		self.server_protocol.will_commands = nil
		self.glock.Unlock()

		client_protocol, err := self.CheckClient()
		if err == nil && client_protocol != nil {
			for {
				command := will_commands.Pop()
				if command == nil {
					break
				}
				_ = client_protocol.Write(command)
				_ = self.server_protocol.FreeLockCommand(command)
			}
		}
		self.glock.Lock()
	}

	if self.client_protocol != nil {
		_ = self.client_protocol.Close()
	}
	err := self.server_protocol.Close()
	self.glock.Unlock()
	return err
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

func (self *TransparencyBinaryServerProtocol) CheckClient() (*TransparencyBinaryClientProtocol, error) {
	arbiter_waiter := self.manager.arbiter_waiter
	if arbiter_waiter != nil {
		<- arbiter_waiter
	}

	if self.client_protocol != nil {
		if self.client_protocol.client_protocol != nil {
			return self.client_protocol, nil
		}
		self.client_protocol = nil
	}

	if self.slock.state == STATE_SYNC {
		waiter := make(chan bool, 1)
		self.slock.replication_manager.WaitInitSynced(waiter)
		succed := <- waiter
		if !succed {
			return nil, io.EOF
		}
	}

	if self.slock.state != STATE_FOLLOWER {
		return nil, io.EOF
	}

	client_protocol, err := self.manager.OpenClient()
	if err != nil {
		return nil, err
	}
	client_protocol.server_protocol = self
	self.client_protocol = client_protocol
	return self.client_protocol, nil
}

func (self *TransparencyBinaryServerProtocol) Process() error {
	buf := self.server_protocol.rbuf
	for ; !self.closed; {
		for self.server_protocol.rlen - self.server_protocol.rindex < 64 {
			n, err := self.stream.conn.Read(buf[self.server_protocol.rlen:])
			if err != nil {
				return err
			}
			self.server_protocol.rlen += n
		}

		if self.slock.state == STATE_LEADER {
			return AGAIN
		}

		for self.server_protocol.rlen - self.server_protocol.rindex >= 64 {
			err := self.ProcessParse(buf[self.server_protocol.rindex:])
			self.server_protocol.rindex += 64
			if self.server_protocol.rindex == self.server_protocol.rlen {
				self.server_protocol.rindex, self.server_protocol.rlen = 0, 0
			}
			if err != nil {
				return err
			}
		}
	}
	return io.EOF
}

func (self *TransparencyBinaryServerProtocol) ProcessParse(buf []byte) error {
	if len(buf) < 64 {
		return errors.New("command data too short")
	}

	mv := uint16(buf[0]) | uint16(buf[1])<<8
	if mv != 0x0156 {
		if mv&0xff != uint16(protocol.MAGIC) {
			command := protocol.Command{}
			err := command.Decode(buf)
			if err != nil {
				return err
			}
			_ = self.server_protocol.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_MAGIC))
			return errors.New("Unknown Magic")
		}

		if (mv>>8)&0xff != uint16(protocol.VERSION) {
			command := protocol.Command{}
			err := command.Decode(buf)
			if err != nil {
				return err
			}
			_ = self.server_protocol.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_VERSION))
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

		if lock_command.DbId == 0xff {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}

		if self.slock.state == STATE_LEADER {
			db := self.slock.dbs[lock_command.DbId]
			if db == nil {
				db = self.slock.GetOrNewDB(lock_command.DbId)
			}
			return db.Lock(self, lock_command)
		}

		client_protocol, err := self.CheckClient()
		if err != nil || client_protocol == nil {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}

		err = client_protocol.Write(lock_command)
		if err != nil {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_ERROR, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}
		_ = self.server_protocol.FreeLockCommand(lock_command)
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

		if lock_command.DbId == 0xff {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}

		if self.slock.state == STATE_LEADER {
			db := self.slock.dbs[lock_command.DbId]
			if db == nil {
				err := self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}
			return db.UnLock(self, lock_command)
		}

		client_protocol, err := self.CheckClient()
		if err != nil || client_protocol == nil {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}

		err = client_protocol.Write(lock_command)
		if err != nil {
			err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_ERROR, 0, 0)
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return err
		}
		_ = self.server_protocol.FreeLockCommand(lock_command)
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
		case protocol.COMMAND_CALL:
			call_command := protocol.CallCommand{}
			err := call_command.Decode(buf)
			if err != nil {
				return err
			}

			call_command.Data = make([]byte, call_command.ContentLen)
			if call_command.ContentLen > 0 {
				rindex, content_len := self.server_protocol.rindex + 64, int(call_command.ContentLen)
				if self.server_protocol.rlen - rindex >= content_len {
					copy(call_command.Data, self.server_protocol.rbuf[rindex: rindex + content_len])
					self.server_protocol.rindex += content_len
					content_len = 0
				} else if self.server_protocol.rlen - rindex > 0 {
					copy(call_command.Data, self.server_protocol.rbuf[rindex: self.server_protocol.rlen])
					content_len -= self.server_protocol.rlen - rindex
					self.server_protocol.rindex += self.server_protocol.rlen - rindex
				}

				if content_len > 0 {
					_, err := self.stream.ReadBytes(call_command.Data[int(call_command.ContentLen) - content_len:])
					if err != nil {
						return err
					}
				}
			}
			err = self.ProcessCommad(&call_command)
			if err != nil {
				return err
			}
			return nil
		case protocol.COMMAND_WILL_LOCK:
			command = self.server_protocol.GetLockCommand()
		case protocol.COMMAND_WILL_UNLOCK:
			command = self.server_protocol.GetLockCommand()
		case protocol.COMMAND_LEADER:
			command = &protocol.LeaderCommand{}
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
	if self.slock.state != STATE_LEADER {
		switch command.GetCommandType() {
		case protocol.COMMAND_LOCK:
			lock_command := command.(*protocol.LockCommand)

			if lock_command.DbId == 0xff {
				err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}

			client_protocol, err := self.CheckClient()
			if err != nil || client_protocol == nil {
				err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}

			err = client_protocol.Write(lock_command)
			if err != nil {
				err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return nil

		case protocol.COMMAND_UNLOCK:
			lock_command := command.(*protocol.LockCommand)

			if lock_command.DbId == 0xff {
				err := self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.FreeLockCommand(lock_command)
				return err
			}

			client_protocol, err := self.CheckClient()
			if err != nil || client_protocol == nil {
				err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}

			err = client_protocol.Write(lock_command)
			if err != nil {
				err := self.server_protocol.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.server_protocol.FreeLockCommand(lock_command)
				return err
			}
			_ = self.server_protocol.FreeLockCommand(lock_command)
			return nil

		case protocol.COMMAND_INIT:
			init_command := command.(*protocol.InitCommand)
			err := self.Init(init_command.ClientId)
			if err != nil {
				return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_ERROR, 0))
			}

			self.slock.glock.Lock()
			self.slock.streams[init_command.ClientId] = self.server_protocol
			self.slock.glock.Unlock()

			client_protocol, err := self.CheckClient()
			if err != nil || client_protocol == nil {
				return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_STATE_ERROR, 0))
			}

			err = client_protocol.Write(init_command)
			if err != nil {
				return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_ERROR, 0))
			}
			return nil
		}
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
	manager						*TransparencyManager
	glock                       *sync.Mutex
	stream                      *Stream
	server_protocol				*TextServerProtocol
	client_protocol 			*TransparencyBinaryClientProtocol
	lock_waiter                 chan *protocol.LockResultCommand
	closed                      bool
}

func NewTransparencyTextServerProtocol(slock *SLock, stream *Stream, server_protocol *TextServerProtocol) *TransparencyTextServerProtocol {
	transparency_protocol := &TransparencyTextServerProtocol{slock, slock.replication_manager.transparency_manager, &sync.Mutex{},
		stream, server_protocol, nil, make(chan *protocol.LockResultCommand, 4), false}
	if server_protocol.handlers == nil {
		_, _ = server_protocol.FindHandler("LOCK")
	}
	if server_protocol.handlers != nil {
		server_protocol.handlers["LOCK"] = transparency_protocol.CommandHandlerLock
		server_protocol.handlers["UNLOCK"] = transparency_protocol.CommandHandlerUnlock
		server_protocol.handlers["PUSH"] = transparency_protocol.CommandHandlerPush
	}
	return transparency_protocol
}

func (self *TransparencyTextServerProtocol) FindHandler(name string) (TextServerProtocolCommandHandler, error) {
	if self.server_protocol.handlers == nil {
		handler, err := self.server_protocol.FindHandler(name)
		self.server_protocol.handlers["LOCK"] = self.CommandHandlerLock
		self.server_protocol.handlers["UNLOCK"] = self.CommandHandlerUnlock
		self.server_protocol.handlers["PUSH"] = self.CommandHandlerPush

		if name != "LOCK" && name != "UNLOCK" && name != "PUSH" {
			return handler, err
		}
	}

	if name == "LOCK" {
		return self.CommandHandlerLock, nil
	}
	if name == "UNLOCK" {
		return self.CommandHandlerUnlock, nil
	}
	if name == "PUSH" {
		return self.CommandHandlerPush, nil
	}
	return self.server_protocol.FindHandler(name)
}

func (self *TransparencyTextServerProtocol) Init(client_id [16]byte) error {
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
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	will_commands := self.server_protocol.will_commands
	if will_commands != nil {
		self.server_protocol.will_commands = nil
		self.glock.Unlock()

		client_protocol, err := self.CheckClient()
		if err == nil && client_protocol != nil {
			for {
				command := will_commands.Pop()
				if command == nil {
					break
				}
				_ = client_protocol.Write(command)
				_ = self.server_protocol.FreeLockCommand(command)
			}
		}
		self.glock.Lock()
	}

	if self.client_protocol != nil {
		_ = self.manager.ReleaseClient(self.client_protocol)
	}
	err := self.server_protocol.Close()
	self.glock.Unlock()
	return err
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

func (self *TransparencyTextServerProtocol) CheckClient() (*TransparencyBinaryClientProtocol, error) {
	arbiter_waiter := self.manager.arbiter_waiter
	if arbiter_waiter != nil {
		<- arbiter_waiter
	}

	if self.client_protocol != nil {
		if self.client_protocol.client_protocol != nil {
			return self.client_protocol, nil
		}
		self.client_protocol = nil
	}

	if self.slock.state == STATE_SYNC {
		waiter := make(chan bool, 1)
		self.slock.replication_manager.WaitInitSynced(waiter)
		succed := <- waiter
		if !succed {
			return nil, io.EOF
		}
	}

	if self.slock.state != STATE_FOLLOWER {
		return nil, io.EOF
	}

	client_protocol, err := self.manager.AcquireClient(self)
	if err != nil {
		return nil, err
	}
	self.client_protocol = client_protocol
	return self.client_protocol, nil
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
			self.server_protocol.total_command_count++
			command_name := self.server_protocol.parser.GetCommandType()
			if command_handler, err := self.FindHandler(command_name); err == nil {
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

			if self.slock.state == STATE_LEADER {
				return AGAIN
			}
		}
	}
	return nil
}

func (self *TransparencyTextServerProtocol) RunCommand() error {
	self.server_protocol.total_command_count++
	command_name := self.server_protocol.parser.GetCommandType()
	if command_handler, err := self.FindHandler(command_name); err == nil {
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
	return nil
}

func (self *TransparencyTextServerProtocol) ProcessParse(buf []byte) error {
	return self.server_protocol.ProcessParse(buf)
}

func (self *TransparencyTextServerProtocol) ProcessBuild(command protocol.ICommand) error {
	return self.server_protocol.ProcessBuild(command)
}

func (self *TransparencyTextServerProtocol) ProcessCommad(command protocol.ICommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.server_protocol.ProcessCommad(command)
}

func (self *TransparencyTextServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.server_protocol.ProcessLockCommand(lock_command)
}

func (self *TransparencyTextServerProtocol) ProcessLockResultCommand(lock_command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.server_protocol.ProcessLockResultCommand(lock_command, result, lcount, lrcount)
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
	self.server_protocol.InitLockCommand()
}

func (self *TransparencyTextServerProtocol) UnInitLockCommand() {
	self.server_protocol.UnInitLockCommand()
}

func (self *TransparencyTextServerProtocol) GetLockCommand() *protocol.LockCommand {
	return self.server_protocol.GetLockCommand()
}

func (self *TransparencyTextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.server_protocol.FreeLockCommand(command)
}

func (self *TransparencyTextServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.server_protocol.FreeLockCommandLocked(command)
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

	if lock_command.DbId == 0xff {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	if lock_command.CommandType == protocol.COMMAND_WILL_LOCK {
		if self.server_protocol.will_commands == nil {
			self.glock.Lock()
			if self.server_protocol.will_commands == nil {
				self.server_protocol.will_commands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		lock_command.CommandType = protocol.COMMAND_LOCK
		_ = self.server_protocol.will_commands.Push(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(true, "OK", nil))
	}

	client_protocol, cerr := self.CheckClient()
	if cerr != nil || client_protocol == nil {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	self.server_protocol.lock_request_id = lock_command.RequestId
	err = client_protocol.Write(lock_command)
	if err != nil {
		self.server_protocol.lock_request_id[0], self.server_protocol.lock_request_id[1], self.server_protocol.lock_request_id[2], self.server_protocol.lock_request_id[3], self.server_protocol.lock_request_id[4], self.server_protocol.lock_request_id[5], self.server_protocol.lock_request_id[6], self.server_protocol.lock_request_id[7],
			self.server_protocol.lock_request_id[8], self.server_protocol.lock_request_id[9], self.server_protocol.lock_request_id[10], self.server_protocol.lock_request_id[11], self.server_protocol.lock_request_id[12], self.server_protocol.lock_request_id[13], self.server_protocol.lock_request_id[14], self.server_protocol.lock_request_id[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		if self.client_protocol != nil {
			_ = self.manager.ReleaseClient(self.client_protocol)
		}
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Lock Error", nil))
	}

	lock_command_result := <- self.lock_waiter
	if lock_command_result.Result == 0 {
		self.server_protocol.lock_id = lock_command.LockId
	}
	if self.client_protocol != nil {
		_ = self.manager.ReleaseClient(self.client_protocol)
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
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command.LockId)))
	buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Lcount)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Count + 1)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Rcount + 1)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n"))

	_ = self.server_protocol.FreeLockCommand(lock_command)
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

	if lock_command.DbId == 0xff {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	if lock_command.CommandType == protocol.COMMAND_WILL_UNLOCK {
		if self.server_protocol.will_commands == nil {
			self.glock.Lock()
			if self.server_protocol.will_commands == nil {
				self.server_protocol.will_commands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		lock_command.CommandType = protocol.COMMAND_UNLOCK
		_ = self.server_protocol.will_commands.Push(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(true, "OK", nil))
	}

	client_protocol, cerr := self.CheckClient()
	if cerr != nil || client_protocol == nil {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	self.server_protocol.lock_request_id = lock_command.RequestId
	err = client_protocol.Write(lock_command)
	if err != nil {
		self.server_protocol.lock_request_id[0], self.server_protocol.lock_request_id[1], self.server_protocol.lock_request_id[2], self.server_protocol.lock_request_id[3], self.server_protocol.lock_request_id[4], self.server_protocol.lock_request_id[5], self.server_protocol.lock_request_id[6], self.server_protocol.lock_request_id[7],
			self.server_protocol.lock_request_id[8], self.server_protocol.lock_request_id[9], self.server_protocol.lock_request_id[10], self.server_protocol.lock_request_id[11], self.server_protocol.lock_request_id[12], self.server_protocol.lock_request_id[13], self.server_protocol.lock_request_id[14], self.server_protocol.lock_request_id[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		if self.client_protocol != nil {
			_ = self.manager.ReleaseClient(self.client_protocol)
		}
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR UnLock Error", nil))
	}

	lock_command_result := <- self.lock_waiter
	if lock_command_result.Result == 0 {
		self.server_protocol.lock_id[0], self.server_protocol.lock_id[1], self.server_protocol.lock_id[2], self.server_protocol.lock_id[3],
		self.server_protocol.lock_id[4], self.server_protocol.lock_id[5], self.server_protocol.lock_id[6], self.server_protocol.lock_id[7],
			self.server_protocol.lock_id[8], self.server_protocol.lock_id[9], self.server_protocol.lock_id[10], self.server_protocol.lock_id[11],
			self.server_protocol.lock_id[12], self.server_protocol.lock_id[13], self.server_protocol.lock_id[14], self.server_protocol.lock_id[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
	}
	if self.client_protocol != nil {
		_ = self.manager.ReleaseClient(self.client_protocol)
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
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command.LockId)))
	buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Lcount)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Count + 1)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lock_command_result.Rcount + 1)
	buf_index += copy(wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	buf_index += copy(wbuf[buf_index:], []byte(tr))

	buf_index += copy(wbuf[buf_index:], []byte("\r\n"))

	_ = self.server_protocol.FreeLockCommand(lock_command)
	self.server_protocol.free_command_result = lock_command_result
	return self.stream.WriteBytes(wbuf[:buf_index])
}


func (self *TransparencyTextServerProtocol) CommandHandlerPush(server_protocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.server_protocol.CommandHandlerPush(server_protocol, args)
	}

	lock_command, err := self.server_protocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR " + err.Error(), nil))
	}

	if lock_command.DbId == 0xff {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	client_protocol, cerr := self.CheckClient()
	if cerr != nil || client_protocol == nil {
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	err = client_protocol.Write(lock_command)
	if err != nil {
		if self.client_protocol != nil {
			_ = self.manager.ReleaseClient(self.client_protocol)
		}
		_ = self.server_protocol.FreeLockCommand(lock_command)
		return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(false, "ERR Lock Error", nil))
	}

	if self.client_protocol != nil {
		_ = self.manager.ReleaseClient(self.client_protocol)
	}
	_ = self.server_protocol.FreeLockCommand(lock_command)
	return self.stream.WriteBytes(self.server_protocol.parser.BuildResponse(true, "OK", nil))
}

type TransparencyManager struct {
	slock 				*SLock
	glock 				*sync.Mutex
	clients 			*TransparencyBinaryClientProtocol
	idle_clients 		*TransparencyBinaryClientProtocol
	leader_address		string
	closed				bool
	closed_waiter   	chan bool
	wakeup_signal		chan bool
	arbiter_waiter		chan bool
}

func NewTransparencyManager() *TransparencyManager {
	manager := &TransparencyManager{nil, &sync.Mutex{}, nil, nil, "",
		false, make(chan bool, 1), nil, nil}
	go manager.Run()
	return manager
}

func (self *TransparencyManager) Close() error {
	if self.closed {
		return nil
	}

	self.glock.Lock()
	self.closed = true
	current_client := self.clients
	for ; current_client != nil; {
		if current_client.client_protocol != nil {
			_ = current_client.client_protocol.Close()
		}
		current_client = current_client.next_client
	}

	current_client = self.idle_clients
	for ; current_client != nil; {
		if current_client.client_protocol != nil {
			_ = current_client.client_protocol.Close()
		}
		current_client = current_client.next_client
	}
	self.glock.Unlock()
	self.Wakeup()
	return nil
}

func (self *TransparencyManager) Run() {
	timeout := 120
	for ; !self.closed; {
		self.glock.Lock()
		self.wakeup_signal = make(chan bool, 1)
		self.glock.Unlock()

		select {
		case <-self.wakeup_signal:
			self.glock.Lock()
			timeout = self.CheckArbiterWaiter()
			self.glock.Unlock()
		case <-time.After(time.Duration(timeout) * time.Second):
			self.glock.Lock()
			self.wakeup_signal = nil
			timeout = self.CheckArbiterWaiter()
			self.glock.Unlock()
			self.CheckIdleTimeout()
		}
	}
	close(self.closed_waiter)
}

func (self *TransparencyManager) Wakeup() {
	self.glock.Lock()
	if self.wakeup_signal != nil {
		close(self.wakeup_signal)
		self.wakeup_signal = nil
	}
	self.glock.Unlock()
}

func (self *TransparencyManager) CheckArbiterWaiter() int {
	if self.arbiter_waiter != nil {
		close(self.arbiter_waiter)
		self.arbiter_waiter = nil
	}
	if self.leader_address == "" && self.slock.state != STATE_LEADER && !self.closed {
		self.arbiter_waiter = make(chan bool, 1)
		return 2
	}
	return 120
}

func (self *TransparencyManager) CheckIdleTimeout() {
	defer self.glock.Unlock()
	self.glock.Lock()

	now, idle_count := time.Now(), uint(0)
	current_client := self.idle_clients
	self.idle_clients = nil
	for ; current_client != nil; {
		if now.Unix() - current_client.idle_time.Unix() > 900 || idle_count > 5 {
			if current_client.client_protocol != nil {
				_ = current_client.client_protocol.Close()
			}
		} else {
			current_client.next_client = self.idle_clients
			self.idle_clients = current_client
			idle_count++
		}
		current_client = current_client.next_client
	}
}

func (self *TransparencyManager) AcquireClient(server_protocol ServerProtocol) (*TransparencyBinaryClientProtocol, error) {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.closed {
		return nil, errors.New("server closed")
	}

	if self.idle_clients == nil {
		binary_client, err := self.OpenClient()
		if err != nil {
			return binary_client, err
		}

		binary_client.next_client = self.clients
		self.clients = binary_client
		binary_client.server_protocol = server_protocol
		return binary_client, err
	}

	binary_client := self.idle_clients
	self.idle_clients = self.idle_clients.next_client
	binary_client.next_client = self.clients
	self.clients = binary_client
	binary_client.server_protocol = server_protocol
	return binary_client, nil
}

func (self *TransparencyManager) ReleaseClient(binary_client *TransparencyBinaryClientProtocol) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.clients == binary_client {
		self.clients = binary_client.next_client
	} else {
		current_client := self.clients
		for ; current_client != nil; {
			if current_client.next_client == binary_client {
				current_client.next_client = binary_client.next_client
				break
			}
			current_client = current_client.next_client
		}
	}

	if binary_client.closed || self.closed {
		if binary_client.client_protocol != nil {
			_ = binary_client.client_protocol.Close()
		}
	} else {
		binary_client.next_client = self.idle_clients
		self.idle_clients = binary_client
	}

	if binary_client.server_protocol != nil {
		switch binary_client.server_protocol.(type) {
		case *TransparencyBinaryServerProtocol:
			server_protocol := binary_client.server_protocol.(*TransparencyBinaryServerProtocol)
			server_protocol.client_protocol = nil
		case *TransparencyTextServerProtocol:
			server_protocol := binary_client.server_protocol.(*TransparencyTextServerProtocol)
			server_protocol.client_protocol = nil
		}
	}
	binary_client.server_protocol = nil
	binary_client.idle_time = time.Now()
	return nil
}

func (self *TransparencyManager) OpenClient() (*TransparencyBinaryClientProtocol, error) {
	if self.closed || self.slock.state == STATE_LEADER || self.leader_address == "" {
		return nil, errors.New("can not create new client")
	}

	binary_client := NewTransparencyBinaryClientProtocol(self)
	err := binary_client.Open(self.leader_address)
	if err != nil {
		return nil, err
	}
	binary_client.idle_time = time.Now()
	go binary_client.Process()
	return binary_client, nil
}

func (self *TransparencyManager) CloseClient(binary_client *TransparencyBinaryClientProtocol) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.clients == binary_client {
		self.clients = binary_client.next_client
	} else {
		current_client := self.clients
		for ; current_client != nil; {
			if current_client.next_client == binary_client {
				current_client.next_client = binary_client.next_client
				break
			}
			current_client = current_client.next_client
		}
	}

	if self.idle_clients == binary_client {
		self.idle_clients = binary_client.next_client
	} else {
		current_client := self.idle_clients
		for ; current_client != nil; {
			if current_client.next_client == binary_client {
				current_client.next_client = binary_client.next_client
				break
			}
			current_client = current_client.next_client
		}
	}

	if binary_client.server_protocol != nil {
		switch binary_client.server_protocol.(type) {
		case *TransparencyBinaryServerProtocol:
			server_protocol := binary_client.server_protocol.(*TransparencyBinaryServerProtocol)
			server_protocol.client_protocol = nil
		case *TransparencyTextServerProtocol:
			server_protocol := binary_client.server_protocol.(*TransparencyTextServerProtocol)
			server_protocol.client_protocol = nil
		}
	}
	binary_client.server_protocol = nil
	binary_client.idle_time = time.Now()
	return nil
}

func (self *TransparencyManager) ProcessFinish(binary_client *TransparencyBinaryClientProtocol) error {
	if self.closed || self.slock.state == STATE_LEADER || self.leader_address == "" || binary_client.closed {
		return io.EOF
	}

	err := binary_client.RetryOpen(self.leader_address)
	if err != nil {
		return err
	}
	return nil
}

func (self *TransparencyManager) ChangeLeader(address string) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	changed := false
	if self.leader_address != address {
		self.slock.Log().Infof("Transparency start change leader to %s", address)
		changed = true
	}
	self.leader_address = address

	if changed || address == "" {
		current_client := self.clients
		for ; current_client != nil; {
			if current_client.client_protocol != nil {
				_ = current_client.client_protocol.Close()
			}
			current_client = current_client.next_client
		}

		current_client = self.idle_clients
		for ; current_client != nil; {
			if current_client.client_protocol != nil {
				_ = current_client.client_protocol.Close()
			}
			current_client = current_client.next_client
		}
	}

	if changed {
		if self.wakeup_signal != nil {
			close(self.wakeup_signal)
			self.wakeup_signal = nil
		}
		self.slock.Log().Infof("Transparency finish change leader to %s", address)
	}
	return nil
}