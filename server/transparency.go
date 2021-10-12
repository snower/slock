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
	manager           *TransparencyManager
	glock             *sync.Mutex
	stream            *client.Stream
	clientProtocol    *client.BinaryClientProtocol
	serverProtocol    ServerProtocol
	nextClient        *TransparencyBinaryClientProtocol
	initCommand       *protocol.InitCommand
	initResultCommand *protocol.InitResultCommand
	latestCommandType uint8
	latestRequestId   [16]byte
	leaderAddress     string
	localAddress      string
	closed            bool
	idleTime          time.Time
}

func NewTransparencyBinaryClientProtocol(manager *TransparencyManager) *TransparencyBinaryClientProtocol {
	return &TransparencyBinaryClientProtocol{manager, &sync.Mutex{}, nil, nil,
		nil, nil, nil, nil, 0xff, [16]byte{},
		"", "", false, time.Now()}
}

func (self *TransparencyBinaryClientProtocol) Open(leaderAddress string) error {
	conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	clientProtocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.clientProtocol = clientProtocol
	self.leaderAddress = leaderAddress
	self.localAddress = conn.LocalAddr().String()
	if self.initCommand != nil {
		err := self.Write(self.initCommand)
		if err != nil {
			return err
		}
	}
	self.manager.slock.Log().Infof("Transparency client connected %s, leader %s", self.localAddress, self.leaderAddress)
	return nil
}

func (self *TransparencyBinaryClientProtocol) RetryOpen(leaderAddress string) error {
	self.manager.slock.Log().Infof("Transparency client reconnect %s, leader %s", self.localAddress, self.leaderAddress)
	if self.clientProtocol != nil {
		_ = self.clientProtocol.Close()
		self.clientProtocol = nil
		self.stream = nil
	}

	err := self.Open(leaderAddress)
	if err != nil {
		return err
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) Close() error {
	if self.clientProtocol != nil {
		_ = self.clientProtocol.Close()
	}

	self.closed = true
	self.manager.slock.Log().Infof("Transparency client close %s, leader %s", self.localAddress, self.leaderAddress)
	return nil
}

func (self *TransparencyBinaryClientProtocol) Write(command protocol.ICommand) error {
	if self.clientProtocol == nil {
		return errors.New("client not open")
	}

	err := self.clientProtocol.Write(command)
	if err != nil {
		return err
	}
	self.latestCommandType = command.GetCommandType()
	self.latestRequestId = command.GetRequestId()
	return nil
}

func (self *TransparencyBinaryClientProtocol) Process() {
	defer func() {
		_ = self.Close()
		_ = self.manager.CloseClient(self)
		self.stream = nil
		self.clientProtocol = nil
		self.manager.slock.Log().Infof("Transparency client closed %s, leader %s", self.localAddress, self.leaderAddress)
	}()

	for !self.closed {
		command, err := self.clientProtocol.Read()
		if err != nil {
			self.rollbackLatestCommand()
			err := self.manager.processFinish(self)
			if err != nil {
				return
			}
			continue
		}

		if self.serverProtocol == nil {
			continue
		}

		switch self.serverProtocol.(type) {
		case *TransparencyBinaryServerProtocol:
			err := self.processBinaryProcotol(command)
			if err != nil {
				return
			}
		case *TransparencyTextServerProtocol:
			err := self.processTextProcotol(command)
			if err != nil {
				return
			}
		}
	}
}

func (self *TransparencyBinaryClientProtocol) processBinaryProcotol(command protocol.CommandDecode) error {
	serverProtocol := self.serverProtocol.(*TransparencyBinaryServerProtocol)
	switch command.(type) {
	case *protocol.LockResultCommand:
		lockResultCommand := command.(*protocol.LockResultCommand)
		if self.latestRequestId == lockResultCommand.RequestId {
			self.latestCommandType = 0xff
		}
		return serverProtocol.Write(lockResultCommand)
	case *protocol.InitResultCommand:
		initResultCommand := command.(*protocol.InitResultCommand)
		if self.latestRequestId == initResultCommand.RequestId {
			self.latestCommandType = 0xff
		}
		self.initResultCommand = initResultCommand
		initResultCommand.InitType += 2
		return serverProtocol.Write(initResultCommand)
	case *protocol.CallResultCommand:
		callResultCommand := command.(*protocol.CallResultCommand)
		if self.latestRequestId == callResultCommand.RequestId {
			self.latestCommandType = 0xff
		}
		return serverProtocol.Write(callResultCommand)
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) processTextProcotol(command protocol.CommandDecode) error {
	serverProtocol := self.serverProtocol.(*TransparencyTextServerProtocol)
	switch command.(type) {
	case *protocol.LockResultCommand:
		lockResultCommand := command.(*protocol.LockResultCommand)
		if self.latestRequestId == lockResultCommand.RequestId {
			self.latestCommandType = 0xff
		}

		textProtocol := serverProtocol.serverProtocol
		if lockResultCommand.RequestId == textProtocol.lockRequestId {
			textProtocol.lockRequestId[0], textProtocol.lockRequestId[1], textProtocol.lockRequestId[2], textProtocol.lockRequestId[3], textProtocol.lockRequestId[4], textProtocol.lockRequestId[5], textProtocol.lockRequestId[6], textProtocol.lockRequestId[7],
				textProtocol.lockRequestId[8], textProtocol.lockRequestId[9], textProtocol.lockRequestId[10], textProtocol.lockRequestId[11], textProtocol.lockRequestId[12], textProtocol.lockRequestId[13], textProtocol.lockRequestId[14], textProtocol.lockRequestId[15] =
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0

			serverProtocol.lockWaiter <- lockResultCommand
		}
	}
	return nil
}

func (self *TransparencyBinaryClientProtocol) rollbackLatestCommand() {
	var resultCommand protocol.CommandDecode = nil
	if self.latestCommandType != 0xff {
		command := protocol.ResultCommand{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: self.latestCommandType,
			RequestId: self.latestRequestId, Result: protocol.RESULT_ERROR}
		switch self.latestCommandType {
		case protocol.COMMAND_LOCK:
			resultCommand = &protocol.LockResultCommand{ResultCommand: command}
		case protocol.COMMAND_UNLOCK:
			resultCommand = &protocol.LockResultCommand{ResultCommand: command}
		case protocol.COMMAND_INIT:
			resultCommand = &protocol.LockResultCommand{ResultCommand: command}
		case protocol.COMMAND_CALL:
			resultCommand = &protocol.CallResultCommand{ResultCommand: command}
		}
	}

	if self.serverProtocol != nil {
		switch self.serverProtocol.(type) {
		case *TransparencyBinaryServerProtocol:
			if resultCommand != nil {
				_ = self.processBinaryProcotol(resultCommand)
			}
			serverProtocol := self.serverProtocol.(*TransparencyBinaryServerProtocol)
			serverProtocol.clientProtocol = nil
		case *TransparencyTextServerProtocol:
			if resultCommand != nil {
				_ = self.processTextProcotol(resultCommand)
			}
			serverProtocol := self.serverProtocol.(*TransparencyTextServerProtocol)
			serverProtocol.clientProtocol = nil
		}
		self.serverProtocol = nil
	}
	self.latestCommandType = 0xff
}

type TransparencyBinaryServerProtocol struct {
	slock          *SLock
	manager        *TransparencyManager
	glock          *sync.Mutex
	stream         *Stream
	serverProtocol *BinaryServerProtocol
	clientProtocol *TransparencyBinaryClientProtocol
	initCommand    *protocol.InitCommand
	closed         bool
}

func NewTransparencyBinaryServerProtocol(slock *SLock, stream *Stream, serverProtocol *BinaryServerProtocol) *TransparencyBinaryServerProtocol {
	transparencyServerProtocol := &TransparencyBinaryServerProtocol{slock, slock.replicationManager.transparencyManager, &sync.Mutex{}, stream,
		serverProtocol, nil, nil, false}

	_, _ = serverProtocol.FindCallMethod("LIST_LOCK")
	serverProtocol.callMethods["LIST_LOCK"] = transparencyServerProtocol.commandHandleListLockCommand
	serverProtocol.callMethods["LIST_LOCKED"] = transparencyServerProtocol.commandHandleListLockedCommand
	serverProtocol.callMethods["LIST_WAIT"] = transparencyServerProtocol.commandHandleListWaitCommand
	return transparencyServerProtocol
}

func (self *TransparencyBinaryServerProtocol) Init(clientId [16]byte) error {
	return self.serverProtocol.Init(clientId)
}

func (self *TransparencyBinaryServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	willCommands := self.serverProtocol.willCommands
	if willCommands != nil {
		self.serverProtocol.willCommands = nil
		self.glock.Unlock()

		clientProtocol, err := self.CheckClient()
		if err == nil && clientProtocol != nil {
			for {
				command := willCommands.Pop()
				if command == nil {
					break
				}
				_ = clientProtocol.Write(command)
				_ = self.serverProtocol.FreeLockCommand(command)
			}
		}
		self.glock.Lock()
	}

	if self.clientProtocol != nil {
		_ = self.clientProtocol.Close()
	}
	err := self.serverProtocol.Close()
	self.glock.Unlock()
	return err
}

func (self *TransparencyBinaryServerProtocol) Lock() {
	self.serverProtocol.Lock()
}

func (self *TransparencyBinaryServerProtocol) Unlock() {
	self.serverProtocol.Unlock()
}

func (self *TransparencyBinaryServerProtocol) Read() (protocol.CommandDecode, error) {
	return self.serverProtocol.Read()
}

func (self *TransparencyBinaryServerProtocol) Write(result protocol.CommandEncode) error {
	return self.serverProtocol.Write(result)
}

func (self *TransparencyBinaryServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.Read()
}

func (self *TransparencyBinaryServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	return self.Write(result)
}

func (self *TransparencyBinaryServerProtocol) CheckClient() (*TransparencyBinaryClientProtocol, error) {
	arbiterWaiter := self.manager.arbiterWaiter
	if arbiterWaiter != nil {
		<-arbiterWaiter
	}

	if self.clientProtocol != nil {
		if self.clientProtocol.clientProtocol != nil {
			return self.clientProtocol, nil
		}
		self.clientProtocol = nil
	}

	if self.slock.state == STATE_SYNC {
		waiter := make(chan bool, 1)
		self.slock.replicationManager.WaitInitSynced(waiter)
		succed := <-waiter
		if !succed {
			return nil, io.EOF
		}
	}

	if self.slock.state != STATE_FOLLOWER {
		return nil, io.EOF
	}

	self.manager.glock.Lock()
	clientProtocol, err := self.manager.OpenClient(self.initCommand)
	if err != nil {
		self.manager.glock.Unlock()
		return nil, err
	}
	clientProtocol.nextClient = self.manager.clients
	self.manager.clients = clientProtocol
	clientProtocol.serverProtocol = self
	self.clientProtocol = clientProtocol
	self.manager.glock.Unlock()
	return self.clientProtocol, nil
}

func (self *TransparencyBinaryServerProtocol) Process() error {
	buf := self.serverProtocol.rbuf
	for !self.closed {
		for self.serverProtocol.rlen-self.serverProtocol.rindex < 64 {
			n, err := self.stream.conn.Read(buf[self.serverProtocol.rlen:])
			if err != nil {
				return err
			}
			self.serverProtocol.rlen += n
		}

		if self.slock.state == STATE_LEADER {
			return AGAIN
		}

		for self.serverProtocol.rlen-self.serverProtocol.rindex >= 64 {
			err := self.ProcessParse(buf[self.serverProtocol.rindex:])
			self.serverProtocol.rindex += 64
			if self.serverProtocol.rindex == self.serverProtocol.rlen {
				self.serverProtocol.rindex, self.serverProtocol.rlen = 0, 0
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
			_ = self.serverProtocol.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_MAGIC))
			return errors.New("Unknown Magic")
		}

		if (mv>>8)&0xff != uint16(protocol.VERSION) {
			command := protocol.Command{}
			err := command.Decode(buf)
			if err != nil {
				return err
			}
			_ = self.serverProtocol.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_VERSION))
			return errors.New("Unknown Version")
		}
	}

	self.serverProtocol.totalCommandCount++
	commandType := uint8(buf[2])
	switch commandType {
	case protocol.COMMAND_LOCK:
		lockCommand := self.serverProtocol.freeCommands.PopRight()
		if lockCommand == nil {
			lockCommand = self.serverProtocol.GetLockCommandLocked()
		}

		lockCommand.CommandType = commandType

		lockCommand.RequestId[0], lockCommand.RequestId[1], lockCommand.RequestId[2], lockCommand.RequestId[3], lockCommand.RequestId[4], lockCommand.RequestId[5], lockCommand.RequestId[6], lockCommand.RequestId[7],
			lockCommand.RequestId[8], lockCommand.RequestId[9], lockCommand.RequestId[10], lockCommand.RequestId[11], lockCommand.RequestId[12], lockCommand.RequestId[13], lockCommand.RequestId[14], lockCommand.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lockCommand.Flag, lockCommand.DbId = uint8(buf[19]), uint8(buf[20])

		lockCommand.LockId[0], lockCommand.LockId[1], lockCommand.LockId[2], lockCommand.LockId[3], lockCommand.LockId[4], lockCommand.LockId[5], lockCommand.LockId[6], lockCommand.LockId[7],
			lockCommand.LockId[8], lockCommand.LockId[9], lockCommand.LockId[10], lockCommand.LockId[11], lockCommand.LockId[12], lockCommand.LockId[13], lockCommand.LockId[14], lockCommand.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
			lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lockCommand.Timeout, lockCommand.TimeoutFlag, lockCommand.Expried, lockCommand.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lockCommand.Count, lockCommand.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if lockCommand.DbId == 0xff {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}

		if self.slock.state == STATE_LEADER {
			db := self.slock.dbs[lockCommand.DbId]
			if db == nil {
				db = self.slock.GetOrNewDB(lockCommand.DbId)
			}
			return db.Lock(self, lockCommand)
		}

		clientProtocol, err := self.CheckClient()
		if err != nil || clientProtocol == nil {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}

		err = clientProtocol.Write(lockCommand)
		if err != nil {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_ERROR, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return nil
	case protocol.COMMAND_UNLOCK:
		lockCommand := self.serverProtocol.freeCommands.PopRight()
		if lockCommand == nil {
			lockCommand = self.serverProtocol.GetLockCommandLocked()
		}

		lockCommand.CommandType = commandType

		lockCommand.RequestId[0], lockCommand.RequestId[1], lockCommand.RequestId[2], lockCommand.RequestId[3], lockCommand.RequestId[4], lockCommand.RequestId[5], lockCommand.RequestId[6], lockCommand.RequestId[7],
			lockCommand.RequestId[8], lockCommand.RequestId[9], lockCommand.RequestId[10], lockCommand.RequestId[11], lockCommand.RequestId[12], lockCommand.RequestId[13], lockCommand.RequestId[14], lockCommand.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lockCommand.Flag, lockCommand.DbId = uint8(buf[19]), uint8(buf[20])

		lockCommand.LockId[0], lockCommand.LockId[1], lockCommand.LockId[2], lockCommand.LockId[3], lockCommand.LockId[4], lockCommand.LockId[5], lockCommand.LockId[6], lockCommand.LockId[7],
			lockCommand.LockId[8], lockCommand.LockId[9], lockCommand.LockId[10], lockCommand.LockId[11], lockCommand.LockId[12], lockCommand.LockId[13], lockCommand.LockId[14], lockCommand.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
			lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lockCommand.Timeout, lockCommand.TimeoutFlag, lockCommand.Expried, lockCommand.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lockCommand.Count, lockCommand.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if lockCommand.DbId == 0xff {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}

		if self.slock.state == STATE_LEADER {
			db := self.slock.dbs[lockCommand.DbId]
			if db == nil {
				err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}
			return db.UnLock(self, lockCommand)
		}

		clientProtocol, err := self.CheckClient()
		if err != nil || clientProtocol == nil {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}

		err = clientProtocol.Write(lockCommand)
		if err != nil {
			err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_ERROR, 0, 0)
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return err
		}
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return nil
	default:
		var command protocol.ICommand
		switch commandType {
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
			callCommand := protocol.CallCommand{}
			err := callCommand.Decode(buf)
			if err != nil {
				return err
			}

			callCommand.Data = make([]byte, callCommand.ContentLen)
			if callCommand.ContentLen > 0 {
				rindex, contentLen := self.serverProtocol.rindex+64, int(callCommand.ContentLen)
				if self.serverProtocol.rlen-rindex >= contentLen {
					copy(callCommand.Data, self.serverProtocol.rbuf[rindex:rindex+contentLen])
					self.serverProtocol.rindex += contentLen
					contentLen = 0
				} else if self.serverProtocol.rlen-rindex > 0 {
					copy(callCommand.Data, self.serverProtocol.rbuf[rindex:self.serverProtocol.rlen])
					contentLen -= self.serverProtocol.rlen - rindex
					self.serverProtocol.rindex += self.serverProtocol.rlen - rindex
				}

				if contentLen > 0 {
					_, err := self.stream.ReadBytes(callCommand.Data[int(callCommand.ContentLen)-contentLen:])
					if err != nil {
						return err
					}
				}
			}
			err = self.ProcessCommad(&callCommand)
			if err != nil {
				return err
			}
			return nil
		case protocol.COMMAND_WILL_LOCK:
			command = self.serverProtocol.GetLockCommand()
		case protocol.COMMAND_WILL_UNLOCK:
			command = self.serverProtocol.GetLockCommand()
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
	return self.serverProtocol.ProcessBuild(command)
}

func (self *TransparencyBinaryServerProtocol) ProcessCommad(command protocol.ICommand) error {
	if self.slock.state != STATE_LEADER {
		switch command.GetCommandType() {
		case protocol.COMMAND_LOCK:
			lockCommand := command.(*protocol.LockCommand)

			if lockCommand.DbId == 0xff {
				err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}

			clientProtocol, err := self.CheckClient()
			if err != nil || clientProtocol == nil {
				err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}

			err = clientProtocol.Write(lockCommand)
			if err != nil {
				err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return nil

		case protocol.COMMAND_UNLOCK:
			lockCommand := command.(*protocol.LockCommand)

			if lockCommand.DbId == 0xff {
				err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
				_ = self.FreeLockCommand(lockCommand)
				return err
			}

			clientProtocol, err := self.CheckClient()
			if err != nil || clientProtocol == nil {
				err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}

			err = clientProtocol.Write(lockCommand)
			if err != nil {
				err := self.serverProtocol.ProcessLockResultCommand(lockCommand, protocol.RESULT_STATE_ERROR, 0, 0)
				_ = self.serverProtocol.FreeLockCommand(lockCommand)
				return err
			}
			_ = self.serverProtocol.FreeLockCommand(lockCommand)
			return nil

		case protocol.COMMAND_INIT:
			initCommand := command.(*protocol.InitCommand)
			err := self.Init(initCommand.ClientId)
			if err != nil {
				return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_ERROR, 0))
			}

			self.slock.glock.Lock()
			self.slock.streams[initCommand.ClientId] = self.serverProtocol
			self.initCommand = initCommand
			self.slock.glock.Unlock()

			clientProtocol, err := self.CheckClient()
			if err != nil || clientProtocol == nil {
				return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_STATE_ERROR, 0))
			}
			if clientProtocol.initCommand == initCommand {
				return nil
			}

			err = clientProtocol.Write(initCommand)
			if err != nil {
				return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_ERROR, 0))
			}
			return nil
		}
	}
	return self.serverProtocol.ProcessCommad(command)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	return self.serverProtocol.ProcessLockCommand(lockCommand)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.serverProtocol.ProcessLockResultCommand(command, result, lcount, lrcount)
}

func (self *TransparencyBinaryServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.serverProtocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
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
	self.serverProtocol.InitLockCommand()
}

func (self *TransparencyBinaryServerProtocol) UnInitLockCommand() {
	self.serverProtocol.UnInitLockCommand()
}

func (self *TransparencyBinaryServerProtocol) GetLockCommand() *protocol.LockCommand {
	return self.serverProtocol.GetLockCommand()
}

func (self *TransparencyBinaryServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
	return self.serverProtocol.GetLockCommandLocked()
}

func (self *TransparencyBinaryServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.serverProtocol.FreeLockCommand(command)
}

func (self *TransparencyBinaryServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.serverProtocol.FreeLockCommandLocked(command)
}

func (self *TransparencyBinaryServerProtocol) commandHandleListLockCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandleListLockCommand(serverProtocol, command)
	}

	clientProtocol, err := self.CheckClient()
	if err != nil || clientProtocol == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "CLIENT_ERROR", nil), nil
	}

	err = clientProtocol.Write(command)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "WRITE_ERROR", nil), nil
	}
	return nil, nil
}

func (self *TransparencyBinaryServerProtocol) commandHandleListLockedCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandleListLockedCommand(serverProtocol, command)
	}

	clientProtocol, err := self.CheckClient()
	if err != nil || clientProtocol == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "CLIENT_ERROR", nil), nil
	}

	err = clientProtocol.Write(command)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "WRITE_ERROR", nil), nil
	}
	return nil, nil
}

func (self *TransparencyBinaryServerProtocol) commandHandleListWaitCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandleListWaitCommand(serverProtocol, command)
	}

	clientProtocol, err := self.CheckClient()
	if err != nil || clientProtocol == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "CLIENT_ERROR", nil), nil
	}

	err = clientProtocol.Write(command)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "WRITE_ERROR", nil), nil
	}
	return nil, nil
}

type TransparencyTextServerProtocol struct {
	slock          *SLock
	manager        *TransparencyManager
	glock          *sync.Mutex
	stream         *Stream
	serverProtocol *TextServerProtocol
	clientProtocol *TransparencyBinaryClientProtocol
	lockWaiter     chan *protocol.LockResultCommand
	closed         bool
}

func NewTransparencyTextServerProtocol(slock *SLock, stream *Stream, serverProtocol *TextServerProtocol) *TransparencyTextServerProtocol {
	transparencyProtocol := &TransparencyTextServerProtocol{slock, slock.replicationManager.transparencyManager, &sync.Mutex{},
		stream, serverProtocol, nil, make(chan *protocol.LockResultCommand, 4), false}
	if serverProtocol.handlers == nil {
		_, _ = serverProtocol.FindHandler("LOCK")
	}
	if serverProtocol.handlers != nil {
		serverProtocol.handlers["LOCK"] = transparencyProtocol.commandHandlerLock
		serverProtocol.handlers["UNLOCK"] = transparencyProtocol.commandHandlerUnlock
		serverProtocol.handlers["PUSH"] = transparencyProtocol.commandHandlerPush
	}
	return transparencyProtocol
}

func (self *TransparencyTextServerProtocol) FindHandler(name string) (TextServerProtocolCommandHandler, error) {
	if self.serverProtocol.handlers == nil {
		handler, err := self.serverProtocol.FindHandler(name)
		self.serverProtocol.handlers["LOCK"] = self.commandHandlerLock
		self.serverProtocol.handlers["UNLOCK"] = self.commandHandlerUnlock
		self.serverProtocol.handlers["PUSH"] = self.commandHandlerPush

		if name != "LOCK" && name != "UNLOCK" && name != "PUSH" {
			return handler, err
		}
	}

	if name == "LOCK" {
		return self.commandHandlerLock, nil
	}
	if name == "UNLOCK" {
		return self.commandHandlerUnlock, nil
	}
	if name == "PUSH" {
		return self.commandHandlerPush, nil
	}
	return self.serverProtocol.FindHandler(name)
}

func (self *TransparencyTextServerProtocol) Init(_ [16]byte) error {
	return nil
}

func (self *TransparencyTextServerProtocol) Lock() {
	self.serverProtocol.Lock()
}

func (self *TransparencyTextServerProtocol) Unlock() {
	self.serverProtocol.Unlock()
}

func (self *TransparencyTextServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	willCommands := self.serverProtocol.willCommands
	if willCommands != nil {
		self.serverProtocol.willCommands = nil
		self.glock.Unlock()

		clientProtocol, err := self.CheckClient()
		if err == nil && clientProtocol != nil {
			for {
				command := willCommands.Pop()
				if command == nil {
					break
				}
				_ = clientProtocol.Write(command)
				_ = self.serverProtocol.FreeLockCommand(command)
			}
		}
		self.glock.Lock()
	}

	if self.clientProtocol != nil {
		_ = self.manager.ReleaseClient(self.clientProtocol)
	}
	err := self.serverProtocol.Close()
	self.glock.Unlock()
	return err
}

func (self *TransparencyTextServerProtocol) GetParser() *protocol.TextParser {
	return self.serverProtocol.GetParser()
}

func (self *TransparencyTextServerProtocol) Read() (protocol.CommandDecode, error) {
	return self.serverProtocol.Read()
}

func (self *TransparencyTextServerProtocol) Write(result protocol.CommandEncode) error {
	return self.serverProtocol.Write(result)
}

func (self *TransparencyTextServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.serverProtocol.ReadCommand()
}

func (self *TransparencyTextServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	return self.serverProtocol.WriteCommand(result)
}

func (self *TransparencyTextServerProtocol) CheckClient() (*TransparencyBinaryClientProtocol, error) {
	arbiterWaiter := self.manager.arbiterWaiter
	if arbiterWaiter != nil {
		<-arbiterWaiter
	}

	if self.clientProtocol != nil {
		if self.clientProtocol.clientProtocol != nil {
			return self.clientProtocol, nil
		}
		self.clientProtocol = nil
	}

	if self.slock.state == STATE_SYNC {
		waiter := make(chan bool, 1)
		self.slock.replicationManager.WaitInitSynced(waiter)
		succed := <-waiter
		if !succed {
			return nil, io.EOF
		}
	}

	if self.slock.state != STATE_FOLLOWER {
		return nil, io.EOF
	}

	clientProtocol, err := self.manager.AcquireClient(self)
	if err != nil {
		return nil, err
	}
	self.clientProtocol = clientProtocol
	return self.clientProtocol, nil
}

func (self *TransparencyTextServerProtocol) Process() error {
	rbuf := self.serverProtocol.parser.GetReadBuf()
	for !self.closed {
		if self.serverProtocol.parser.IsBufferEnd() {
			n, err := self.stream.Read(rbuf)
			if err != nil {
				return err
			}

			self.serverProtocol.parser.BufferUpdate(n)
		}

		err := self.serverProtocol.parser.ParseRequest()
		if err != nil {
			return err
		}

		if self.serverProtocol.parser.IsParseFinish() {
			self.serverProtocol.totalCommandCount++
			commandName := self.serverProtocol.parser.GetCommandType()
			if commandHandler, err := self.FindHandler(commandName); err == nil {
				err := commandHandler(self.serverProtocol, self.serverProtocol.parser.GetArgs())
				if err != nil {
					return err
				}
			} else {
				err := self.commandHandlerUnknownCommand(self.serverProtocol, self.serverProtocol.parser.GetArgs())
				if err != nil {
					return err
				}
			}

			self.serverProtocol.parser.Reset()

			if self.slock.state == STATE_LEADER {
				return AGAIN
			}
		}
	}
	return nil
}

func (self *TransparencyTextServerProtocol) RunCommand() error {
	self.serverProtocol.totalCommandCount++
	commandName := self.serverProtocol.parser.GetCommandType()
	if commandHandler, err := self.FindHandler(commandName); err == nil {
		err := commandHandler(self.serverProtocol, self.serverProtocol.parser.GetArgs())
		if err != nil {
			return err
		}
	} else {
		err := self.commandHandlerUnknownCommand(self.serverProtocol, self.serverProtocol.parser.GetArgs())
		if err != nil {
			return err
		}
	}

	self.serverProtocol.parser.Reset()
	return nil
}

func (self *TransparencyTextServerProtocol) ProcessParse(buf []byte) error {
	return self.serverProtocol.ProcessParse(buf)
}

func (self *TransparencyTextServerProtocol) ProcessBuild(command protocol.ICommand) error {
	return self.serverProtocol.ProcessBuild(command)
}

func (self *TransparencyTextServerProtocol) ProcessCommad(command protocol.ICommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.serverProtocol.ProcessCommad(command)
}

func (self *TransparencyTextServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	if self.slock.state != STATE_LEADER {
		return errors.New("state error")
	}
	return self.serverProtocol.ProcessLockCommand(lockCommand)
}

func (self *TransparencyTextServerProtocol) ProcessLockResultCommand(lockCommand *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.serverProtocol.ProcessLockResultCommand(lockCommand, result, lcount, lrcount)
}

func (self *TransparencyTextServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.serverProtocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
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
	self.serverProtocol.InitLockCommand()
}

func (self *TransparencyTextServerProtocol) UnInitLockCommand() {
	self.serverProtocol.UnInitLockCommand()
}

func (self *TransparencyTextServerProtocol) GetLockCommand() *protocol.LockCommand {
	return self.serverProtocol.GetLockCommand()
}

func (self *TransparencyTextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.serverProtocol.FreeLockCommand(command)
}

func (self *TransparencyTextServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.serverProtocol.FreeLockCommandLocked(command)
}

func (self *TransparencyTextServerProtocol) commandHandlerUnknownCommand(serverProtocol *TextServerProtocol, args []string) error {
	return self.serverProtocol.commandHandlerUnknownCommand(serverProtocol, args)
}

func (self *TransparencyTextServerProtocol) commandHandlerLock(serverProtocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandlerLock(serverProtocol, args)
	}

	lockCommand, err := self.serverProtocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	if lockCommand.CommandType == protocol.COMMAND_WILL_LOCK {
		if self.serverProtocol.willCommands == nil {
			self.glock.Lock()
			if self.serverProtocol.willCommands == nil {
				self.serverProtocol.willCommands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		lockCommand.CommandType = protocol.COMMAND_LOCK
		_ = self.serverProtocol.willCommands.Push(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(true, "OK", nil))
	}

	clientProtocol, cerr := self.CheckClient()
	if cerr != nil || clientProtocol == nil {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	self.serverProtocol.lockRequestId = lockCommand.RequestId
	err = clientProtocol.Write(lockCommand)
	if err != nil {
		self.serverProtocol.lockRequestId[0], self.serverProtocol.lockRequestId[1], self.serverProtocol.lockRequestId[2], self.serverProtocol.lockRequestId[3], self.serverProtocol.lockRequestId[4], self.serverProtocol.lockRequestId[5], self.serverProtocol.lockRequestId[6], self.serverProtocol.lockRequestId[7],
			self.serverProtocol.lockRequestId[8], self.serverProtocol.lockRequestId[9], self.serverProtocol.lockRequestId[10], self.serverProtocol.lockRequestId[11], self.serverProtocol.lockRequestId[12], self.serverProtocol.lockRequestId[13], self.serverProtocol.lockRequestId[14], self.serverProtocol.lockRequestId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		if self.clientProtocol != nil {
			_ = self.manager.ReleaseClient(self.clientProtocol)
		}
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Lock Error", nil))
	}

	lockCommandResult := <-self.lockWaiter
	if lockCommandResult.Result == 0 {
		self.serverProtocol.lockId = lockCommand.LockId
	}
	if self.clientProtocol != nil {
		_ = self.manager.ReleaseClient(self.clientProtocol)
	}

	bufIndex := 0
	tr := ""

	wbuf := self.serverProtocol.parser.GetWriteBuf()
	bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommand.LockId)))
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

	_ = self.serverProtocol.FreeLockCommand(lockCommand)
	self.serverProtocol.freeCommandResult = lockCommandResult
	return self.stream.WriteBytes(wbuf[:bufIndex])
}

func (self *TransparencyTextServerProtocol) commandHandlerUnlock(serverProtocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandlerUnlock(serverProtocol, args)
	}

	lockCommand, err := self.serverProtocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	if lockCommand.CommandType == protocol.COMMAND_WILL_UNLOCK {
		if self.serverProtocol.willCommands == nil {
			self.glock.Lock()
			if self.serverProtocol.willCommands == nil {
				self.serverProtocol.willCommands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		lockCommand.CommandType = protocol.COMMAND_UNLOCK
		_ = self.serverProtocol.willCommands.Push(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(true, "OK", nil))
	}

	clientProtocol, cerr := self.CheckClient()
	if cerr != nil || clientProtocol == nil {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	self.serverProtocol.lockRequestId = lockCommand.RequestId
	err = clientProtocol.Write(lockCommand)
	if err != nil {
		self.serverProtocol.lockRequestId[0], self.serverProtocol.lockRequestId[1], self.serverProtocol.lockRequestId[2], self.serverProtocol.lockRequestId[3], self.serverProtocol.lockRequestId[4], self.serverProtocol.lockRequestId[5], self.serverProtocol.lockRequestId[6], self.serverProtocol.lockRequestId[7],
			self.serverProtocol.lockRequestId[8], self.serverProtocol.lockRequestId[9], self.serverProtocol.lockRequestId[10], self.serverProtocol.lockRequestId[11], self.serverProtocol.lockRequestId[12], self.serverProtocol.lockRequestId[13], self.serverProtocol.lockRequestId[14], self.serverProtocol.lockRequestId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		if self.clientProtocol != nil {
			_ = self.manager.ReleaseClient(self.clientProtocol)
		}
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR UnLock Error", nil))
	}

	lockCommandResult := <-self.lockWaiter
	if lockCommandResult.Result == 0 {
		self.serverProtocol.lockId[0], self.serverProtocol.lockId[1], self.serverProtocol.lockId[2], self.serverProtocol.lockId[3],
			self.serverProtocol.lockId[4], self.serverProtocol.lockId[5], self.serverProtocol.lockId[6], self.serverProtocol.lockId[7],
			self.serverProtocol.lockId[8], self.serverProtocol.lockId[9], self.serverProtocol.lockId[10], self.serverProtocol.lockId[11],
			self.serverProtocol.lockId[12], self.serverProtocol.lockId[13], self.serverProtocol.lockId[14], self.serverProtocol.lockId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
	}
	if self.clientProtocol != nil {
		_ = self.manager.ReleaseClient(self.clientProtocol)
	}

	bufIndex := 0
	tr := ""

	wbuf := self.serverProtocol.parser.GetWriteBuf()
	bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommand.LockId)))
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

	_ = self.serverProtocol.FreeLockCommand(lockCommand)
	self.serverProtocol.freeCommandResult = lockCommandResult
	return self.stream.WriteBytes(wbuf[:bufIndex])
}

func (self *TransparencyTextServerProtocol) commandHandlerPush(serverProtocol *TextServerProtocol, args []string) error {
	if self.slock.state == STATE_LEADER {
		return self.serverProtocol.commandHandlerPush(serverProtocol, args)
	}

	lockCommand, err := self.serverProtocol.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Uknown DB Error", nil))
	}

	clientProtocol, cerr := self.CheckClient()
	if cerr != nil || clientProtocol == nil {
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Leader Server Error", nil))
	}

	err = clientProtocol.Write(lockCommand)
	if err != nil {
		if self.clientProtocol != nil {
			_ = self.manager.ReleaseClient(self.clientProtocol)
		}
		_ = self.serverProtocol.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(false, "ERR Lock Error", nil))
	}

	if self.clientProtocol != nil {
		_ = self.manager.ReleaseClient(self.clientProtocol)
	}
	_ = self.serverProtocol.FreeLockCommand(lockCommand)
	return self.stream.WriteBytes(self.serverProtocol.parser.BuildResponse(true, "OK", nil))
}

type TransparencyManager struct {
	slock         *SLock
	glock         *sync.Mutex
	clients       *TransparencyBinaryClientProtocol
	idleClients   *TransparencyBinaryClientProtocol
	leaderAddress string
	closed        bool
	closedWaiter  chan bool
	wakeupSignal  chan bool
	arbiterWaiter chan bool
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
	currentClient := self.clients
	for currentClient != nil {
		if currentClient.clientProtocol != nil {
			_ = currentClient.clientProtocol.Close()
		}
		currentClient = currentClient.nextClient
	}

	currentClient = self.idleClients
	for currentClient != nil {
		if currentClient.clientProtocol != nil {
			_ = currentClient.clientProtocol.Close()
		}
		currentClient = currentClient.nextClient
	}
	self.glock.Unlock()
	self.Wakeup()
	return nil
}

func (self *TransparencyManager) Run() {
	timeout := 120
	for !self.closed {
		self.glock.Lock()
		self.wakeupSignal = make(chan bool, 1)
		self.glock.Unlock()

		select {
		case <-self.wakeupSignal:
			self.glock.Lock()
			timeout = self.CheckArbiterWaiter()
			self.glock.Unlock()
		case <-time.After(time.Duration(timeout) * time.Second):
			self.glock.Lock()
			self.wakeupSignal = nil
			timeout = self.CheckArbiterWaiter()
			self.glock.Unlock()
			self.checkIdleTimeout()
		}
	}
	close(self.closedWaiter)
}

func (self *TransparencyManager) Wakeup() {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
}

func (self *TransparencyManager) CheckArbiterWaiter() int {
	if self.arbiterWaiter != nil {
		close(self.arbiterWaiter)
		self.arbiterWaiter = nil
	}
	if self.leaderAddress == "" && self.slock.state != STATE_LEADER && !self.closed {
		self.arbiterWaiter = make(chan bool, 1)
		return 2
	}
	return 120
}

func (self *TransparencyManager) checkIdleTimeout() {
	defer self.glock.Unlock()
	self.glock.Lock()

	now, idleCount := time.Now(), uint(0)
	currentClient := self.idleClients
	self.idleClients = nil
	for currentClient != nil {
		if now.Unix()-currentClient.idleTime.Unix() > 900 || idleCount > 5 {
			if currentClient.clientProtocol != nil {
				_ = currentClient.clientProtocol.Close()
			}
		} else {
			currentClient.nextClient = self.idleClients
			self.idleClients = currentClient
			idleCount++
		}
		currentClient = currentClient.nextClient
	}
}

func (self *TransparencyManager) AcquireClient(serverProtocol ServerProtocol) (*TransparencyBinaryClientProtocol, error) {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.closed {
		return nil, errors.New("server closed")
	}

	if self.idleClients == nil {
		binaryClient, err := self.OpenClient(nil)
		if err != nil {
			return binaryClient, err
		}

		binaryClient.nextClient = self.clients
		self.clients = binaryClient
		binaryClient.serverProtocol = serverProtocol
		return binaryClient, err
	}

	binaryClient := self.idleClients
	self.idleClients = self.idleClients.nextClient
	binaryClient.nextClient = self.clients
	self.clients = binaryClient
	binaryClient.serverProtocol = serverProtocol
	return binaryClient, nil
}

func (self *TransparencyManager) ReleaseClient(binaryClient *TransparencyBinaryClientProtocol) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.clients == binaryClient {
		self.clients = binaryClient.nextClient
	} else {
		currentClient := self.clients
		for currentClient != nil {
			if currentClient.nextClient == binaryClient {
				currentClient.nextClient = binaryClient.nextClient
				break
			}
			currentClient = currentClient.nextClient
		}
	}

	if binaryClient.closed || self.closed {
		if binaryClient.clientProtocol != nil {
			_ = binaryClient.clientProtocol.Close()
		}
	} else {
		binaryClient.nextClient = self.idleClients
		self.idleClients = binaryClient
	}

	if binaryClient.serverProtocol != nil {
		switch binaryClient.serverProtocol.(type) {
		case *TransparencyBinaryServerProtocol:
			serverProtocol := binaryClient.serverProtocol.(*TransparencyBinaryServerProtocol)
			serverProtocol.clientProtocol = nil
		case *TransparencyTextServerProtocol:
			serverProtocol := binaryClient.serverProtocol.(*TransparencyTextServerProtocol)
			serverProtocol.clientProtocol = nil
		}
	}
	binaryClient.serverProtocol = nil
	binaryClient.idleTime = time.Now()
	return nil
}

func (self *TransparencyManager) OpenClient(initCommand *protocol.InitCommand) (*TransparencyBinaryClientProtocol, error) {
	if self.closed || self.slock.state == STATE_LEADER || self.leaderAddress == "" {
		return nil, errors.New("can not create new client")
	}

	binaryClient := NewTransparencyBinaryClientProtocol(self)
	binaryClient.initCommand = initCommand
	err := binaryClient.Open(self.leaderAddress)
	if err != nil {
		return nil, err
	}
	binaryClient.idleTime = time.Now()
	go binaryClient.Process()
	return binaryClient, nil
}

func (self *TransparencyManager) CloseClient(binaryClient *TransparencyBinaryClientProtocol) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.clients == binaryClient {
		self.clients = binaryClient.nextClient
	} else {
		currentClient := self.clients
		for currentClient != nil {
			if currentClient.nextClient == binaryClient {
				currentClient.nextClient = binaryClient.nextClient
				break
			}
			currentClient = currentClient.nextClient
		}
	}

	if self.idleClients == binaryClient {
		self.idleClients = binaryClient.nextClient
	} else {
		currentClient := self.idleClients
		for currentClient != nil {
			if currentClient.nextClient == binaryClient {
				currentClient.nextClient = binaryClient.nextClient
				break
			}
			currentClient = currentClient.nextClient
		}
	}

	if binaryClient.serverProtocol != nil {
		switch binaryClient.serverProtocol.(type) {
		case *TransparencyBinaryServerProtocol:
			serverProtocol := binaryClient.serverProtocol.(*TransparencyBinaryServerProtocol)
			serverProtocol.clientProtocol = nil
		case *TransparencyTextServerProtocol:
			serverProtocol := binaryClient.serverProtocol.(*TransparencyTextServerProtocol)
			serverProtocol.clientProtocol = nil
		}
	}
	binaryClient.serverProtocol = nil
	binaryClient.idleTime = time.Now()
	return nil
}

func (self *TransparencyManager) processFinish(binaryClient *TransparencyBinaryClientProtocol) error {
	if binaryClient.initResultCommand != nil && binaryClient.serverProtocol != nil && self.leaderAddress != binaryClient.leaderAddress {
		initType := binaryClient.initResultCommand.InitType
		if self.leaderAddress == "" {
			binaryClient.initResultCommand.InitType = 5
		} else {
			binaryClient.initResultCommand.InitType = 6
		}
		_ = binaryClient.serverProtocol.Write(binaryClient.initResultCommand)
		binaryClient.initResultCommand.InitType = initType
	}

	if self.closed || self.slock.state == STATE_LEADER || self.leaderAddress == "" || binaryClient.closed {
		return io.EOF
	}

	err := binaryClient.RetryOpen(self.leaderAddress)
	if err != nil {
		return err
	}
	return nil
}

func (self *TransparencyManager) ChangeLeader(address string) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	changed := false
	if self.leaderAddress != address {
		self.slock.Log().Infof("Transparency start change leader to %s", address)
		changed = true
	}
	self.leaderAddress = address

	if changed || address == "" {
		currentClient := self.clients
		for currentClient != nil {
			if currentClient.clientProtocol != nil {
				_ = currentClient.clientProtocol.Close()
			}
			currentClient = currentClient.nextClient
		}

		currentClient = self.idleClients
		for currentClient != nil {
			if currentClient.clientProtocol != nil {
				_ = currentClient.clientProtocol.Close()
			}
			currentClient = currentClient.nextClient
		}
	}

	if changed {
		if self.wakeupSignal != nil {
			close(self.wakeupSignal)
			self.wakeupSignal = nil
		}
		self.slock.Log().Infof("Transparency finish change leader to %s", address)
	}
	return nil
}
