package server

import (
	"errors"
	"github.com/hhkbp2/go-logging"
	"github.com/snower/slock/protocol"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	STATE_INIT = iota
	STATE_LEADER
	STATE_FOLLOWER
	STATE_SYNC
	STATE_CONFIG
	STATE_VOTE
	STATE_CLOSE
)

type SLock struct {
	server                 *Server
	dbs                    []*LockDB
	glock                  *sync.Mutex
	aof                    *Aof
	replicationManager     *ReplicationManager
	arbiterManager         *ArbiterManager
	subscribeManager       *SubscribeManager
	admin                  *Admin
	logger                 logging.Logger
	protocolSessions       map[uint32]*ServerProtocolSession
	protocolSessionsGlock  *sync.Mutex
	clients                map[[16]byte]ServerProtocol
	clientsGlock           *sync.Mutex
	uptime                 *time.Time
	freeLockCommandQueue   *LockCommandQueue
	freeLockCommandLock    *sync.Mutex
	freeLockCommandCount   int32
	statsTotalCommandCount uint64
	state                  uint8
}

func NewSLock(config *ServerConfig, logger logging.Logger) *SLock {
	SetConfig(config)

	aof := NewAof()
	replicationManager := NewReplicationManager()
	subscribeManager := NewSubscribeManager()
	admin := NewAdmin()
	now := time.Now()
	slock := &SLock{nil, make([]*LockDB, 256), &sync.Mutex{}, aof, replicationManager, nil, subscribeManager, admin, logger,
		make(map[uint32]*ServerProtocolSession, STREAMS_INIT_COUNT), &sync.Mutex{}, make(map[[16]byte]ServerProtocol, STREAMS_INIT_COUNT), &sync.Mutex{}, &now,
		NewLockCommandQueue(16, 64, FREE_COMMAND_QUEUE_INIT_SIZE*int32(Config.DBConcurrent)), &sync.Mutex{}, 0, 0, STATE_INIT}
	aof.slock = slock
	replicationManager.slock = slock
	replicationManager.transparencyManager.slock = slock
	subscribeManager.slock = slock
	admin.slock = slock
	defaultServerProtocol = NewDefaultServerProtocol(slock)
	return slock
}

func (self *SLock) Init(server *Server) error {
	self.server = server
	if Config.ReplSet != "" {
		dataDir, err := filepath.Abs(Config.DataDir)
		if err != nil {
			return err
		}

		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			self.logger.Errorf("Slock data dir config error %v", err)
			return err
		}

		self.updateState(STATE_CONFIG)
		self.arbiterManager = NewArbiterManager(self, Config.ReplSet)
		err = self.arbiterManager.Load()
		if err != nil {
			self.logger.Errorf("Arbiter load error %v", err)
			return err
		}
		self.logger.Infof("Slock init by replset")
		return nil
	}

	if Config.SlaveOf != "" {
		return self.initFollower(Config.SlaveOf)
	}
	return self.initLeader()
}

func (self *SLock) initLeader() error {
	self.updateState(STATE_INIT)
	err := self.aof.LoadAndInit()
	if err != nil {
		self.logger.Errorf("Aof LoadOrInit error %v", err)
		return err
	}

	self.updateState(STATE_LEADER)
	err = self.replicationManager.Init("")
	if err != nil {
		self.logger.Errorf("Replication init error %v", err)
		return err
	}
	self.logger.Infof("Slock init by leader")
	return nil
}

func (self *SLock) initFollower(leaderAddress string) error {
	_, err := net.ResolveTCPAddr("tcp", leaderAddress)
	if err != nil {
		return errors.New("host invalid error")
	}

	self.updateState(STATE_INIT)
	err = self.aof.Init()
	if err != nil {
		self.logger.Errorf("Aof init error %v", err)
		return err
	}

	self.updateState(STATE_SYNC)
	err = self.replicationManager.Init(leaderAddress)
	if err != nil {
		self.logger.Errorf("Replication init error %v", err)
		return err
	}
	_ = self.subscribeManager.ChangeLeader(leaderAddress)
	self.logger.Infof("Slock init by follower")
	return nil
}

func (self *SLock) Start() {
	if Config.ReplSet != "" {
		err := self.arbiterManager.Start()
		if err != nil {
			self.logger.Errorf("Arbiter start error %v", err)
			return
		}
		self.logger.Infof("Slock start by replset")
		return
	}

	if Config.SlaveOf != "" {
		self.startFollower()
		return
	}
	self.startLeader()
}

func (self *SLock) startLeader() {
	self.logger.Infof("Slock start by leader")
}

func (self *SLock) startFollower() {
	err := self.replicationManager.StartSync()
	if err != nil {
		self.logger.Errorf("Replication start sync error %v", err)
		return
	}
	self.logger.Infof("Slock start by follower")
	return
}

func (self *SLock) Close() {
	if self.arbiterManager != nil {
		_ = self.arbiterManager.Close()
	}
	self.replicationManager.Close()
	self.glock.Lock()
	for i, db := range self.dbs {
		if db != nil {
			db.Close()
			self.dbs[i] = nil
		}
	}
	self.glock.Unlock()
	self.admin.Close()
	self.subscribeManager.Close()
	self.aof.Close()
	self.server = nil
	self.logger.Infof("Slock closed")
}

func (self *SLock) updateState(state uint8) {
	self.state = state

	for _, db := range self.dbs {
		if db != nil && db.status != STATE_CLOSE && state != STATE_CLOSE {
			db.status = state

			for i := uint16(0); i < db.managerMaxGlocks; i++ {
				db.managerGlocks[i].Lock()
				db.managerGlocks[i].Unlock()
			}
		}
	}
}

func (self *SLock) GetAof() *Aof {
	return self.aof
}

func (self *SLock) GetReplicationManager() *ReplicationManager {
	return self.replicationManager
}

func (self *SLock) GetArbiterManager() *ArbiterManager {
	return self.arbiterManager
}

func (self *SLock) GetSubscribeManager() *SubscribeManager {
	return self.subscribeManager
}

func (self *SLock) GetAdmin() *Admin {
	return self.admin
}

func (self *SLock) GetOrNewDB(dbId uint8) *LockDB {
	self.glock.Lock()
	if self.dbs[dbId] == nil {
		self.dbs[dbId] = NewLockDB(self, dbId)
	}
	self.glock.Unlock()
	return self.dbs[dbId]
}

func (self *SLock) GetDB(dbId uint8) *LockDB {
	if self.dbs[dbId] == nil {
		return self.GetOrNewDB(dbId)
	}
	return self.dbs[dbId]
}

func (self *SLock) doLockComamnd(db *LockDB, serverProtocol ServerProtocol, command *protocol.LockCommand) error {
	return db.Lock(serverProtocol, command)
}

func (self *SLock) doUnLockComamnd(db *LockDB, serverProtocol ServerProtocol, command *protocol.LockCommand) error {
	return db.UnLock(serverProtocol, command)
}

func (self *SLock) GetState(serverProtocol ServerProtocol, command *protocol.StateCommand) error {
	dbState := uint8(0)

	db := self.dbs[command.DbId]
	if db != nil {
		dbState = 1
	}

	if db == nil {
		return serverProtocol.Write(protocol.NewStateResultCommand(command, protocol.RESULT_SUCCED, 0, dbState, nil))
	}
	return serverProtocol.Write(protocol.NewStateResultCommand(command, protocol.RESULT_SUCCED, 0, dbState, db.GetState()))
}

func (self *SLock) Log() logging.Logger {
	return self.logger
}

func (self *SLock) addServerProtocol(serverProtocol ServerProtocol) *ServerProtocolSession {
	self.protocolSessionsGlock.Lock()
	session := &ServerProtocolSession{0, serverProtocol, 0}
	for {
		serverProtocolSessionIdIndex++
		if serverProtocolSessionIdIndex == 0 {
			continue
		}
		if _, ok := self.protocolSessions[serverProtocolSessionIdIndex]; ok {
			continue
		}
		session.sessionId = serverProtocolSessionIdIndex
		break
	}
	self.protocolSessions[session.sessionId] = session
	self.protocolSessionsGlock.Unlock()
	return nil
}

func (self *SLock) removeServerProtocol(serverProtocolSession *ServerProtocolSession) error {
	if serverProtocolSession == nil {
		return nil
	}

	self.protocolSessionsGlock.Lock()
	if _, ok := self.protocolSessions[serverProtocolSession.sessionId]; ok {
		delete(self.protocolSessions, serverProtocolSession.sessionId)
	}
	self.protocolSessionsGlock.Unlock()
	return nil
}

func (self *SLock) checkServerProtocolSession() error {
	sessions := make([]*ServerProtocolSession, 0)
	self.protocolSessionsGlock.Lock()
	for _, session := range self.protocolSessions {
		sessions = append(sessions, session)
	}
	self.protocolSessionsGlock.Unlock()

	for _, session := range sessions {
		var freeLockCommands *LockCommandQueue = nil
		totalCommandCount := uint64(0)
		switch session.serverProtocol.(type) {
		case *MemWaiterServerProtocol:
			serverProtocol := session.serverProtocol.(*MemWaiterServerProtocol)
			if serverProtocol.closed {
				_ = self.removeServerProtocol(session)
				continue
			}
			freeLockCommands = serverProtocol.lockedFreeCommands
			totalCommandCount = serverProtocol.totalCommandCount
			if len(serverProtocol.proxys) > 4 {
				serverProtocol.Lock()
				for i := 4; i < len(serverProtocol.proxys); i++ {
					serverProtocol.proxys[i].serverProtocol = defaultServerProtocol
				}
				serverProtocol.proxys = serverProtocol.proxys[:4]
				serverProtocol.Unlock()
			}
		case *BinaryServerProtocol:
			serverProtocol := session.serverProtocol.(*BinaryServerProtocol)
			if serverProtocol.closed {
				_ = self.removeServerProtocol(session)
				continue
			}
			freeLockCommands = serverProtocol.lockedFreeCommands
			totalCommandCount = serverProtocol.totalCommandCount
			if len(serverProtocol.proxys) > 4 {
				serverProtocol.Lock()
				for i := 4; i < len(serverProtocol.proxys); i++ {
					serverProtocol.proxys[i].serverProtocol = defaultServerProtocol
				}
				serverProtocol.proxys = serverProtocol.proxys[:4]
				serverProtocol.Unlock()
			}
		case *TextServerProtocol:
			serverProtocol := session.serverProtocol.(*TextServerProtocol)
			if serverProtocol.closed {
				_ = self.removeServerProtocol(session)
				continue
			}
			freeLockCommands = serverProtocol.lockedFreeCommands
			totalCommandCount = serverProtocol.totalCommandCount
			if len(serverProtocol.proxys) > 4 {
				serverProtocol.Lock()
				for i := 4; i < len(serverProtocol.proxys); i++ {
					serverProtocol.proxys[i].serverProtocol = defaultServerProtocol
				}
				serverProtocol.proxys = serverProtocol.proxys[:4]
				serverProtocol.Unlock()
			}
		default:
			_ = self.removeServerProtocol(session)
			continue
		}

		avgTotalCommandCount := (totalCommandCount - session.totalCommandCount) / 60
		if avgTotalCommandCount < uint64(freeLockCommands.Len()) {
			freeCount := int((uint64(freeLockCommands.Len()) - avgTotalCommandCount) / 2)
			for i := 0; i < freeCount; i++ {
				session.serverProtocol.Lock()
				lockCommand := freeLockCommands.PopRight()
				session.serverProtocol.Unlock()
				if lockCommand != nil {
					self.freeLockCommandLock.Lock()
					_ = self.freeLockCommandQueue.Push(lockCommand)
					self.freeLockCommandCount++
					self.freeLockCommandLock.Unlock()
				}
			}
		}
		session.totalCommandCount = totalCommandCount
	}
	return nil
}

func (self *SLock) freeLockCommand(command *protocol.LockCommand) *protocol.LockCommand {
	self.freeLockCommandLock.Lock()
	_ = self.freeLockCommandQueue.Push(command)
	self.freeLockCommandCount++
	self.freeLockCommandLock.Unlock()
	return command
}

func (self *SLock) getLockCommand() *protocol.LockCommand {
	self.freeLockCommandLock.Lock()
	command := self.freeLockCommandQueue.PopRight()
	if command != nil {
		self.freeLockCommandCount--
	}
	self.freeLockCommandLock.Unlock()
	return command
}

func (self *SLock) freeLockCommands(commands []*protocol.LockCommand) error {
	self.freeLockCommandLock.Lock()
	for _, command := range commands {
		err := self.freeLockCommandQueue.Push(command)
		if err != nil {
			continue
		}
		self.freeLockCommandCount++
	}
	self.freeLockCommandLock.Unlock()
	return nil
}

func (self *SLock) getLockCommands(count int32) []*protocol.LockCommand {
	self.freeLockCommandLock.Lock()
	if count > self.freeLockCommandCount {
		count = self.freeLockCommandCount
	}
	commands := make([]*protocol.LockCommand, count)
	for i := int32(0); i < count; i++ {
		command := self.freeLockCommandQueue.PopRight()
		if command == nil {
			break
		}
		commands[i] = command
		self.freeLockCommandCount--
	}
	self.freeLockCommandLock.Unlock()
	return commands
}
