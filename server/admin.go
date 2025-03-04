package server

import (
	"fmt"
	"github.com/hhkbp2/go-logging"
	"github.com/snower/slock/protocol"
	"io"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var STATE_NAMES = []string{"initing", "leader", "follower", "syncing", "config", "vote", "close"}
var ROLE_NAMES = []string{"unknown", "leader", "follower", "arbiter"}

type Admin struct {
	slock  *SLock
	server *Server
	closed bool
}

func NewAdmin() *Admin {
	admin := &Admin{nil, nil, false}
	return admin
}

func (self *Admin) GetHandlers() map[string]TextServerProtocolCommandHandler {
	handlers := make(map[string]TextServerProtocolCommandHandler, 64)
	handlers["SHUTDOWN"] = self.commandHandleShutdownCommand
	handlers["BGREWRITEAOF"] = self.commandHandleBgRewritAaofCommand
	handlers["REWRITEAOF"] = self.commandHandleRewriteAofCommand
	handlers["ECHO"] = self.commandHandleEchoCommand
	handlers["PING"] = self.commandHandlePingCommand
	handlers["QUIT"] = self.commandHandleQuitCommand
	handlers["INFO"] = self.commandHandleInfoCommand
	handlers["SHOW"] = self.commandHandleShowCommand
	handlers["CONFIG"] = self.commandHandleConfigCommand
	handlers["CLIENT"] = self.commandHandleClientCommand
	handlers["FLUSHDB"] = self.commandHandleFlushDBCommand
	handlers["FLUSHALL"] = self.commandHandleFlushAllCommand
	handlers["SLAVEOF"] = self.commandHandleClientSlaveOfCommand
	handlers["REPLSET"] = self.commandHandleReplsetCommand
	return handlers
}

func (self *Admin) Close() {
	self.closed = true
}

func (self *Admin) commandHandleShutdownCommand(serverProtocol *TextServerProtocol, _ []string) error {
	err := serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
	if err != nil {
		return err
	}

	go func() {
		self.slock.Log().Infof("Admin command execute shutdown server")
		if self.server != nil {
			self.server.Close()
		}
	}()
	return io.EOF
}

func (self *Admin) commandHandleBgRewritAaofCommand(serverProtocol *TextServerProtocol, _ []string) error {
	if self.slock.state != STATE_LEADER {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "State Error", nil))
	}
	self.slock.aof.aofGlock.Lock()
	if self.slock.aof.isRewriting || self.slock.aof.isWaitRewite {
		self.slock.aof.aofGlock.Unlock()
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "Already Rewriting", nil))
	}
	err := serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
	if err != nil {
		self.slock.aof.aofGlock.Unlock()
		return err
	}

	go func() {
		self.slock.Log().Infof("Admin command execute aof files rewrite")
		_ = self.slock.GetAof().RewriteAofFile(true)
		self.slock.aof.aofGlock.Unlock()
	}()
	return nil
}

func (self *Admin) commandHandleRewriteAofCommand(serverProtocol *TextServerProtocol, _ []string) error {
	if self.slock.state != STATE_LEADER {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "State Error", nil))
	}
	self.slock.aof.aofGlock.Lock()
	if self.slock.aof.isRewriting || self.slock.aof.isWaitRewite {
		self.slock.aof.aofGlock.Unlock()
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "Already Rewriting", nil))
	}
	_ = self.slock.GetAof().RewriteAofFile(true)
	self.slock.aof.aofGlock.Unlock()
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleFlushDBCommand(serverProtocol *TextServerProtocol, args []string) error {
	if self.slock.state != STATE_LEADER {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "State Error", nil))
	}

	defer self.slock.glock.Unlock()
	self.slock.glock.Lock()

	if len(args) < 2 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Parse Len Error", nil))
	}

	dbId, err := strconv.Atoi(args[1])
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Parse DB_ID Error", nil))
	}
	db := self.slock.dbs[uint8(dbId)]
	if db == nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR No Such DB", nil))
	}

	err = db.FlushDB()
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, fmt.Sprintf("ERR Flush DB Error %s", err.Error()), nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleFlushAllCommand(serverProtocol *TextServerProtocol, _ []string) error {
	if self.slock.state != STATE_LEADER {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "State Error", nil))
	}

	defer self.slock.glock.Unlock()
	self.slock.glock.Lock()

	for dbId, db := range self.slock.dbs {
		err := db.FlushDB()
		if err != nil {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, fmt.Sprintf("ERR Flush DB %d Error %s", dbId, err.Error()), nil))
		}
	}

	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleEchoCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) != 2 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", args[1:]))
}

func (self *Admin) commandHandlePingCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) > 1 {
		if len(args) != 2 {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
		}
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", args[1:]))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "PONG", nil))
}

func (self *Admin) commandHandleQuitCommand(serverProtocol *TextServerProtocol, _ []string) error {
	err := serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
	if err != nil {
		return err
	}
	return io.EOF
}

func (self *Admin) commandHandleInfoCommand(serverProtocol *TextServerProtocol, args []string) error {
	infos := make([]string, 0)
	section := ""
	if len(args) >= 2 {
		section = strings.ToLower(args[1])
	}

	if section == "" || section == "server" {
		infos = append(infos, "# Server")
		infos = append(infos, fmt.Sprintf("version:%s", VERSION))
		infos = append(infos, fmt.Sprintf("process_id:%d", os.Getpid()))
		infos = append(infos, fmt.Sprintf("tcp_bind:%s", Config.Bind))
		infos = append(infos, fmt.Sprintf("tcp_port:%d", Config.Port))
		infos = append(infos, fmt.Sprintf("uptime_in_seconds:%d", time.Now().Unix()-self.slock.uptime.Unix()))
		infos = append(infos, fmt.Sprintf("state:%s", STATE_NAMES[self.slock.state]))
		infos = append(infos, "")
	}

	if section == "" || section == "clients" {
		infos = append(infos, "# Clients")
		infos = append(infos, fmt.Sprintf("total_clients:%d", self.server.connectedCount))
		infos = append(infos, fmt.Sprintf("connected_clients:%d", self.server.connectingCount))
		infos = append(infos, "")
	}

	if section == "" || section == "memory" {
		memoryStats := runtime.MemStats{}
		runtime.ReadMemStats(&memoryStats)
		infos = append(infos, "# Memory")
		infos = append(infos, fmt.Sprintf("used_memory:%d", memoryStats.HeapAlloc))
		infos = append(infos, fmt.Sprintf("used_memory_rss:%d", memoryStats.HeapSys))
		infos = append(infos, fmt.Sprintf("memory_alloc:%d", memoryStats.Alloc))
		infos = append(infos, fmt.Sprintf("memory_total_alloc:%d", memoryStats.TotalAlloc))
		infos = append(infos, fmt.Sprintf("memory_sys:%d", memoryStats.Sys))
		infos = append(infos, fmt.Sprintf("memory_mallocs:%d", memoryStats.Mallocs))
		infos = append(infos, fmt.Sprintf("memory_frees:%d", memoryStats.Frees))
		infos = append(infos, fmt.Sprintf("memory_heap_alloc:%d", memoryStats.HeapAlloc))
		infos = append(infos, fmt.Sprintf("memory_heap_sys:%d", memoryStats.HeapSys))
		infos = append(infos, fmt.Sprintf("memory_heap_idle:%d", memoryStats.HeapIdle))
		infos = append(infos, fmt.Sprintf("memory_heap_released:%d", memoryStats.HeapReleased))
		infos = append(infos, fmt.Sprintf("memory_heap_objects:%d", memoryStats.HeapObjects))
		infos = append(infos, fmt.Sprintf("memory_gc_sys:%d", memoryStats.GCSys))
		infos = append(infos, fmt.Sprintf("memory_gc_last:%d", memoryStats.LastGC))
		infos = append(infos, fmt.Sprintf("memory_gc_next:%d", memoryStats.NextGC))
		infos = append(infos, fmt.Sprintf("memory_gc_pause_totalns:%d", memoryStats.PauseTotalNs))
		infos = append(infos, fmt.Sprintf("memory_gc_num:%d", memoryStats.NumGC))
		infos = append(infos, fmt.Sprintf("memory_gc_num_forced:%d", memoryStats.NumForcedGC))
		infos = append(infos, "")
	}

	if (section == "" || section == "arbiter") && self.slock.arbiterManager != nil {
		infos = append(infos, "# Arbiter")
		infos = append(infos, fmt.Sprintf("name:%s", self.slock.arbiterManager.name))
		infos = append(infos, fmt.Sprintf("gid:%s", self.slock.arbiterManager.gid))
		infos = append(infos, fmt.Sprintf("version:%d", self.slock.arbiterManager.version))
		infos = append(infos, fmt.Sprintf("vertime:%d", self.slock.arbiterManager.vertime))

		for i, member := range self.slock.arbiterManager.GetMembers() {
			arbiter, isself, status, aofId := "no", "no", "offline", member.aofId
			if member.arbiter != 0 {
				arbiter = "yes"
			}
			if member.isSelf {
				isself = "yes"
				aofId = self.slock.arbiterManager.GetCurrentAofID()
			}
			if member.status == ARBITER_MEMBER_STATUS_ONLINE {
				status = "online"
			}
			infos = append(infos, fmt.Sprintf("member%d:host=%s,weight=%d,arbiter=%s,role=%s,status=%s,self=%s,aof_id=%s,update=%d,delay=%.2f", i+1, member.host, member.weight,
				arbiter, ROLE_NAMES[member.role], status, isself, FormatAofId(aofId), member.lastUpdated/1e6, float64(member.lastDelay)/1e6))
		}
		infos = append(infos, "")
	}

	if section == "" || section == "replication" {
		infos = append(infos, "# Replication")
		if self.slock.state == STATE_LEADER {
			infos = append(infos, "role:leader")
			infos = append(infos, fmt.Sprintf("connected_followers:%d", len(self.slock.replicationManager.serverChannels)))
			infos = append(infos, fmt.Sprintf("current_aof_id:%s", FormatAofId(self.slock.replicationManager.currentAofId)))
			infos = append(infos, fmt.Sprintf("current_offset:%d", self.slock.replicationManager.bufferQueue.seq))
			for i, serverChannel := range self.slock.replicationManager.serverChannels {
				serverChannelProtocol := serverChannel.protocol
				if serverChannelProtocol == nil {
					continue
				}

				var behindOffset uint64
				if self.slock.replicationManager.bufferQueue.seq < serverChannel.bufferCursor.seq {
					behindOffset = 0xffffffffffffffff - serverChannel.bufferCursor.seq + self.slock.replicationManager.bufferQueue.seq
				} else {
					behindOffset = self.slock.replicationManager.bufferQueue.seq - serverChannel.bufferCursor.seq - 1
				}
				status := "sending"
				if serverChannel.pulledState == 2 {
					status = "pending"
				}
				aofFileSendFinish := "no"
				if serverChannel.sendedFiles {
					aofFileSendFinish = "yes"
				}
				state := serverChannel.state
				infos = append(infos, fmt.Sprintf("follower%d:host=%s,aof_id=%s,behind_offset=%d,status=%s,push_count=%d,send_count=%d,ack_count=%d,send_data_size=%d,aof_file_send_finish=%s", i+1,
					serverChannelProtocol.RemoteAddr().String(), FormatAofId(serverChannel.bufferCursor.currentAofId), behindOffset, status,
					state.pushCount, state.sendCount, state.ackCount, state.sendDataSize, aofFileSendFinish))
			}
		} else {
			infos = append(infos, "role:follower")
			infos = append(infos, fmt.Sprintf("leader_host:%s", self.slock.replicationManager.leaderAddress))
			clientChannel := self.slock.replicationManager.clientChannel
			if clientChannel != nil && clientChannel.stream != nil {
				infos = append(infos, "leader_link_status:up")
			} else {
				infos = append(infos, "leader_link_status:down")
			}

			if clientChannel != nil {
				infos = append(infos, fmt.Sprintf("current_aof_id:%s", FormatAofId(clientChannel.currentAofId)))
				if clientChannel.recvedFiles {
					infos = append(infos, "aof_file_recv_finish:yes")
				} else {
					infos = append(infos, "aof_file_recv_finish:no")
				}
				state := clientChannel.state
				infos = append(infos, fmt.Sprintf("load_count:%d", state.loadCount))
				infos = append(infos, fmt.Sprintf("connect_count:%d", state.connectCount))
				infos = append(infos, fmt.Sprintf("recv_count:%d", state.recvCount))
				infos = append(infos, fmt.Sprintf("replay_count:%d", state.replayCount))
				infos = append(infos, fmt.Sprintf("append_count:%d", state.appendCount))
				infos = append(infos, fmt.Sprintf("push_count:%d", state.pushCount))
				infos = append(infos, fmt.Sprintf("ack_count:%d", state.ackCount))
				infos = append(infos, fmt.Sprintf("recv_data_size:%d", state.recvDataSize))
			}
		}
		infos = append(infos, "")
	}

	if section == "" || section == "transparency" {
		infos = append(infos, "# Transparency")
		transparencyManager := self.slock.replicationManager.transparencyManager
		clientCount, clientIdleCount := 0, 0
		transparencyManager.glock.Lock()
		currentClient := transparencyManager.clients
		for currentClient != nil {
			clientCount++
			currentClient = currentClient.nextClient
		}
		currentClient = transparencyManager.idleClients
		for currentClient != nil {
			clientIdleCount++
			currentClient = currentClient.nextClient
		}
		transparencyManager.glock.Unlock()
		infos = append(infos, fmt.Sprintf("leader:%s", transparencyManager.leaderAddress))
		infos = append(infos, fmt.Sprintf("client_count:%d", clientCount))
		infos = append(infos, fmt.Sprintf("client_idle_count:%d", clientIdleCount))
		infos = append(infos, "")
	}

	if section == "" || section == "stats" {
		dbCount := 0
		freeLockManagerCount := 0
		freeLockCount := 0
		freeLockCommandCount, cacheLockCommandCount := 0, 0
		totalCommandCount := uint64(0)
		longTimeoutQueueCount, longTimeoutLockCount, longExpriedQueueCount, longExpriedLockCount := 0, 0, 0, 0
		exectorCount, exectorRunningCount, exectorCoroutineCount, exectorCoroutineRunningCount, exectorQueueCount, exectorExecuteCount := 0, 0, 0, 0, 0, uint64(0)
		for _, db := range self.slock.dbs {
			if db != nil {
				dbCount++
				freeLockManagerHead, freeLockManagerTail := db.freeLockManagerHead, db.freeLockManagerTail
				if freeLockManagerHead >= freeLockManagerTail {
					freeLockManagerCount += int(freeLockManagerHead - freeLockManagerTail)
				} else {
					if freeLockManagerHead < 0x7fffffff && freeLockManagerTail > 0x7fffffff {
						freeLockManagerCount += int(0xffffffff - freeLockManagerTail + freeLockManagerHead)
					}
				}
				for i := uint16(0); i < db.managerMaxGlocks; i++ {
					db.managerGlocks[i].LowPriorityLock()
					freeLockCount += int(db.freeLocks[i].Len())
					longTimeoutQueueCount += len(db.longTimeoutLocks[i])
					for _, longLocks := range db.longTimeoutLocks[i] {
						longTimeoutLockCount += int(longLocks.lockCount - longLocks.freeCount)
					}
					longExpriedQueueCount += len(db.longExpriedLocks[i])
					for _, longLocks := range db.longExpriedLocks[i] {
						longExpriedLockCount += int(longLocks.lockCount - longLocks.freeCount)
					}
					exector := db.exectors[i]
					if exector != nil {
						exectorCount++
						if exector.queueWaited < exector.runningCount {
							exectorRunningCount++
						}
						exectorCoroutineCount += exector.runningCount
						exectorCoroutineRunningCount += exector.runningCount - exector.queueWaited
						exectorQueueCount += exector.queueCount
						exectorExecuteCount += exector.executeCount
					}
					db.managerGlocks[i].LowPriorityUnlock()
				}
			}
		}

		freeLockCommandCount += int(self.slock.freeLockCommandCount)
		totalCommandCount += self.slock.statsTotalCommandCount
		for _, stream := range self.server.GetStreams() {
			streamProtocol := stream.protocol
			if streamProtocol != nil {
				switch streamProtocol.(type) {
				case *MemWaiterServerProtocol:
					memWaitProtocol := streamProtocol.(*MemWaiterServerProtocol)
					cacheLockCommandCount += int(memWaitProtocol.freeCommandIndex)
					cacheLockCommandCount += int(memWaitProtocol.lockedFreeCommands.Len())
					totalCommandCount += memWaitProtocol.totalCommandCount
				case *BinaryServerProtocol:
					binaryProtocol := streamProtocol.(*BinaryServerProtocol)
					cacheLockCommandCount += int(binaryProtocol.freeCommandIndex)
					cacheLockCommandCount += int(binaryProtocol.lockedFreeCommands.Len())
					totalCommandCount += binaryProtocol.totalCommandCount
				case *TextServerProtocol:
					textProtocol := streamProtocol.(*TextServerProtocol)
					cacheLockCommandCount += int(textProtocol.freeCommandIndex)
					cacheLockCommandCount += int(textProtocol.lockedFreeCommands.Len())
					totalCommandCount += textProtocol.totalCommandCount
				}
			}
		}

		infos = append(infos, "# Stats")
		infos = append(infos, fmt.Sprintf("db_count:%d", dbCount))
		infos = append(infos, fmt.Sprintf("free_command_count:%d", freeLockCommandCount))
		infos = append(infos, fmt.Sprintf("cache_command_count:%d", cacheLockCommandCount))
		infos = append(infos, fmt.Sprintf("free_lock_manager_count:%d", freeLockManagerCount))
		infos = append(infos, fmt.Sprintf("free_lock_count:%d", freeLockCount))
		infos = append(infos, fmt.Sprintf("total_commands_processed:%d", totalCommandCount))
		highPriorityLockActivatingCount, lowPriorityLockActivatingCount := uint64(0), uint64(0)
		highPriorityLockPinningCount, lowPriorityLockPinningCount := uint64(0), uint64(0)
		highPriorityLockActivatedCount, lowPriorityLockActivatedCount := uint64(0), uint64(0)
		for _, db := range self.slock.dbs {
			if db == nil {
				continue
			}
			for j := uint16(0); j < db.managerMaxGlocks; j++ {
				if db.managerGlocks[j].highPriority != 0 {
					highPriorityLockActivatingCount++
				}
				if db.managerGlocks[j].lowPriority != 0 {
					lowPriorityLockActivatingCount++
				}
				highPriorityLockPinningCount += uint64(db.managerGlocks[j].highPriorityAcquireCount)
				lowPriorityLockPinningCount += uint64(db.managerGlocks[j].lowPriority)
				highPriorityLockActivatedCount += db.managerGlocks[j].setHighPriorityCount
				lowPriorityLockActivatedCount += db.managerGlocks[j].setLowPriorityCount
			}
		}
		infos = append(infos, fmt.Sprintf("high_priority_lock_activating_count:%d", highPriorityLockActivatingCount))
		infos = append(infos, fmt.Sprintf("low_priority_lock_activating_count:%d", lowPriorityLockActivatingCount))
		infos = append(infos, fmt.Sprintf("high_priority_lock_pinning_count:%d", highPriorityLockPinningCount))
		infos = append(infos, fmt.Sprintf("low_priority_lock_pinning_count:%d", lowPriorityLockPinningCount))
		infos = append(infos, fmt.Sprintf("high_priority_lock_activated_count:%d", highPriorityLockActivatedCount))
		infos = append(infos, fmt.Sprintf("low_priority_lock_activated_count:%d", lowPriorityLockActivatedCount))
		infos = append(infos, fmt.Sprintf("long_timeout_queue_count:%d", longTimeoutQueueCount))
		infos = append(infos, fmt.Sprintf("long_timeout_lock_count:%d", longTimeoutLockCount))
		infos = append(infos, fmt.Sprintf("long_expried_queue_count:%d", longExpriedQueueCount))
		infos = append(infos, fmt.Sprintf("long_expried_lock_count:%d", longExpriedLockCount))
		infos = append(infos, fmt.Sprintf("exector_count:%d", exectorCount))
		infos = append(infos, fmt.Sprintf("exector_running_count:%d", exectorRunningCount))
		infos = append(infos, fmt.Sprintf("exector_coroutine_count:%d", exectorCoroutineCount))
		infos = append(infos, fmt.Sprintf("exector_coroutine_running_count:%d", exectorCoroutineRunningCount))
		infos = append(infos, fmt.Sprintf("exector_queue_count:%d", exectorQueueCount))
		infos = append(infos, fmt.Sprintf("exector_execute_count:%d", exectorExecuteCount))
		infos = append(infos, "")
	}

	if section == "" || section == "persistence" {
		aof := self.slock.GetAof()
		infos = append(infos, "# Persistence")
		infos = append(infos, fmt.Sprintf("aof_channel_count:%d", aof.channelCount))
		infos = append(infos, fmt.Sprintf("aof_channel_active_count:%d", aof.channelActiveCount))
		channelHandingCount := 0
		for _, channel := range aof.channels {
			channelHandingCount += channel.queueCount
		}
		infos = append(infos, fmt.Sprintf("aof_channel_handing_count:%d", channelHandingCount))
		infos = append(infos, fmt.Sprintf("aof_count:%d", aof.aofLockCount))
		infos = append(infos, fmt.Sprintf("aof_ring_buffer_duplicate_count:%d", self.slock.replicationManager.bufferQueue.dupCount))
		if aof.aofFile != nil {
			infos = append(infos, fmt.Sprintf("aof_file_name:%s", aof.aofFile.filename))
			infos = append(infos, fmt.Sprintf("aof_file_size:%d", aof.aofFile.size))
			infos = append(infos, fmt.Sprintf("aof_file_data_size:%d", aof.aofFile.dataSize))
		}
		if aof.isRewriting {
			infos = append(infos, "aof_rewriting:yes")
		} else {
			infos = append(infos, "aof_rewriting:no")
		}
		infos = append(infos, "")
	}

	if section == "" || section == "subscribe" {
		infos = append(infos, "# Subscribe")
		infos = append(infos, fmt.Sprintf("subscriber:%d", len(self.slock.subscribeManager.fastSubscribers)))
		infos = append(infos, fmt.Sprintf("subscribe_channel_count:%d", self.slock.subscribeManager.channelCount))
		infos = append(infos, fmt.Sprintf("subscribe_channel_active_count:%d", self.slock.subscribeManager.channelActiveCount))
		channelHandingCount := 0
		for _, channel := range self.slock.subscribeManager.channels {
			channelHandingCount += channel.queueCount
		}
		infos = append(infos, fmt.Sprintf("subscribe_channel_handing_count:%d", channelHandingCount))
		infos = append(infos, "")
	}

	if section == "" || section == "keyspace" {
		infos = append(infos, "# Keyspace")
		for dbId, db := range self.slock.dbs {
			if db != nil {
				dbState := db.GetState()
				dbInfos := make([]string, 0)
				dbInfos = append(dbInfos, fmt.Sprintf("lock_count=%d", dbState.LockCount))
				dbInfos = append(dbInfos, fmt.Sprintf("unlock_count=%d", dbState.UnLockCount))
				dbInfos = append(dbInfos, fmt.Sprintf("locked_count=%d", dbState.LockedCount))
				dbInfos = append(dbInfos, fmt.Sprintf("wait_count=%d", dbState.WaitCount))
				dbInfos = append(dbInfos, fmt.Sprintf("timeouted_count=%d", dbState.TimeoutedCount))
				dbInfos = append(dbInfos, fmt.Sprintf("expried_count=%d", dbState.ExpriedCount))
				dbInfos = append(dbInfos, fmt.Sprintf("unlock_error_count=%d", dbState.UnlockErrorCount))
				dbInfos = append(dbInfos, fmt.Sprintf("key_count=%d", dbState.KeyCount))
				if self.slock.state == STATE_LEADER {
					ackDb := self.slock.replicationManager.GetAckDB(uint8(dbId))
					if ackDb != nil && ackDb.ackLocks != nil {
						waitAckCount := 0
						for i := uint16(0); i < ackDb.ackMaxGlocks; i++ {
							if len(ackDb.ackLocks[i]) >= len(ackDb.commandAofs[i]) {
								waitAckCount += len(ackDb.ackLocks[i])
							} else {
								waitAckCount += len(ackDb.commandAofs[i])
							}
						}
						dbInfos = append(dbInfos, fmt.Sprintf("wait_ack_count=%d", waitAckCount))
					} else {
						dbInfos = append(dbInfos, "wait_ack_count=0")
					}
				} else if self.slock.state == STATE_FOLLOWER {
					ackDb := self.slock.replicationManager.GetAckDB(uint8(dbId))
					if ackDb != nil && ackDb.ackLocks != nil {
						waitAckCount := 0
						for i := uint16(0); i < ackDb.ackMaxGlocks; i++ {
							waitAckCount += len(ackDb.ackLocks[i])
						}
						dbInfos = append(dbInfos, fmt.Sprintf("wait_ack_count=%d", waitAckCount))
					} else {
						dbInfos = append(dbInfos, "wait_ack_count=0")
					}
				} else {
					dbInfos = append(dbInfos, "wait_ack_count=0")
				}
				infos = append(infos, fmt.Sprintf("db%d:%s", dbId, strings.Join(dbInfos, ",")))
			}
		}
		infos = append(infos, "")
	}

	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", []string{strings.Join(infos, "\r\n")}))
}

func (self *Admin) commandHandleShowCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 1 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	db := self.slock.dbs[serverProtocol.dbId]
	if db == nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR DB Empty", nil))
	}

	if len(args) == 1 || (len(args) == 2 && args[1] == "*") {
		return self.commandHandleShowDBCommand(serverProtocol, args, db)
	}

	if len(args) == 3 && strings.ToUpper(args[2]) == "WAIT" {
		return self.commandHandleShowLockWaitCommand(serverProtocol, args, db)
	}
	return self.commandHandleShowLockCommand(serverProtocol, args, db)
}

func (self *Admin) commandHandleShowDBCommand(serverProtocol *TextServerProtocol, _ []string, db *LockDB) error {
	lockManagers := make([]*LockManager, 0)
	for _, value := range db.fastLocks {
		lockManager := value.manager
		if lockManager != nil && lockManager.locked > 0 {
			lockManagers = append(lockManagers, lockManager)
		}
	}

	db.mGlock.Lock()
	for _, lockManager := range db.locks {
		if lockManager.locked > 0 {
			lockManagers = append(lockManagers, lockManager)
		}
	}
	db.mGlock.Unlock()

	dbInfos := make([]string, 0)
	for _, lockManager := range lockManagers {
		dbInfos = append(dbInfos, fmt.Sprintf("%x", lockManager.lockKey))
		dbInfos = append(dbInfos, fmt.Sprintf("%d", lockManager.locked))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", dbInfos))
}

func (self *Admin) commandHandleShowLockCommand(serverProtocol *TextServerProtocol, args []string, db *LockDB) error {
	command := protocol.LockCommand{}
	serverProtocol.GetCommandConverter().ConvertArgId2LockId(args[1], &command.LockKey)

	lockManager := db.GetLockManager(&command)
	if lockManager == nil || lockManager.locked <= 0 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Unknown Lock Manager Error", nil))
	}

	lockInfos := make([]string, 0)
	lockManager.glock.LowPriorityLock()
	if lockManager.currentLock != nil {
		lock := lockManager.currentLock

		state := uint8(0)
		if lock.timeouted {
			state |= 0x01
		}
		if lock.expried {
			state |= 0x02
		}
		if lock.longWaitIndex > 0 {
			state |= 0x04
		}
		if lock.isAof {
			state |= 0x08
		}

		lockInfos = append(lockInfos, fmt.Sprintf("%x", lock.command.LockId))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.startTime))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.timeoutTime))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.expriedTime))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.locked))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.aofTime))
		lockInfos = append(lockInfos, fmt.Sprintf("%d", state))
		lockData := lockManager.GetLockData()
		if lockData != nil {
			lockInfos = append(lockInfos, string(lockData))
		}
	}

	if lockManager.locks != nil {
		for i := range lockManager.locks.queue.IterNodes() {
			nodeQueues := lockManager.locks.queue.IterNodeQueues(int32(i))
			for _, lock := range nodeQueues {
				if lock.locked == 0 {
					continue
				}

				state := uint8(0)
				if lock.timeouted {
					state |= 0x01
				}
				if lock.expried {
					state |= 0x02
				}
				if lock.longWaitIndex > 0 {
					state |= 0x04
				}
				if lock.isAof {
					state |= 0x08
				}

				lockInfos = append(lockInfos, fmt.Sprintf("%x", lock.command.LockId))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.startTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.timeoutTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.expriedTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.locked))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.aofTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", state))
				lockData := lockManager.GetLockData()
				if lockData != nil {
					lockInfos = append(lockInfos, string(lockData))
				}
			}
		}
	}
	lockManager.glock.LowPriorityUnlock()
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", lockInfos))
}

func (self *Admin) commandHandleShowLockWaitCommand(serverProtocol *TextServerProtocol, args []string, db *LockDB) error {
	command := protocol.LockCommand{}
	serverProtocol.GetCommandConverter().ConvertArgId2LockId(args[1], &command.LockKey)

	lockManager := db.GetLockManager(&command)
	if lockManager == nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Unknown Lock Manager Error", nil))
	}

	lockInfos := make([]string, 0)
	lockManager.glock.LowPriorityLock()
	if lockManager.waitLocks != nil {
		for _, waitLocks := range lockManager.waitLocks.IterNodes() {
			for _, lock := range waitLocks {
				if lock.timeouted {
					continue
				}

				state := uint8(0)
				if lock.timeouted {
					state |= 0x01
				}
				if lock.expried {
					state |= 0x02
				}
				if lock.longWaitIndex > 0 {
					state |= 0x04
				}
				if lock.isAof {
					state |= 0x08
				}

				lockInfos = append(lockInfos, fmt.Sprintf("%x", lock.command.LockId))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.startTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.timeoutTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.expriedTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.locked))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", lock.aofTime))
				lockInfos = append(lockInfos, fmt.Sprintf("%d", state))
				lockData := lockManager.GetLockData()
				if lockData != nil {
					lockInfos = append(lockInfos, string(lockData))
				}
			}
		}
	}
	lockManager.glock.LowPriorityUnlock()
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", lockInfos))
}

func (self *Admin) commandHandleConfigCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 2 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	if strings.ToUpper(args[1]) == "SET" {
		return self.commandHandleConfigSetCommand(serverProtocol, args)
	}
	return self.commandHandleConfigGetCommand(serverProtocol, args)
}

func (self *Admin) commandHandleConfigGetCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) >= 3 && strings.ToUpper(args[2]) == "DATABASES" {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", []string{"databases", "254"}))
	}

	ConfigValue := reflect.ValueOf(Config).Elem()
	ConfigType := ConfigValue.Type()
	infos := make([]string, 0)
	for i := 0; i < ConfigType.NumField(); i++ {
		configName := strings.ToUpper(ConfigType.Field(i).Tag.Get("long"))
		if len(args) >= 3 && configName != strings.ToUpper(args[2]) {
			continue
		}

		infos = append(infos, configName)
		value := ConfigValue.Field(i).Interface()
		switch value.(type) {
		case string:
			infos = append(infos, value.(string))
		case uint:
			infos = append(infos, fmt.Sprintf("%d", value.(uint)))
		default:
			infos = append(infos, fmt.Sprintf("%v", value))
		}
	}

	if len(infos) <= 0 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Unknown Config Parameter", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", infos))
}

func (self *Admin) commandHandleConfigSetCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 4 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	switch strings.ToUpper(args[2]) {
	case "DB_LOCK_AOF_TIME":
		dbLockAofTime, err := strconv.Atoi(args[3])
		if err != nil {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Parameter Value Error", nil))
		}

		Config.DBLockAofTime = uint(dbLockAofTime)
		for _, db := range self.slock.dbs {
			if db != nil {
				db.aofTime = uint8(dbLockAofTime)
			}
		}
	case "AOF_FILE_REWRITE_SIZE":
		aofFileRewriteSize, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Parameter Value Error", nil))
		}
		Config.DBLockAofTime = uint(aofFileRewriteSize)
		self.slock.GetAof().rewriteSize = uint32(aofFileRewriteSize)
	case "LOG_LEVEL":
		logger := self.slock.Log()
		loggingLevel := logging.LevelInfo
		switch args[3] {
		case "DEBUG":
			loggingLevel = logging.LevelDebug
		case "INFO":
			loggingLevel = logging.LevelInfo
		case "WARNING":
			loggingLevel = logging.LevelWarning
		case "ERROR":
			loggingLevel = logging.LevelError
		default:
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Unknown Log Level", nil))
		}
		Config.LogLevel = args[2]
		for _, handler := range logger.GetHandlers() {
			_ = handler.SetLevel(loggingLevel)
		}
		_ = logger.SetLevel(loggingLevel)
	default:
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR UnSupport Config Set Parameter", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleClientCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 2 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	if strings.ToUpper(args[1]) == "KILL" {
		return self.commandHandleClientKillCommand(serverProtocol, args)
	}
	return self.commandHandleClientListCommand(serverProtocol, args)
}

func (self *Admin) commandHandleClientListCommand(serverProtocol *TextServerProtocol, _ []string) error {
	infos := make([]string, 0)
	for _, stream := range self.server.GetStreams() {
		protocolName, clientId, commandCount := "", [16]byte{}, uint64(0)
		if stream.protocol != nil {
			switch stream.protocol.(type) {
			case *BinaryServerProtocol:
				binaryProtocol := stream.protocol.(*BinaryServerProtocol)
				protocolName = "binary"
				clientId = binaryProtocol.proxys[0].clientId
				commandCount += binaryProtocol.totalCommandCount
			case *TextServerProtocol:
				textProtocol := stream.protocol.(*TextServerProtocol)
				protocolName = "text"
				commandCount += textProtocol.totalCommandCount
			}
		}

		fd := ""
		if tcpConn, ok := stream.conn.(*net.TCPConn); ok {
			tcpConnFile, err := tcpConn.File()
			if err == nil {
				fd = fmt.Sprintf("%d", tcpConnFile.Fd())
			}
		}
		infos = append(infos, fmt.Sprintf("id=%d addr=%s fd=%s protocol=%s age=%d client_id=%x command_count=%d", stream.streamId, stream.RemoteAddr().String(),
			fd, protocolName, time.Now().Unix()-stream.startTime.Unix(), clientId, commandCount))
	}
	infos = append(infos, "\r\n")
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", []string{strings.Join(infos, "\r\n")}))
}

func (self *Admin) commandHandleClientKillCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	for _, stream := range self.server.GetStreams() {
		if stream.RemoteAddr().String() == args[2] {
			err := stream.Close()
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Client Close Error", nil))
			}
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
		}
	}

	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR No such client", nil))
}

func (self *Admin) commandHandleClientSlaveOfCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) == 1 || (len(args) >= 2 && args[1] == "") {
		if self.slock.state == STATE_LEADER {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
		}

		err := self.slock.replicationManager.SwitchToLeader()
		if err != nil {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Change Error", nil))
		}
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
	} else if len(args) >= 3 && args[1] != "" && args[2] != "" {
		if self.slock.state == STATE_FOLLOWER {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
		}

		err := self.slock.replicationManager.SwitchToFollower(fmt.Sprintf("%s:%s", args[1], args[2]))
		if err != nil {
			return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Change Error", nil))
		}
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
	}

	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
}

func (self *Admin) commandHandleReplsetCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 1 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	if self.slock.arbiterManager == nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Not Replset server", nil))
	}

	commandName := strings.ToUpper(args[1])
	switch commandName {
	case "CONFIG":
		return self.commandHandleReplsetConfigCommand(serverProtocol, args)
	case "ADD":
		return self.commandHandleReplsetAddCommand(serverProtocol, args)
	case "REMOVE":
		return self.commandHandleReplsetRemoveCommand(serverProtocol, args)
	case "SET":
		return self.commandHandleReplsetSetCommand(serverProtocol, args)
	case "GET":
		return self.commandHandleReplsetGetCommand(serverProtocol, args)
	case "MEMBERS":
		return self.commandHandleReplsetMembersCommand(serverProtocol, args)
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR unkonwn command", nil))
}

func (self *Admin) commandHandleReplsetConfigCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	weight, arbiter := 1, 0
	for i := 0; i < (len(args)-3)/2; i++ {
		switch strings.ToUpper(args[i*2+3]) {
		case "WEIGHT":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse weight error", nil))
			}
			if v >= 0 {
				weight = v
			}
		case "ARBITER":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse arbiter error", nil))
			}
			if v >= 0 {
				arbiter = v
			}
		}
	}

	err := self.slock.arbiterManager.Config(args[2], uint32(weight), uint32(arbiter))
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR config error", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleReplsetAddCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	weight, arbiter := 1, 0
	for i := 0; i < (len(args)-3)/2; i++ {
		switch strings.ToUpper(args[i*2+3]) {
		case "WEIGHT":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse weight error", nil))
			}
			if v >= 0 {
				weight = v
			}
		case "ARBITER":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse arbiter error", nil))
			}
			if v >= 0 {
				arbiter = v
			}
		}
	}

	err := self.slock.arbiterManager.AddMember(args[2], uint32(weight), uint32(arbiter))
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR add error", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleReplsetRemoveCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	err := self.slock.arbiterManager.RemoveMember(args[2])
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR remove error", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleReplsetSetCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	weight, arbiter := 1, 0
	for i := 0; i < (len(args)-3)/2; i++ {
		switch strings.ToUpper(args[i*2+3]) {
		case "WEIGHT":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse weight error", nil))
			}
			if v >= 0 {
				weight = v
			}
		case "ARBITER":
			v, err := strconv.Atoi(args[i*2+4])
			if err != nil {
				return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR parse arbiter error", nil))
			}
			if v >= 0 {
				arbiter = v
			}
		}
	}

	err := self.slock.arbiterManager.UpdateMember(args[2], uint32(weight), uint32(arbiter))
	if err != nil {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR update error", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) commandHandleReplsetGetCommand(serverProtocol *TextServerProtocol, args []string) error {
	if len(args) < 3 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
	}

	results := make([]string, 0)
	for _, member := range self.slock.arbiterManager.members {
		if member.host != args[2] {
			continue
		}

		results = append(results, member.host)
		results = append(results, fmt.Sprintf("%d", member.weight))
		results = append(results, fmt.Sprintf("%d", member.arbiter))
		results = append(results, ROLE_NAMES[member.role])
		if member.status == ARBITER_MEMBER_STATUS_ONLINE {
			results = append(results, "online")
		} else {
			results = append(results, "offline")
		}
	}

	if len(results) == 0 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR unknown member", nil))
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", results))
}

func (self *Admin) commandHandleReplsetMembersCommand(serverProtocol *TextServerProtocol, _ []string) error {
	if len(self.slock.arbiterManager.members) == 0 {
		return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(false, "ERR not config", nil))
	}

	results := make([]string, 0)
	for _, member := range self.slock.arbiterManager.members {
		results = append(results, member.host)
		results = append(results, fmt.Sprintf("%d", member.weight))
		results = append(results, fmt.Sprintf("%d", member.arbiter))
		results = append(results, ROLE_NAMES[member.role])
		if member.status == ARBITER_MEMBER_STATUS_ONLINE {
			results = append(results, "online")
		} else {
			results = append(results, "offline")
		}
	}
	return serverProtocol.stream.WriteBytes(serverProtocol.parser.BuildResponse(true, "", results))
}
