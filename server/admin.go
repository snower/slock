package server

import (
    "fmt"
    "github.com/hhkbp2/go-logging"
    "io"
    "net"
    "os"
    "reflect"
    "runtime"
    "strconv"
    "strings"
    "time"
)

var STATE_NAMES  = []string{"initing", "leader", "follower", "syncing", "config", "vote", "close"}

type Admin struct {
    slock *SLock
    server *Server
    closed bool
}

func NewAdmin() *Admin{
    admin := &Admin{nil,nil,false}
    return admin
}

func (self *Admin) GetHandlers() map[string]TextServerProtocolCommandHandler{
    handlers := make(map[string]TextServerProtocolCommandHandler, 64)
    handlers["SHUTDOWN"] = self.CommandHandleShutdownCommand
    handlers["BGREWRITEAOF"] = self.CommandHandleBgRewritAaofCommand
    handlers["REWRITEAOF"] = self.CommandHandleRewriteAofCommand
    handlers["ECHO"] = self.CommandHandleEchoCommand
    handlers["PING"] = self.CommandHandlePingCommand
    handlers["QUIT"] = self.CommandHandleQuitCommand
    handlers["INFO"] = self.CommandHandleInfoCommand
    handlers["SHOW"] = self.CommandHandleShowCommand
    handlers["CONFIG"] = self.CommandHandleConfigCommand
    handlers["CLIENT"] = self.CommandHandleClientCommand
    handlers["FLUSHDB"] = self.CommandHandleFlushDBCommand
    handlers["FLUSHALL"] = self.CommandHandleFlushAllCommand
    handlers["SLAVEOF"] = self.CommandHandleClientSlaveOfCommand
    handlers["REPLSET"] = self.CommandHandleReplsetCommand
    return handlers
}

func (self *Admin) Close() {
    self.closed = true
}

func (self *Admin) CommandHandleShutdownCommand(server_protocol *TextServerProtocol, args []string) error {
    err := server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
    if err != nil {
        return err
    }

    go func() {
        self.slock.Log().Infof("Admin Shutdown Server")
        if self.server != nil {
            self.server.Close()
        }
    }()
    return io.EOF
}

func (self *Admin) CommandHandleBgRewritAaofCommand(server_protocol *TextServerProtocol, args []string) error {
    err := server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
    if err != nil {
        return err
    }

    go func() {
        self.slock.Log().Infof("Aof Rewrite")
        self.slock.GetAof().RewriteAofFile()
    }()
    return nil
}

func (self *Admin) CommandHandleRewriteAofCommand(server_protocol *TextServerProtocol, args []string) error {
    self.slock.GetAof().RewriteAofFile()
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleFlushDBCommand(server_protocol *TextServerProtocol, args []string) error {
    defer self.slock.glock.Unlock()
    self.slock.glock.Lock()

    if len(args) < 2 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Parse Len Error", nil))
    }

    db_id, err := strconv.Atoi(args[1])
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Parse DB_ID Error", nil))
    }
    db := self.slock.dbs[uint8(db_id)]
    if db == nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR No Such DB", nil))
    }

    err = db.FlushDB()
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, fmt.Sprintf("ERR Flush DB Error %s", err.Error()), nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleFlushAllCommand(server_protocol *TextServerProtocol, args []string) error {
    defer self.slock.glock.Unlock()
    self.slock.glock.Lock()

    for db_id, db := range self.slock.dbs {
        err := db.FlushDB()
        if err != nil {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, fmt.Sprintf("ERR Flush DB %d Error %s", db_id, err.Error()), nil))
        }
    }

    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleEchoCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) != 2 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", args[1:]))
}

func (self *Admin) CommandHandlePingCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) > 1 {
        if len(args) != 2 {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
        }
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", args[1:]))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "PONG", nil))
}

func (self *Admin) CommandHandleQuitCommand(server_protocol *TextServerProtocol, args []string) error {
    err := server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
    if err != nil {
        return err
    }
    return io.EOF
}

func (self *Admin) CommandHandleInfoCommand(server_protocol *TextServerProtocol, args []string) error {
    infos := make([]string, 0)

    infos = append(infos, "# Server")
    infos = append(infos, fmt.Sprintf("version:%s", VERSION))
    infos = append(infos, fmt.Sprintf("process_id:%d", os.Getpid()))
    infos = append(infos, fmt.Sprintf("tcp_bind:%s", Config.Bind))
    infos = append(infos, fmt.Sprintf("tcp_port:%d", Config.Port))
    infos = append(infos, fmt.Sprintf("uptime_in_seconds:%d", time.Now().Unix() - self.slock.uptime.Unix()))
    infos = append(infos, fmt.Sprintf("state:%s", STATE_NAMES[self.slock.state]))

    infos = append(infos, "\r\n# Clients")
    infos = append(infos, fmt.Sprintf("total_clients:%d", self.server.connected_count))
    infos = append(infos, fmt.Sprintf("connected_clients:%d", self.server.connecting_count))

    memory_stats := runtime.MemStats{}
    runtime.ReadMemStats(&memory_stats)
    infos = append(infos, "\r\n# Memory")
    infos = append(infos, fmt.Sprintf("used_memory:%d", memory_stats.HeapAlloc))
    infos = append(infos, fmt.Sprintf("used_memory_rss:%d", memory_stats.HeapSys))
    infos = append(infos, fmt.Sprintf("memory_alloc:%d", memory_stats.Alloc))
    infos = append(infos, fmt.Sprintf("memory_total_alloc:%d", memory_stats.TotalAlloc))
    infos = append(infos, fmt.Sprintf("memory_sys:%d", memory_stats.Sys))
    infos = append(infos, fmt.Sprintf("memory_mallocs:%d", memory_stats.Mallocs))
    infos = append(infos, fmt.Sprintf("memory_frees:%d", memory_stats.Frees))
    infos = append(infos, fmt.Sprintf("memory_heap_alloc:%d", memory_stats.HeapAlloc))
    infos = append(infos, fmt.Sprintf("memory_heap_sys:%d", memory_stats.HeapSys))
    infos = append(infos, fmt.Sprintf("memory_heap_idle:%d", memory_stats.HeapIdle))
    infos = append(infos, fmt.Sprintf("memory_heap_released:%d", memory_stats.HeapReleased))
    infos = append(infos, fmt.Sprintf("memory_heap_objects:%d", memory_stats.HeapObjects))
    infos = append(infos, fmt.Sprintf("memory_gc_sys:%d", memory_stats.GCSys))
    infos = append(infos, fmt.Sprintf("memory_gc_last:%d", memory_stats.LastGC))
    infos = append(infos, fmt.Sprintf("memory_gc_next:%d", memory_stats.NextGC))
    infos = append(infos, fmt.Sprintf("memory_gc_pause_totalns:%d", memory_stats.PauseTotalNs))
    infos = append(infos, fmt.Sprintf("memory_gc_num:%d", memory_stats.NumGC))
    infos = append(infos, fmt.Sprintf("memory_gc_num_forced:%d", memory_stats.NumForcedGC))

    db_count := 0
    free_lock_manager_count := 0
    free_lock_count := 0
    free_lock_command_count := 0
    total_command_count := uint64(0)
    for _, db := range self.slock.dbs {
        if db != nil {
            db_count++
            free_lock_manager_head, free_lock_manager_tail := db.free_lock_manager_head, db.free_lock_manager_tail
            if free_lock_manager_head >= free_lock_manager_tail {
                free_lock_manager_count += int(free_lock_manager_head - free_lock_manager_tail)
            } else {
                if free_lock_manager_head < 0x7fffffff && free_lock_manager_tail > 0x7fffffff {
                    free_lock_manager_count += int(0xffffffff - free_lock_manager_tail + free_lock_manager_head)
                }
            }
            for i := int8(0); i < db.manager_max_glocks; i++ {
                free_lock_count += int(db.free_locks[i].Len())
            }
        }
    }

    free_lock_command_count += int(self.slock.free_lock_command_count)
    total_command_count += self.slock.stats_total_command_count
    for _, stream := range self.server.streams {
        if stream.protocol != nil {
            switch stream.protocol.(type) {
            case *BinaryServerProtocol:
                binary_protocol := stream.protocol.(*BinaryServerProtocol)
                free_lock_command_count += int(binary_protocol.free_commands.Len())
                free_lock_command_count += int(binary_protocol.locked_free_commands.Len())
                total_command_count += binary_protocol.total_command_count
            case *TextServerProtocol:
                text_protocol := stream.protocol.(*TextServerProtocol)
                free_lock_command_count += int(text_protocol.free_commands.Len())
                total_command_count += text_protocol.total_command_count
            }
        }
    }

    if self.slock.arbiter_manager != nil {
        infos = append(infos, "\r\n# Arbiter")
        infos = append(infos, fmt.Sprintf("name:%s", self.slock.arbiter_manager.name))
        infos = append(infos, fmt.Sprintf("gid:%s", self.slock.arbiter_manager.gid))
        infos = append(infos, fmt.Sprintf("version:%d", self.slock.arbiter_manager.version))
        infos = append(infos, fmt.Sprintf("vertime:%d", self.slock.arbiter_manager.vertime))
        roles := []string{"unknown", "leader", "follower", "arbiter", "data"}
        for i, member := range self.slock.arbiter_manager.members {
            arbiter, isself, status, aof_id := "no", "no", "offline", member.aof_id
            if member.arbiter != 0 {
                arbiter = "yes"
            }
            if member.isself {
                isself = "yes"
                aof_id = self.slock.arbiter_manager.GetCurrentAofID()
            }
            if member.status == ARBITER_MEMBER_STATUS_ONLINE {
                status = "online"
            }
            infos = append(infos, fmt.Sprintf("member%d:host=%s,weight=%d,arbiter=%s,role=%s,status=%s,self=%s,aof_id=%x,update=%d,delay=%.2f", i + 1, member.host, member.weight,
                arbiter, roles[member.role], status, isself, aof_id, member.last_updated / 1e6, float64(member.last_delay) / 1e6))
        }
    }

    infos = append(infos, "\r\n# Replication")
    if self.slock.state == STATE_LEADER {
        infos = append(infos, "role:leader")
        infos = append(infos, fmt.Sprintf("connected_followers:%d", len(self.slock.replication_manager.server_channels)))
        infos = append(infos, fmt.Sprintf("current_aof_id:%x", self.slock.replication_manager.current_request_id))
        infos = append(infos, fmt.Sprintf("current_offset:%d", self.slock.replication_manager.buffer_queue.current_index))
        for i, server_channel := range self.slock.replication_manager.server_channels {
            if server_channel.protocol == nil {
                continue
            }

            status := "sending"
            if server_channel.pulled == 1 {
                status = "pending"
            }
            infos = append(infos, fmt.Sprintf("follower%d:host=%s,aof_id=%x,behind_offset=%d,status=%s", i + 1,
                server_channel.protocol.RemoteAddr().String(), server_channel.current_request_id,
                self.slock.replication_manager.buffer_queue.current_index - server_channel.buffer_index, status))
        }
    } else {
        infos = append(infos, "role:follower")
        infos = append(infos, fmt.Sprintf("leader_host:%s", self.slock.replication_manager.leader_address))
        if self.slock.replication_manager.client_channel != nil && !self.slock.replication_manager.client_channel.closed {
            infos = append(infos, "leader_link_status:up")
        } else {
            infos = append(infos, "leader_link_status:down")
        }

        if self.slock.replication_manager.client_channel != nil {
            infos = append(infos, fmt.Sprintf("current_aof_id:%x", self.slock.replication_manager.client_channel.current_request_id))
            infos = append(infos, fmt.Sprintf("load_offset:%d", self.slock.replication_manager.client_channel.loaded_count))
        }
    }

    infos = append(infos, "\r\n# Stats")
    infos = append(infos, fmt.Sprintf("db_count:%d", db_count))
    infos = append(infos, fmt.Sprintf("free_command_count:%d", free_lock_command_count))
    infos = append(infos, fmt.Sprintf("free_lock_manager_count:%d", free_lock_manager_count))
    infos = append(infos, fmt.Sprintf("free_lock_count:%d", free_lock_count))
    infos = append(infos, fmt.Sprintf("total_commands_processed:%d", total_command_count))

    aof := self.slock.GetAof()
    infos = append(infos, "\r\n# Persistence")
    infos = append(infos, fmt.Sprintf("aof_channel_count:%d", aof.channel_count))
    infos = append(infos, fmt.Sprintf("aof_channel_active:%d", aof.actived_channel_count))
    infos = append(infos, fmt.Sprintf("aof_count:%d", aof.aof_lock_count))
    if aof.aof_file != nil {
        infos = append(infos, fmt.Sprintf("aof_file_name:%s", aof.aof_file.filename))
        infos = append(infos, fmt.Sprintf("aof_file_size:%d", aof.aof_file.size))
    }

    infos = append(infos, "\r\n# Keyspace")
    for db_id, db := range self.slock.dbs {
        if db != nil {
            db_state:= db.GetState()
            db_infos := make([]string, 0)
            db_infos = append(db_infos, fmt.Sprintf("lock_count=%d", db_state.LockCount))
            db_infos = append(db_infos, fmt.Sprintf("unlock_count=%d", db_state.UnLockCount))
            db_infos = append(db_infos, fmt.Sprintf("locked_count=%d", db_state.LockedCount))
            db_infos = append(db_infos, fmt.Sprintf("wait_count=%d", db_state.WaitCount))
            db_infos = append(db_infos, fmt.Sprintf("timeouted_count=%d", db_state.TimeoutedCount))
            db_infos = append(db_infos, fmt.Sprintf("expried_count=%d", db_state.ExpriedCount))
            db_infos = append(db_infos, fmt.Sprintf("unlock_error_count=%d", db_state.UnlockErrorCount))
            db_infos = append(db_infos, fmt.Sprintf("key_count=%d", db_state.KeyCount))
            if self.slock.state == STATE_LEADER {
                ack_db := self.slock.replication_manager.GetAckDB(uint8(db_id))
                if ack_db != nil && ack_db.locks != nil {
                    if len(ack_db.locks) >= len(ack_db.requests) {
                        db_infos = append(db_infos, fmt.Sprintf("wait_ack_count=%d", len(ack_db.locks)))
                    } else {
                        db_infos = append(db_infos, fmt.Sprintf("wait_ack_count=%d", len(ack_db.requests)))
                    }
                } else {
                    db_infos = append(db_infos,"wait_ack_count=0")
                }
            } else if self.slock.state == STATE_FOLLOWER {
                ack_db := self.slock.replication_manager.GetAckDB(uint8(db_id))
                if ack_db != nil && ack_db.ack_locks != nil {
                    db_infos = append(db_infos, fmt.Sprintf("wait_ack_count=%d", len(ack_db.ack_locks)))
                } else {
                    db_infos = append(db_infos,"wait_ack_count=0")
                }
            } else {
                db_infos = append(db_infos,"wait_ack_count=0")
            }
            infos = append(infos, fmt.Sprintf("db%d:%s", db_id, strings.Join(db_infos, ",")))
        }
    }

    infos = append(infos, "\r\n")

    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", []string{strings.Join(infos, "\r\n")}))
}

func (self *Admin) CommandHandleShowCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 2 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    db_id, err := strconv.Atoi(args[1])
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR DB Id Error", nil))
    }

    db := self.slock.dbs[uint8(db_id)]
    if db == nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR DB Uninit Error", nil))
    }

    if len(args) == 2 {
        return self.CommandHandleShowDBCommand(server_protocol, args, db)
    }
    return self.CommandHandleShowLockCommand(server_protocol, args, db)
}

func (self *Admin) CommandHandleShowDBCommand(server_protocol *TextServerProtocol, args []string, db *LockDB) error {
    db.glock.Lock()
    lock_managers := make([]*LockManager, 0)
    for _, lock_manager := range db.locks {
        if lock_manager.locked > 0 {
            lock_managers = append(lock_managers, lock_manager)
        }
    }
    db.glock.Unlock()

    db_infos := make([]string, 0)
    for _, lock_manager := range lock_managers {
        db_infos = append(db_infos, fmt.Sprintf("%x", lock_manager.lock_key))
        db_infos = append(db_infos, fmt.Sprintf("%d", lock_manager.locked))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", db_infos))
}

func (self *Admin) CommandHandleShowLockCommand(server_protocol *TextServerProtocol, args []string, db *LockDB) error {
    lock_key := [16]byte{}
    server_protocol.ArgsToLockComandParseId(args[2], &lock_key)

    db.glock.Lock()
    lock_manager, ok := db.locks[lock_key]
    db.glock.Unlock()

    if !ok || lock_manager.locked <= 0 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Unknown Lock Manager Error", nil))
    }

    lock_infos := make([]string, 0)
    lock_manager.glock.Lock()
    if lock_manager.current_lock != nil {
        lock := lock_manager.current_lock

        state := uint8(0)
        if lock.timeouted {
            state |= 0x01
        }

        if lock.expried {
            state |= 0x02
        }

        if lock.long_wait_index > 0 {
            state |= 0x04
        }

        if lock.is_aof {
            state |= 0x08
        }

        lock_infos = append(lock_infos, fmt.Sprintf("%x", lock.command.LockId))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.start_time))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.timeout_time))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.expried_time))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.locked))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.aof_time))
        lock_infos = append(lock_infos, fmt.Sprintf("%d", state))
    }

    if lock_manager.lock_maps != nil {
        for _, lock := range lock_manager.lock_maps {
            state := uint8(0)
            if lock.timeouted {
                state |= 0x01
            }

            if lock.expried {
                state |= 0x02
            }

            if lock.long_wait_index > 0 {
                state |= 0x04
            }

            if lock.is_aof {
                state |= 0x08
            }

            lock_infos = append(lock_infos, fmt.Sprintf("%x", lock.command.LockId))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.start_time))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.timeout_time))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.expried_time))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.locked))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", lock.aof_time))
            lock_infos = append(lock_infos, fmt.Sprintf("%d", state))
        }
    }
    lock_manager.glock.Unlock()
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", lock_infos))
}

func (self *Admin) CommandHandleConfigCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 2 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    if strings.ToUpper(args[1]) == "SET" {
        return self.CommandHandleConfigSetCommand(server_protocol, args)
    }
    return self.CommandHandleConfigGetCommand(server_protocol, args)
}

func (self *Admin) CommandHandleConfigGetCommand(server_protocol *TextServerProtocol, args []string) error {
    ConfigValue := reflect.ValueOf(Config).Elem()
    ConfigType := ConfigValue.Type()
    infos := []string{}
    for i := 0; i < ConfigType.NumField(); i++ {
        config_name := strings.ToUpper(ConfigType.Field(i).Tag.Get("long"))
        if len(args) >= 3 && config_name != strings.ToUpper(args[2]) {
            continue
        }

        infos = append(infos, config_name)
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
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Unknown Config Parameter", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", infos))
}

func (self *Admin) CommandHandleConfigSetCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 4 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    switch strings.ToUpper(args[2]) {
    case "DB_LOCK_AOF_TIME":
        db_lock_aof_time, err := strconv.Atoi(args[3])
        if err != nil {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Parameter Value Error", nil))
        }

        Config.DBLockAofTime = uint(db_lock_aof_time)
        for _, db := range self.slock.dbs {
            if db != nil {
                db.aof_time = uint8(db_lock_aof_time)
            }
        }
    case "AOF_FILE_REWRITE_SIZE":
        aof_file_rewrite_size, err := strconv.Atoi(args[3])
        if err != nil {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Parameter Value Error", nil))
        }
        Config.DBLockAofTime = uint(aof_file_rewrite_size)
        self.slock.GetAof().rewrite_size = uint32(aof_file_rewrite_size)
    case "LOG_LEVEL":
        logger := self.slock.Log()
        logging_level := logging.LevelInfo
        switch args[3] {
        case "DEBUG":
            logging_level = logging.LevelDebug
        case "INFO":
            logging_level = logging.LevelInfo
        case "WARNING":
            logging_level = logging.LevelWarning
        case "ERROR":
            logging_level = logging.LevelError
        default:
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Unknown Log Level", nil))
        }
        Config.LogLevel = args[2]
        for _, handler := range logger.GetHandlers() {
            handler.SetLevel(logging_level)
        }
        logger.SetLevel(logging_level)
    default:
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR UnSupport Config Set Parameter", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleClientCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 2 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    if strings.ToUpper(args[1]) == "KILL" {
        return self.CommandHandleClientKillCommand(server_protocol, args)
    }
    return self.CommandHandleClientListCommand(server_protocol, args)
}

func (self *Admin) CommandHandleClientListCommand(server_protocol *TextServerProtocol, args []string) error {
    infos := []string{}
    for _, stream := range self.server.streams {
        protocol_name, client_id, command_count := "", [16]byte{}, uint64(0)
        if stream.protocol != nil {
            switch stream.protocol.(type) {
            case *BinaryServerProtocol:
                binary_protocol := stream.protocol.(*BinaryServerProtocol)
                protocol_name = "binary"
                client_id = binary_protocol.client_id
                command_count += binary_protocol.total_command_count
            case *TextServerProtocol:
                text_protocol := stream.protocol.(*TextServerProtocol)
                protocol_name = "text"
                command_count += text_protocol.total_command_count
            }
        }

        fd := ""
        if tcp_conn, ok := stream.conn.(*net.TCPConn); ok {
            tcp_conn_file, err := tcp_conn.File()
            if err == nil {
                fd = fmt.Sprintf("%d", tcp_conn_file.Fd())
            }
        }
        infos = append(infos, fmt.Sprintf("id=%d addr=%s fd=%s protocol=%s age=%d client_id=%x command_count=%d", stream.stream_id, stream.RemoteAddr().String(),
            fd, protocol_name, time.Now().Unix() - stream.start_time.Unix(), client_id, command_count))
    }
    infos = append(infos, "\r\n")
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "", []string{strings.Join(infos, "\r\n")}))
}

func (self *Admin) CommandHandleClientKillCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 3 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    for _, stream := range self.server.streams {
        if stream.RemoteAddr().String() == args[2] {
            err := stream.Close()
            if err != nil {
                return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Client Close Error", nil))
            }
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
        }
    }

    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR No such client", nil))
}

func (self *Admin) CommandHandleClientSlaveOfCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) == 1 || (len(args) >= 2 && args[1] == "") {
        if self.slock.state == STATE_LEADER {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
        }

        err := self.slock.replication_manager.SwitchToLeader()
        if err != nil {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Change Error", nil))
        }
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
    } else if len(args) >= 3 && args[1] != "" && args[2] != "" {
        if self.slock.state == STATE_FOLLOWER {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
        }

        err := self.slock.replication_manager.SwitchToFollower(fmt.Sprintf("%s:%s", args[1], args[2]))
        if err != nil {
            return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Change Error", nil))
        }
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
    }

    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
}

func (self *Admin) CommandHandleReplsetCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 1 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    if self.slock.arbiter_manager == nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Not Replset server", nil))
    }

    command_name := strings.ToUpper(args[1])
    switch command_name {
    case "CONFIG":
        return self.CommandHandleReplsetConfigCommand(server_protocol, args)
    case "ADD":
        return self.CommandHandleReplsetAddCommand(server_protocol, args)
    case "REMOVE":
        return self.CommandHandleReplsetRemoveCommand(server_protocol, args)
    case "SET":
        return self.CommandHandleReplsetSetCommand(server_protocol, args)
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR unkonwn command", nil))
}

func (self *Admin) CommandHandleReplsetConfigCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 3 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    err := self.slock.arbiter_manager.Config(args[2], 1, 0)
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR config error", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleReplsetAddCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 3 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    err := self.slock.arbiter_manager.AddMember(args[2], 1, 0)
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR add error", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleReplsetRemoveCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 3 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    err := self.slock.arbiter_manager.RemoveMember(args[2])
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR remove error", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}

func (self *Admin) CommandHandleReplsetSetCommand(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 3 {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR Command Arguments Error", nil))
    }

    err := self.slock.arbiter_manager.UpdateMember(args[2], 1, 0)
    if err != nil {
        return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(false, "ERR update error", nil))
    }
    return server_protocol.stream.WriteBytes(server_protocol.parser.BuildResponse(true, "OK", nil))
}