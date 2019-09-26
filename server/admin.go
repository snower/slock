package server

import (
    "fmt"
    "io"
    "os"
    "runtime"
    "strings"
    "time"
)

type Admin struct {
    slock *SLock
    server *Server
    is_stop bool
}

func NewAdmin() *Admin{
    admin := &Admin{nil,nil,false}
    return admin
}

func (self *Admin) GetHandlers() map[string]TextServerProtocolCommandHandler{
    handlers := make(map[string]TextServerProtocolCommandHandler, 64)
    handlers["SHUTDOWN"] = self.CommandHandleShutdownCommand
    handlers["QUIT"] = self.CommandHandleQuitCommand
    handlers["INFO"] = self.CommandHandleInfoCommand
    return handlers
}

func (self *Admin) Close() {
    self.slock = nil
    self.server = nil
    self.is_stop = true
}

func (self *Admin) CommandHandleShutdownCommand(server_protocol *TextServerProtocol, args []string) error {
    err := server_protocol.stream.WriteAllBytes(server_protocol.parser.Build(true, "OK", nil))
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

func (self *Admin) CommandHandleQuitCommand(server_protocol *TextServerProtocol, args []string) error {
    err := server_protocol.stream.WriteAllBytes(server_protocol.parser.Build(true, "OK", nil))
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
    infos = append(infos, fmt.Sprintf("uptime_seconds:%d", time.Now().Unix() - self.slock.uptime.Unix()))

    infos = append(infos, "\r\n# Clients")
    infos = append(infos, fmt.Sprintf("connected_clients:%d", self.server.connected_count))
    infos = append(infos, fmt.Sprintf("connecting_clients:%d", self.server.connecting_count))

    memory_stats := runtime.MemStats{}
    runtime.ReadMemStats(&memory_stats)
    infos = append(infos, "\r\n# Memory")
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
    for _, db := range self.slock.dbs {
        if db != nil {
            db_count++
            free_lock_manager_count += int(db.free_lock_manager_count) + 1
            for i := int8(0); i < db.manager_max_glocks; i++ {
                free_lock_count += int(db.free_locks[i].Len())
            }
        }
    }
    infos = append(infos, "\r\n# Stats")
    infos = append(infos, fmt.Sprintf("db_count:%d", db_count))
    infos = append(infos, fmt.Sprintf("free_command_count:%d", self.slock.free_lock_command_count))
    infos = append(infos, fmt.Sprintf("free_lock_manager_count:%d", free_lock_manager_count))
    infos = append(infos, fmt.Sprintf("free_lock_count:%d", free_lock_count))

    aof := self.slock.GetAof()
    infos = append(infos, "\r\n# Aof")
    infos = append(infos, fmt.Sprintf("aof_channel_count:%d", aof.channel_count))
    infos = append(infos, fmt.Sprintf("aof_channel_active:%d", aof.actived_channel_count))
    infos = append(infos, fmt.Sprintf("aof_count:%d", aof.aof_lock_count))
    infos = append(infos, fmt.Sprintf("aof_file_name:%s", aof.aof_file.filename))
    infos = append(infos, fmt.Sprintf("aof_file_size:%d", aof.aof_file.size))

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
            infos = append(infos, fmt.Sprintf("db%d:%s", db_id, strings.Join(db_infos, ",")))
        }
    }

    infos = append(infos, "\r\n")

    err := server_protocol.stream.WriteAllBytes(server_protocol.parser.Build(true, "", []string{strings.Join(infos, "\r\n")}))
    if err != nil {
        return err
    }
    return nil
}