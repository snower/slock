package server

import (
    "fmt"
    "io"
    "os"
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
    infos = append(infos, fmt.Sprintf("process_id:%d", os.Getpid()))
    infos = append(infos, fmt.Sprintf("tcp_port:%d", self.server.connected_count))
    infos = append(infos, fmt.Sprintf("uptime_in_seconds:%d", time.Now().Unix() - self.slock.uptime.Unix()))

    infos = append(infos, "\r\n# Clients")
    infos = append(infos, fmt.Sprintf("connected_clients:%d", self.server.connected_count))
    infos = append(infos, fmt.Sprintf("connecting_clients:%d", self.server.connecting_count))

    infos = append(infos, "\r\n# Aof")
    infos = append(infos, fmt.Sprintf("aof_filename:%s", self.slock.GetAof().aof_file.filename))

    infos = append(infos, "\r\n# Keyspace")
    for db_id, db := range self.slock.dbs {
        if db != nil {
            db_state:= db.GetState()
            db_infos := make([]string, 0)
            db_infos = append(db_infos, fmt.Sprintf("LockCount=%d", db_state.LockCount))
            db_infos = append(db_infos, fmt.Sprintf("UnLockCount=%d", db_state.UnLockCount))
            db_infos = append(db_infos, fmt.Sprintf("LockedCount=%d", db_state.LockedCount))
            db_infos = append(db_infos, fmt.Sprintf("WaitCount=%d", db_state.WaitCount))
            db_infos = append(db_infos, fmt.Sprintf("TimeoutedCount=%d", db_state.TimeoutedCount))
            db_infos = append(db_infos, fmt.Sprintf("ExpriedCount=%d", db_state.ExpriedCount))
            db_infos = append(db_infos, fmt.Sprintf("UnlockErrorCount=%d", db_state.UnlockErrorCount))
            db_infos = append(db_infos, fmt.Sprintf("KeyCount=%d", db_state.KeyCount))
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