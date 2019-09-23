package server

import (
    "io"
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