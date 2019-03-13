package main

import "flag"
import (
    "fmt"
    "github.com/snower/slock/server"
    "github.com/snower/slock/client"
)

func ShowDBStateInfo(host string, port int, db_id uint8)  {
    slock_client := client.NewClient(host, port)
    err := slock_client.Open()
    if err != nil {
        fmt.Printf("Connect Error: %v", err)
        return
    }

    state := slock_client.SelectDB(uint8(db_id)).State()
    if state.DbState == 0 {
        fmt.Println("Slock DB not used")
    }else{
        fmt.Printf("slock DB ID:\t%d\n", db_id)
        fmt.Printf("LockCount:\t%d\n", state.State.LockCount)
        fmt.Printf("UnLockCount:\t%d\n", state.State.UnLockCount)
        fmt.Printf("LockedCount:\t%d\n", state.State.LockedCount)
        fmt.Printf("WaitCount:\t%d\n", state.State.WaitCount)
        fmt.Printf("TimeoutedCount:\t%d\n", state.State.TimeoutedCount)
        fmt.Printf("ExpriedCount:\t%d\n", state.State.ExpriedCount)
        fmt.Printf("UnlockErrorCount:\t%d\n", state.State.UnlockErrorCount)
        fmt.Printf("KeyCount:\t%d\n", state.State.KeyCount)
        fmt.Println("")
    }
}

func main() {
    port := flag.Int("port", 5658, "port")
    bind_host := flag.String("bind", "0.0.0.0", "bind host")
    client_host := flag.String("host", "127.0.0.1", "client host")
    log := flag.String("log", "-", "log filename")
    log_level := flag.String("log_level", "INFO", "log_level")
    info := flag.Int("info", -1, "show db state info")

    flag.Parse()

    if *info >= 0 {
        ShowDBStateInfo(*client_host, *port, uint8(*info))
        return
    }

    slock := server.NewSLock(*log, *log_level)
    slock_server := server.NewServer(int(*port), *bind_host, slock)
    err := slock_server.Listen()
    if err != nil {
        slock.Log().Infof("start server listen error: %v", err)
        slock.Log().Info("exited")
        return
    }
    slock_server.Loop()
    slock.Log().Info("exited")
}
