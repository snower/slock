package main

import "flag"
import (
    "slock"
    "fmt"
)

func ShowDBStateInfo(host string, port int, db_id uint8)  {
    client := slock.NewClient(host, port)
    err := client.Open()
    if err != nil {
        fmt.Printf("Connect Error: %v", err)
        return
    }

    state := client.SelectDB(uint8(db_id)).State()
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

    lock := slock.NewSLock(*log, *log_level)
    server := slock.NewServer(int(*port), *bind_host, lock)
    err := server.Listen()
    if err != nil {
        lock.Log().Infof("start server listen error: %v", err)
        lock.Log().Info("exited")
        return
    }
    server.Loop()
    lock.Log().Info("exited")
}
