package main

import (
    "bytes"
    "fmt"
    "os"
    "github.com/jessevdk/go-flags"
    "github.com/snower/slock/server"
    "github.com/snower/slock/client"
    "strings"
)

func ShowDBStateInfo()  {
    type InfoConfig struct{
        Host string         `long:"host" description:"server host" default:"127.0.0.1"`
        Port uint           `long:"port" description:"server port" default:"5658"`
        Db uint             `long:"db" description:"show db id" default:"0"`
    }

    config := &InfoConfig{}
    parse := flags.NewParser(config, flags.Default)
    parse.Usage = "[info]\n\tdefault start slock server\n\tinfo command show db state"
    _, err := parse.ParseArgs(os.Args)
    if err != nil {
        if strings.Contains(err.Error(), "unknown flag") {
            var b bytes.Buffer
            parse.WriteHelp(&b)
            fmt.Println(b.String())
        }
        return
    }

    slock_client := client.NewClient(config.Host, config.Port)
    cerr := slock_client.Open()
    if cerr != nil {
        fmt.Printf("Connect Error: %v", err)
        return
    }

    state := slock_client.SelectDB(uint8(config.Db)).State()
    if state.DbState == 0 {
        fmt.Println("Slock DB not used")
    }else{
        fmt.Printf("slock DB ID:\t%d\n", config.Db)
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
    for _, arg := range os.Args {
        switch arg {
        case "info":
            ShowDBStateInfo()
            return
        }
    }

    config := &server.ServerConfig{}
    parse := flags.NewParser(config, flags.Default)
    parse.Usage = "[info]\n\tdefault start slock server\n\tinfo command show db state"
    _, err := parse.ParseArgs(os.Args)
    if err != nil {
        if strings.Contains(err.Error(), "unknown flag") {
            var b bytes.Buffer
            parse.WriteHelp(&b)
            fmt.Println(b.String())
        }
        return
    }

    slock := server.NewSLock(config)
    slock_server := server.NewServer(slock)
    err = slock.Init(slock_server)
    if err != nil {
        slock.Log().Errorf("Init error %v", err)
        slock.Log().Info("Exited")
        return
    }

    err = slock_server.Listen()
    if err != nil {
        slock.Log().Errorf("Start server listen error %v", err)
        slock.Log().Info("Exited")
        return
    }

    slock_server.Loop()
    slock.Log().Info("Exited")
}
