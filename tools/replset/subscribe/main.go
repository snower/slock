package main

import (
	"flag"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"github.com/snower/slock/server"
	"google.golang.org/protobuf/proto"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func subscribe(client *client.Client, finishWaiter chan bool) {
	fmt.Printf("start wait poll arbiter changing\n")
	version, vertime := uint32(0), uint64(0)
	for {
		request := protobuf.ArbiterMemberListRequest{PollTimeout: 5, Version: version, Vertime: vertime}
		data, err := proto.Marshal(&request)
		if err != nil {
			fmt.Printf("poll wait build request error %v\n", err)
			close(finishWaiter)
			return
		}
		command := protocol.NewCallCommand("REPL_MLIST", data)
		commandResult, rerr := client.ExecuteCommand(command, 10)
		if rerr != nil {
			fmt.Printf("poll wait error %v\n", rerr)
			close(finishWaiter)
			return
		}
		callResultCommand, ok := commandResult.(*protocol.CallResultCommand)
		if !ok {
			fmt.Printf("poll wait unkown command result error %v\n", commandResult)
			close(finishWaiter)
			return
		}
		if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
			if callResultCommand.ErrType == "ERR_UNINIT" || callResultCommand.ErrType == "ERR_PTIMEOUT" {
				continue
			}
			fmt.Printf("poll wait command result error %d %v\n", callResultCommand.Result, callResultCommand.ErrType)
			close(finishWaiter)
			return
		}
		response := protobuf.ArbiterMemberListResponse{}
		err = proto.Unmarshal(callResultCommand.Data, &response)
		if err != nil {
			fmt.Printf("poll wait parse response error %v\n", callResultCommand.ErrType)
			close(finishWaiter)
			return
		}
		version, vertime = response.Version, response.Vertime
		if response.Members == nil || len(response.Members) == 0 {
			continue
		}

		fmt.Printf("Replset Member Status: %s\n", time.Now().Format("2006-01-02 15:04:05.999999999-0700"))
		for _, member := range response.Members {
			arbiter, isself, status, abstianed, closed := "no", "no", "offline", "no", "no"
			if member.Arbiter != 0 {
				arbiter = "yes"
			}
			if member.IsSelf {
				isself = "yes"
			}
			if member.Status == server.ARBITER_MEMBER_STATUS_ONLINE {
				status = "online"
			}
			if member.Abstianed {
				abstianed = "yes"
			}
			if member.Closed {
				closed = "yes"
			}
			fmt.Printf("%s Weight:%d,Arbiter:%s,Role:%s,Status:%s,LastUpdated:%d,LastDelay:%.2f,LastError:%d,AofId:%x,IsSelf:%s,Abstianed:%s,Closed:%s\n",
				member.Host, member.Weight, arbiter, server.ROLE_NAMES[member.Role], status, member.LastUpdated/1e6, float64(member.LastDelay)/1e6,
				member.LastError, member.AofId, isself, abstianed, closed)
		}
		fmt.Println()
	}
}

func main() {
	port := flag.Int("port", 5658, "port")
	host := flag.String("host", "127.0.0.1", "host")

	flag.Parse()

	c := client.NewClient(*host, uint(*port))
	err := c.Open()
	if err != nil {
		fmt.Printf("Client open error %v\n", err)
		return
	}

	finishWaiter, stopSignal := make(chan bool, 1), make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go subscribe(c, finishWaiter)
	select {
	case <-finishWaiter:
		err := c.Close()
		if err != nil {
			fmt.Printf("Client close error %v\n", err)
			return
		}
	case <-stopSignal:
		err := c.Close()
		if err != nil {
			fmt.Printf("Client close error %v\n", err)
			return
		}
	case <-c.Unavailable():
		err := c.Close()
		if err != nil {
			fmt.Printf("Client close error %v\n", err)
			return
		}
	}
}
