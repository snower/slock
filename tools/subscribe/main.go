package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func parseLockKey(argId string) [16]byte {
	lockKey := [16]byte{}
	arg_len := len(argId)
	if arg_len == 16 {
		lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
			lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
			byte(argId[0]), byte(argId[1]), byte(argId[2]), byte(argId[3]), byte(argId[4]), byte(argId[5]), byte(argId[6]),
			byte(argId[7]), byte(argId[8]), byte(argId[9]), byte(argId[10]), byte(argId[11]), byte(argId[12]), byte(argId[13]), byte(argId[14]), byte(argId[15])
	} else if arg_len > 16 {
		if arg_len == 32 {
			v, err := hex.DecodeString(argId)
			if err == nil {
				lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
					lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			} else {
				v := md5.Sum([]byte(argId))
				lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
					lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			}
		} else {
			v := md5.Sum([]byte(argId))
			lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
				lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
				v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
				v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
		}
	} else {
		argIndex := 16 - arg_len
		for i := 0; i < 16; i++ {
			if i < argIndex {
				lockKey[i] = 0
			} else {
				lockKey[i] = argId[i-argIndex]
			}
		}
	}
	return lockKey
}

func subscribe(subscriber *client.Subscriber, finishWaiter chan bool) {
	fmt.Printf("Subscriber start waiting command\n")
	for {
		command, err := subscriber.Wait()
		if err != nil {
			fmt.Printf("Subscriber wait error %v\n", err)
			close(finishWaiter)
			return
		}

		now := time.Now().Format("2006-01-02 15:04:05.999999999-0700")
		publishId := uint64(command.RequestId[0]) | uint64(command.RequestId[1])<<8 | uint64(command.RequestId[2])<<16 | uint64(command.RequestId[3])<<24 | uint64(command.RequestId[4])<<32 | uint64(command.RequestId[5])<<40 | uint64(command.RequestId[6])<<48 | uint64(command.RequestId[7])<<56
		versionId := uint32(command.RequestId[8]) | uint32(command.RequestId[9])<<8 | uint32(command.RequestId[10])<<16 | uint32(command.RequestId[11])<<24
		subscribeId := uint32(command.RequestId[12]) | uint32(command.RequestId[13])<<8 | uint32(command.RequestId[14])<<16 | uint32(command.RequestId[15])<<24
		resultType := "unknown"
		if command.Result == protocol.RESULT_TIMEOUT {
			resultType = "timeout"
		} else if command.Result == protocol.RESULT_EXPRIED {
			resultType = "expried"
		}
		fmt.Printf("%s SubscribeId:%d,PublishId:%d,versionId:%d,DBId:%d,LockKey:%x,LockId:%x,type:%s,Count:%d,LCount:%d,RCount:%d,LRCount:%d\n", now, subscribeId,
			publishId, versionId, command.DbId, command.LockKey, command.LockId, resultType, command.Count, command.Lcount, command.Rcount, command.Lrcount)
	}
}

func main() {
	port := flag.Int("port", 5658, "port")
	host := flag.String("host", "127.0.0.1", "host")
	key := flag.String("key", "", "lock key mask")
	expried := flag.Int("expried", 0, "expried")
	size := flag.Int("size", 67108864, "size")

	flag.Parse()

	c := client.NewClient(*host, uint(*port))
	err := c.Open()
	if err != nil {
		fmt.Printf("Client open error %v\n", err)
		return
	}

	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if *key != "" {
		lockKeyMask = parseLockKey(*key)
	}
	subscriber, err := c.SubscribeMask(lockKeyMask, uint32(*expried), uint32(*size))
	if err != nil {
		fmt.Printf("Client subscribe error %v\n", err)
		return
	}
	fmt.Printf("Client subscribe %x succed\n", lockKeyMask)

	finishWaiter, stopSignal := make(chan bool, 1), make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go subscribe(subscriber.(*client.Subscriber), finishWaiter)
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
