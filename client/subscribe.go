package client

import (
	"errors"
	"github.com/snower/slock/protocol"
	"io"
)

type Subscriber struct {
	client           *Client
	subscribeHost    string
	subscribeId      uint64
	lockKeyMask      [16]byte
	expried          uint32
	maxSize          uint32
	currentPublishId uint64
	channel          chan *protocol.LockResultCommand
	closed           bool
	closedWaiter     chan bool
}

func NewSubscriber(client *Client, subscribeHost string, subscribeId uint64, lockKeyMask [16]byte, expried uint32, maxSize uint32) *Subscriber {
	return &Subscriber{client, subscribeHost, subscribeId, lockKeyMask, expried, maxSize,
		0, make(chan *protocol.LockResultCommand, 64), false, make(chan bool, 1)}
}

func (self *Subscriber) Close() error {
	return self.client.CloseSubscribe(self)
}

func (self *Subscriber) Wait() (*protocol.LockResultCommand, error) {
	if self.closed {
		return nil, io.EOF
	}
	resultCommand := <-self.channel
	if resultCommand == nil {
		return nil, io.EOF
	}
	return resultCommand, nil
}

func (self *Subscriber) Push(resultCommand *protocol.LockResultCommand) error {
	if self.closed {
		return errors.New("closed")
	}

	publishId := uint64(resultCommand.RequestId[0]) | uint64(resultCommand.RequestId[1])<<8 | uint64(resultCommand.RequestId[2])<<16 | uint64(resultCommand.RequestId[3])<<24 | uint64(resultCommand.RequestId[4])<<32 | uint64(resultCommand.RequestId[5])<<40 | uint64(resultCommand.RequestId[6])<<48 | uint64(resultCommand.RequestId[7])<<56
	if publishId < self.currentPublishId {
		return nil
	}
	self.currentPublishId = publishId
	self.channel <- resultCommand
	return nil
}
