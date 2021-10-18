package client

import (
	"errors"
	"github.com/snower/slock/protocol"
	"io"
)

var subscriberClientIdIndex uint32 = 0

type Subscriber struct {
	client           *Client
	replset          *ReplsetSubscriber
	clientId         uint32
	subscribeId      uint32
	lockKeyMask      [16]byte
	expried          uint32
	maxSize          uint32
	currentVersionId uint32
	currentPublishId uint64
	channel          chan *protocol.LockResultCommand
	closed           bool
	closedWaiter     chan bool
}

func NewSubscriber(client *Client, clientId uint32, subscribeId uint32, lockKeyMask [16]byte, expried uint32, maxSize uint32) *Subscriber {
	return &Subscriber{client, nil, clientId, subscribeId, lockKeyMask, expried, maxSize,
		0, 0, make(chan *protocol.LockResultCommand, 64), false, make(chan bool, 1)}
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

	if self.replset != nil {
		return self.replset.Push(resultCommand)
	}

	publishId := uint64(resultCommand.RequestId[0]) | uint64(resultCommand.RequestId[1])<<8 | uint64(resultCommand.RequestId[2])<<16 | uint64(resultCommand.RequestId[3])<<24 | uint64(resultCommand.RequestId[4])<<32 | uint64(resultCommand.RequestId[5])<<40 | uint64(resultCommand.RequestId[6])<<48 | uint64(resultCommand.RequestId[7])<<56
	versionId := uint32(resultCommand.RequestId[8]) | uint32(resultCommand.RequestId[9])<<8 | uint32(resultCommand.RequestId[10])<<16 | uint32(resultCommand.RequestId[11])<<24
	if publishId < self.currentPublishId && versionId < self.currentVersionId {
		return nil
	}
	self.currentVersionId = versionId
	self.currentPublishId = publishId
	self.channel <- resultCommand
	return nil
}

type ReplsetSubscriber struct {
	client           *ReplsetClient
	subscribers      []*Subscriber
	lockKeyMask      [16]byte
	expried          uint32
	maxSize          uint32
	currentVersionId uint32
	currentPublishId uint64
	channel          chan *protocol.LockResultCommand
	closed           bool
	closedWaiter     chan bool
}

func NewReplsetSubscriber(client *ReplsetClient, lockKeyMask [16]byte, expried uint32, maxSize uint32) *ReplsetSubscriber {
	return &ReplsetSubscriber{client, make([]*Subscriber, 0), lockKeyMask, expried, maxSize,
		0, 0, make(chan *protocol.LockResultCommand, 64), false, make(chan bool, 1)}
}

func (self *ReplsetSubscriber) Close() error {
	return self.client.CloseSubscribe(self)
}

func (self *ReplsetSubscriber) addSubscriber(subscriber *Subscriber) error {
	self.subscribers = append(self.subscribers, subscriber)
	return nil
}

func (self *ReplsetSubscriber) Wait() (*protocol.LockResultCommand, error) {
	if self.closed {
		return nil, io.EOF
	}
	resultCommand := <-self.channel
	if resultCommand == nil {
		return nil, io.EOF
	}
	return resultCommand, nil
}

func (self *ReplsetSubscriber) Push(resultCommand *protocol.LockResultCommand) error {
	if self.closed {
		return errors.New("closed")
	}

	publishId := uint64(resultCommand.RequestId[0]) | uint64(resultCommand.RequestId[1])<<8 | uint64(resultCommand.RequestId[2])<<16 | uint64(resultCommand.RequestId[3])<<24 | uint64(resultCommand.RequestId[4])<<32 | uint64(resultCommand.RequestId[5])<<40 | uint64(resultCommand.RequestId[6])<<48 | uint64(resultCommand.RequestId[7])<<56
	versionId := uint32(resultCommand.RequestId[8]) | uint32(resultCommand.RequestId[9])<<8 | uint32(resultCommand.RequestId[10])<<16 | uint32(resultCommand.RequestId[11])<<24
	if publishId < self.currentPublishId && versionId < self.currentVersionId {
		return nil
	}
	self.currentVersionId = versionId
	self.currentPublishId = publishId
	self.channel <- resultCommand
	return nil
}
