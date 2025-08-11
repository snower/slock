package server

import (
	"errors"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationBufferQueueItem struct {
	nextItem  *ReplicationBufferQueueItem
	buf       []byte
	data      []byte
	pollCount uint32
	pollIndex uint32
	seq       uint64
}

func NewReplicationBufferQueueItem() *ReplicationBufferQueueItem {
	return &ReplicationBufferQueueItem{nil, make([]byte, 64), nil, 0, 0, 0}
}

func (self *ReplicationBufferQueueItem) Init(buf []byte) {
	self.nextItem = nil
	self.buf = buf
	self.pollCount = 0xffffffff
	self.pollIndex = 0
	self.seq = 0
}

type ReplicationBufferQueueCursor struct {
	currentItem  *ReplicationBufferQueueItem
	currentAofId [16]byte
	buf          []byte
	data         []byte
	seq          uint64
	writed       bool
}

func NewReplicationBufferQueueCursor(buf []byte) *ReplicationBufferQueueCursor {
	return &ReplicationBufferQueueCursor{nil, [16]byte{}, buf, nil, 0xffffffffffffffff, true}
}

type ReplicationBufferQueue struct {
	manager        *ReplicationManager
	glock          *sync.RWMutex
	headItem       *ReplicationBufferQueueItem
	tailItem       *ReplicationBufferQueueItem
	freeHeadItem   *ReplicationBufferQueueItem
	seq            uint64
	usedBufferSize uint64
	bufferSize     uint64
	maxBufferSize  uint64
	pollCount      uint32
	dupCount       uint32
	closed         bool
}

func NewReplicationBufferQueue(manager *ReplicationManager, bufSize uint64, maxSize uint64) *ReplicationBufferQueue {
	queue := &ReplicationBufferQueue{manager, &sync.RWMutex{}, nil,
		nil, nil, 0, 0, bufSize, maxSize,
		0, 0, false}
	queue.InitFreeQueueItems(bufSize / 64)
	return queue
}

func (self *ReplicationBufferQueue) InitFreeQueueItems(count uint64) {
	queueItems := make([]ReplicationBufferQueueItem, count)
	queueItemBuf := make([]byte, count*64)
	for i := uint64(0); i < count; i++ {
		queueItem := &queueItems[i]
		queueItem.Init(queueItemBuf[i*64 : (i+1)*64])
		if self.freeHeadItem != nil {
			queueItem.nextItem = self.freeHeadItem
		}
		self.freeHeadItem = queueItem
	}
}

func (self *ReplicationBufferQueue) AddPoll(cursor *ReplicationBufferQueueCursor) {
	self.glock.Lock()
	self.pollCount++
	currentItem := cursor.currentItem
	for currentItem != nil {
		atomic.AddUint32(&currentItem.pollCount, 1)
		currentItem = currentItem.nextItem
	}
	self.glock.Unlock()
}

func (self *ReplicationBufferQueue) RemovePoll(cursor *ReplicationBufferQueueCursor) {
	self.glock.Lock()
	self.pollCount--
	currentItem := cursor.currentItem
	for currentItem != nil {
		atomic.AddUint32(&currentItem.pollIndex, 1)
		currentItem = currentItem.nextItem
	}
	self.glock.Unlock()
}

func (self *ReplicationBufferQueue) Close() error {
	self.closed = true
	return nil
}

func (self *ReplicationBufferQueue) Push(buf []byte, data []byte) error {
	self.glock.Lock()
	var queueItem *ReplicationBufferQueueItem = nil
	if self.usedBufferSize >= self.bufferSize && self.tailItem != nil {
		if self.tailItem.pollIndex < self.tailItem.pollCount && self.bufferSize < self.maxBufferSize {
			self.InitFreeQueueItems(self.bufferSize / 64)
			self.bufferSize = self.bufferSize * 2
			self.dupCount++
			if self.manager != nil {
				self.manager.slock.logger.Infof("Replication ring buffer duplicate %d %d", self.bufferSize, self.dupCount)
			}
		} else {
			queueItem = self.tailItem
			self.tailItem = self.tailItem.nextItem
			if queueItem.data != nil {
				self.usedBufferSize -= 64
			} else {
				self.usedBufferSize -= uint64(64 + len(queueItem.data))
			}
			if self.usedBufferSize >= self.bufferSize && self.tailItem != nil {
				for self.usedBufferSize >= self.bufferSize && self.tailItem != nil {
					queueItem.data = nil
					queueItem.pollCount = 0xffffffff
					queueItem.pollIndex = 0
					queueItem.seq = 0
					queueItem.nextItem = self.freeHeadItem
					self.freeHeadItem = queueItem

					queueItem = self.tailItem
					self.tailItem = self.tailItem.nextItem
					if queueItem.data != nil {
						self.usedBufferSize -= 64
					} else {
						self.usedBufferSize -= uint64(64 + len(queueItem.data))
					}
				}
			}
		}
	}
	if queueItem == nil {
		if self.freeHeadItem != nil {
			queueItem = self.freeHeadItem
			self.freeHeadItem = self.freeHeadItem.nextItem
		} else {
			queueItem = NewReplicationBufferQueueItem()
		}
	}

	queueItem.nextItem = nil
	copy(queueItem.buf, buf)
	queueItem.data = data
	queueItem.pollCount = self.pollCount
	queueItem.pollIndex = 0
	queueItem.seq = self.seq
	if self.headItem == nil {
		self.headItem = queueItem
		self.tailItem = queueItem
	} else {
		self.headItem.nextItem = queueItem
		self.headItem = queueItem
	}
	if data != nil {
		self.usedBufferSize += 64
	} else {
		self.usedBufferSize += uint64(64 + len(data))
	}
	self.seq++
	self.glock.Unlock()
	return nil
}

func (self *ReplicationBufferQueue) Pop(cursor *ReplicationBufferQueueCursor) error {
	self.glock.RLock()
	currentItem := cursor.currentItem
	if currentItem == nil || currentItem.pollCount == 0xffffffff {
		currentItem = self.tailItem
		if currentItem == nil {
			self.glock.RUnlock()
			return io.EOF
		}
		if currentItem.seq-cursor.seq != 1 && currentItem.seq != 0 && cursor.seq != 0xffffffffffffffff {
			self.glock.RUnlock()
			return errors.New("out of buf")
		}
		cursor.currentItem = currentItem
	} else {
		if currentItem.seq != cursor.seq {
			self.glock.RUnlock()
			return errors.New("out of buf")
		}
		currentItem = currentItem.nextItem
		if currentItem == nil {
			self.glock.RUnlock()
			return io.EOF
		}
		cursor.currentItem = currentItem
	}

	buf := currentItem.buf
	if buf == nil && len(buf) != 64 {
		self.glock.RUnlock()
		return errors.New("out of buf")
	}
	copy(cursor.buf, buf)
	cursor.data = currentItem.data
	cursor.currentAofId[0], cursor.currentAofId[1], cursor.currentAofId[2], cursor.currentAofId[3], cursor.currentAofId[4], cursor.currentAofId[5], cursor.currentAofId[6], cursor.currentAofId[7],
		cursor.currentAofId[8], cursor.currentAofId[9], cursor.currentAofId[10], cursor.currentAofId[11], cursor.currentAofId[12], cursor.currentAofId[13], cursor.currentAofId[14], cursor.currentAofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	cursor.seq = currentItem.seq
	cursor.writed = false
	self.glock.RUnlock()
	return nil
}

func (self *ReplicationBufferQueue) Head(cursor *ReplicationBufferQueueCursor) error {
	self.glock.RLock()
	currentItem := self.headItem
	if currentItem == nil {
		self.glock.RUnlock()
		return errors.New("buffer is empty")
	}

	buf := currentItem.buf
	if buf == nil && len(buf) != 64 {
		self.glock.RUnlock()
		return errors.New("out of buf")
	}
	copy(cursor.buf, buf)
	cursor.data = currentItem.data
	cursor.currentItem = currentItem
	cursor.currentAofId[0], cursor.currentAofId[1], cursor.currentAofId[2], cursor.currentAofId[3], cursor.currentAofId[4], cursor.currentAofId[5], cursor.currentAofId[6], cursor.currentAofId[7],
		cursor.currentAofId[8], cursor.currentAofId[9], cursor.currentAofId[10], cursor.currentAofId[11], cursor.currentAofId[12], cursor.currentAofId[13], cursor.currentAofId[14], cursor.currentAofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	cursor.seq = currentItem.seq
	cursor.writed = true
	self.glock.RUnlock()
	return nil
}

func (self *ReplicationBufferQueue) Search(aofId [16]byte, cursor *ReplicationBufferQueueCursor) error {
	self.glock.RLock()
	currentItem := self.tailItem
	if currentItem == nil {
		self.glock.RUnlock()
		return io.EOF
	}

	for currentItem != nil {
		qbuf := currentItem.buf
		if qbuf == nil || len(qbuf) != 64 {
			currentItem = currentItem.nextItem
			continue
		}
		if aofId[0] != qbuf[3] || aofId[1] != qbuf[4] || aofId[2] != qbuf[5] || aofId[3] != qbuf[6] || aofId[4] != qbuf[7] || aofId[5] != qbuf[8] || aofId[6] != qbuf[9] || aofId[7] != qbuf[10] ||
			aofId[8] != qbuf[11] || aofId[9] != qbuf[12] || aofId[10] != qbuf[13] || aofId[11] != qbuf[14] || aofId[12] != qbuf[15] || aofId[13] != qbuf[16] || aofId[14] != qbuf[17] || aofId[15] != qbuf[18] {
			currentItem = currentItem.nextItem
			continue
		}

		buf := currentItem.buf
		if buf == nil && len(buf) != 64 {
			self.glock.RUnlock()
			return errors.New("out of buf")
		}
		copy(cursor.buf, buf)
		cursor.data = currentItem.data
		cursor.currentItem = currentItem
		cursor.currentAofId[0], cursor.currentAofId[1], cursor.currentAofId[2], cursor.currentAofId[3], cursor.currentAofId[4], cursor.currentAofId[5], cursor.currentAofId[6], cursor.currentAofId[7],
			cursor.currentAofId[8], cursor.currentAofId[9], cursor.currentAofId[10], cursor.currentAofId[11], cursor.currentAofId[12], cursor.currentAofId[13], cursor.currentAofId[14], cursor.currentAofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
		cursor.seq = currentItem.seq
		cursor.writed = true
		self.glock.RUnlock()
		return nil
	}
	self.glock.RUnlock()
	return errors.New("search error")
}

type ReplicationClientState struct {
	connectCount uint64
	loadCount    uint64
	recvCount    uint64
	recvDataSize uint64
	replayCount  uint64
	appendCount  uint64
	pushCount    uint64
	ackCount     uint64
}

type ReplicationClient struct {
	manager         *ReplicationManager
	glock           *sync.Mutex
	stream          *client.Stream
	protocol        *client.BinaryClientProtocol
	aof             *Aof
	aofLock         *AofLock
	currentAofId    [16]byte
	replayAofIndex  uint32
	rbufs           []*AofLock
	rbufIndex       int
	replayQueue     chan *AofLock
	aofQueue        chan *AofLock
	pushQueue       chan *AofLock
	state           *ReplicationClientState
	appendWaiter    chan bool
	replayWaiter    chan bool
	pushWaiter      chan bool
	wakeupSignal    chan bool
	closedWaiter    chan bool
	closed          bool
	connectedLeader bool
	recvedFiles     bool
}

func NewReplicationClient(manager *ReplicationManager) *ReplicationClient {
	state := &ReplicationClientState{0, 0, 0, 0, 0, 0, 0, 0}
	channel := &ReplicationClient{manager, &sync.Mutex{}, nil, nil, manager.slock.GetAof(),
		nil, [16]byte{}, 0, make([]*AofLock, 256), 0, make(chan *AofLock, 64),
		make(chan *AofLock, 64), make(chan *AofLock, 64), state, nil, nil, nil,
		nil, make(chan bool, 1), false, true, false}
	for i := 0; i < len(channel.rbufs); i++ {
		channel.rbufs[i] = NewAofLock()
	}
	return channel
}

func (self *ReplicationClient) Open(addr string) error {
	self.glock.Lock()
	if self.protocol != nil {
		self.glock.Unlock()
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		self.glock.Unlock()
		return err
	}
	stream := client.NewStream(conn)
	clientProtocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.protocol = clientProtocol
	self.closed = false
	self.glock.Unlock()
	return nil
}

func (self *ReplicationClient) Close() error {
	self.glock.Lock()
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	self.glock.Unlock()
	_ = self.WakeupRetryConnect()
	self.manager.slock.logger.Infof("Replication client %s close", self.manager.leaderAddress)
	return nil
}

func (self *ReplicationClient) End() error {
	self.glock.Lock()
	self.closed = true
	if !self.recvedFiles && self.protocol != nil {
		_ = self.protocol.Close()
	}
	self.glock.Unlock()
	_ = self.WakeupRetryConnect()
	self.manager.slock.logger.Infof("Replication client %s end", self.manager.leaderAddress)
	return nil
}

func (self *ReplicationClient) Run() {
	self.currentAofId = self.manager.currentAofId
	for !self.closed {
		self.manager.slock.logger.Infof("Replication client connect leader %s", self.manager.leaderAddress)
		err := self.Open(self.manager.leaderAddress)
		if err != nil {
			self.manager.slock.logger.Errorf("Replication client connect leader %s error %v", self.manager.leaderAddress, err)
			if self.protocol != nil {
				_ = self.protocol.Close()
			}
			self.glock.Lock()
			self.stream = nil
			self.protocol = nil
			self.glock.Unlock()
			self.manager.wakeupInitSyncedWaiters()
			if self.closed {
				break
			}
			_ = self.sleepWhenRetryConnect()
			continue
		}
		self.state.connectCount++

		err = self.InitSync()
		if err != nil {
			if err != io.EOF {
				self.manager.slock.logger.Errorf("Replication client init sync error %s %v", self.manager.leaderAddress, err)
			}
		} else {
			self.manager.clientSycnInited()
			self.manager.slock.logger.Infof("Replication client connected leader %s", self.manager.leaderAddress)
			err = self.Process()
			if err != nil {
				if err != io.EOF && !self.closed {
					self.manager.slock.logger.Errorf("Replication client sync leader %s error %v", self.manager.leaderAddress, err)
				}
			}
		}

		self.glock.Lock()
		appendWaiter := self.appendWaiter
		if appendWaiter != nil {
			self.glock.Unlock()
			<-appendWaiter
			self.glock.Lock()
		}
		replayWaiter := self.replayWaiter
		if replayWaiter != nil {
			self.glock.Unlock()
			<-replayWaiter
			self.glock.Lock()
		}
		pushWaiter := self.pushWaiter
		if pushWaiter != nil {
			self.glock.Unlock()
			<-pushWaiter
			self.glock.Lock()
		}

		if self.protocol != nil {
			_ = self.protocol.Close()
		}
		self.stream = nil
		self.protocol = nil
		self.glock.Unlock()
		self.manager.wakeupInitSyncedWaiters()
		if self.closed {
			break
		}
		_ = self.sleepWhenRetryConnect()
	}

	close(self.closedWaiter)
	self.manager.clientChannel = nil
	self.manager.currentAofId = self.currentAofId
	self.manager.slock.logger.Infof("Replication client connect leader %s closed", self.manager.leaderAddress)
}

func (self *ReplicationClient) sendSyncCommand() (*protobuf.SyncResponse, error) {
	aofId := FormatAofId(self.currentAofId)
	if aofId != "00000000000000000000000000000000" {
		if self.aofLock == nil {
			self.aofLock = NewAofLock()
		}
		self.manager.slock.logger.Infof("Replication client send start sync by aofId %s", aofId)
	} else {
		aofId = ""
		self.manager.slock.logger.Infof("Replication client send start sync")
	}

	request := protobuf.SyncRequest{AofId: aofId}
	data, err := proto.Marshal(&request)
	if err != nil {
		return nil, err
	}
	command := protocol.NewCallCommand("SYNC", data)
	werr := self.protocol.Write(command)
	if werr != nil {
		return nil, werr
	}

	resultCommand, rerr := self.protocol.Read()
	if rerr != nil {
		return nil, rerr
	}

	callResultCommand, ok := resultCommand.(*protocol.CallResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		if callResultCommand.Result == 0 && callResultCommand.ErrType == "ERR_NOT_FOUND" {
			self.currentAofId = [16]byte{}
			self.manager.slock.logger.Infof("Replication client resend file sync all data")
			self.aofLock = nil
			self.recvedFiles = false
			return self.sendSyncCommand()
		}
		return nil, errors.New(callResultCommand.ErrType)
	}

	response := protobuf.SyncResponse{}
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, errors.New("unknown lastest aofId")
	}
	self.manager.slock.logger.Infof("Replication client recv start sync from aofId %s", response.AofId)
	return &response, nil
}

func (self *ReplicationClient) InitSync() error {
	syncResponse, err := self.sendSyncCommand()
	if err != nil {
		return err
	}

	if self.aofLock != nil {
		err = self.aof.Load()
		if err != nil {
			rerr := self.aof.Reset(1, 0)
			if rerr != nil {
				return err
			}
			rerr = self.manager.FlushDB()
			if rerr != nil {
				return err
			}
			self.currentAofId = [16]byte{}
			self.manager.currentAofId = self.currentAofId
			return err
		}
		err = self.sendStarted()
		if err != nil {
			return err
		}
		self.recvedFiles = true
		self.manager.slock.logger.Infof("Replication client start sync, waiting from aofId %s", FormatAofId(self.currentAofId))
		return nil
	}

	aofId, err := ParseAofId(syncResponse.AofId)
	if err != nil {
		return err
	}
	aofFileIndex := uint32(aofId[4]) | uint32(aofId[5])<<8 | uint32(aofId[6])<<16 | uint32(aofId[7])<<24
	aofFileOffset := uint32(aofId[0]) | uint32(aofId[1])<<8 | uint32(aofId[2])<<16 | uint32(aofId[3])<<24
	err = self.aof.Reset(aofFileIndex, aofFileOffset)
	if err != nil {
		return err
	}
	err = self.manager.FlushDB()
	if err != nil {
		return err
	}

	self.aofLock = NewAofLock()
	err = self.sendStarted()
	if err != nil {
		return err
	}

	self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
		self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
		aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15]
	self.manager.slock.logger.Infof("Replication client start recv files util aofId %s", FormatAofId(self.currentAofId))
	return self.recvFiles()
}

func (self *ReplicationClient) sendStarted() error {
	aofLock := NewAofLock()
	aofLock.CommandType = protocol.COMMAND_INIT
	aofLock.AofIndex = 0xffffffff
	aofLock.AofOffset = 0xffffffff
	aofLock.CommandTime = 0xffffffffffffffff
	err := aofLock.Encode()
	if err != nil {
		return err
	}
	self.glock.Lock()
	err = self.stream.WriteBytes(aofLock.buf)
	self.glock.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationClient) recvFiles() error {
	defer func() {
		self.aof.glock.Lock()
		self.aof.isRewriting = false
		if self.aof.rewritedWaiter != nil {
			close(self.aof.rewritedWaiter)
			self.aof.rewritedWaiter = nil
		}
		self.aof.glock.Unlock()
	}()

	self.aof.glock.Lock()
	for self.aof.isRewriting {
		self.aof.glock.Unlock()
		_ = self.aof.WaitRewriteAofFiles()
		self.aof.glock.Lock()
	}
	self.aof.isRewriting = true
	self.aof.glock.Unlock()

	var aofFile *AofFile = nil
	aofIndex := uint32(0)
	for !self.closed {
		err := self.readLock()
		if err != nil {
			if aofFile != nil && aofFile != self.aof.aofFile {
				_ = aofFile.Close()
			}
			return err
		}

		if self.aofLock.CommandType == protocol.COMMAND_INIT && self.aofLock.AofIndex == 0xffffffff && self.aofLock.AofOffset == 0xffffffff && self.aofLock.CommandTime == 0xffffffffffffffff {
			if aofFile != nil {
				err = aofFile.Flush()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client flush aof file %s error %v", aofFile.filename, err)
				}
				if aofFile != self.aof.aofFile {
					err = aofFile.Close()
					if err != nil {
						self.manager.slock.logger.Errorf("Replication client close aof file %s error %v", aofFile.filename, err)
						return err
					}
				}
			}
			self.recvedFiles = true
			self.manager.slock.logger.Infof("Replication client recv files finish, current aofId %s", FormatAofId(self.currentAofId))
			return nil
		}

		currentAofIndex := self.aofLock.AofIndex
		if currentAofIndex != aofIndex || aofFile == nil {
			if aofFile != nil {
				err = aofFile.Flush()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client flush aof file %s error %v", aofFile.filename, err)
				}
				if aofFile != self.aof.aofFile {
					err = aofFile.Close()
					if err != nil {
						self.manager.slock.logger.Errorf("Replication client close aof file %s error %v", aofFile.filename, err)
						return err
					}
				}
			}

			if currentAofIndex == self.aof.aofFileIndex && self.aof.aofFile != nil {
				aofFile = self.aof.aofFile
			} else {
				aofFile, err = self.aof.OpenAofFile(currentAofIndex)
				if err != nil {
					return err
				}
			}
			aofIndex = currentAofIndex
			if aofIndex == 0 {
				self.manager.slock.logger.Infof("Replication client recv file rewrite.aof")
			} else {
				self.manager.slock.logger.Infof(fmt.Sprintf("Replication client recv file %s.%d", "append.aof", aofIndex))
			}
		}

		err = self.aof.LoadLock(self.aofLock)
		if err != nil {
			if aofFile != nil && aofFile != self.aof.aofFile {
				_ = aofFile.Close()
			}
			return err
		}
		err = aofFile.AppendLock(self.aofLock)
		if err != nil {
			if aofFile != nil && aofFile != self.aof.aofFile {
				_ = aofFile.Close()
			}
			return err
		}
		if self.aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			err = aofFile.WriteLockData(self.aofLock)
			if err != nil {
				if aofFile != nil && aofFile != self.aof.aofFile {
					_ = aofFile.Close()
				}
				return err
			}
		}
		self.state.loadCount++

		buf := self.aofLock.buf
		self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
			self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}

	if aofFile != nil && aofFile != self.aof.aofFile {
		_ = aofFile.Close()
	}
	return io.EOF
}

func (self *ReplicationClient) readLock() error {
	buf := self.aofLock.buf
	n, err := self.stream.ReadBytes(buf)
	if err != nil {
		return err
	}
	if n != 64 {
		return errors.New("read buf size error")
	}

	err = self.aofLock.Decode()
	if err != nil {
		return err
	}
	if self.aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		buf, err = self.stream.ReadBytesFrame()
		if err != nil {
			return err
		}
		self.aofLock.data = buf
	}
	return nil
}

func (self *ReplicationClient) Process() error {
	go self.ProcessReplayLock()
	go self.ProcessAofAppend()
	go self.ProcessPushAofLock()

	for !self.closed {
		aofLock := self.rbufs[self.rbufIndex]
		n, err := self.stream.ReadBytes(aofLock.buf)
		if err != nil || n != 64 {
			self.replayQueue <- nil
			self.aofQueue <- nil
			self.pushQueue <- nil
			if err == nil {
				return errors.New("read stream size error")
			}
			return err
		}
		err = aofLock.Decode()
		if err != nil {
			self.replayQueue <- nil
			self.aofQueue <- nil
			self.pushQueue <- nil
			return err
		}
		if aofLock.CommandType == protocol.COMMAND_QUIT {
			self.replayQueue <- nil
			self.aofQueue <- nil
			self.pushQueue <- nil
			self.manager.slock.logger.Infof("Replication client recv quit command")
			return nil
		}
		if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			buf, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				self.replayQueue <- nil
				self.aofQueue <- nil
				self.pushQueue <- nil
				return derr
			}
			aofLock.data = buf
			self.state.recvDataSize += uint64(len(buf))
		}

		self.state.recvCount++
		self.replayQueue <- aofLock
		self.aofQueue <- aofLock
		self.pushQueue <- aofLock
		self.state.loadCount++
		self.rbufIndex++
		if self.rbufIndex >= len(self.rbufs) {
			self.rbufIndex = 0
		}
	}
	self.replayQueue <- nil
	self.aofQueue <- nil
	self.pushQueue <- nil
	return nil
}

func (self *ReplicationClient) ProcessReplayLock() {
	self.glock.Lock()
	if self.replayWaiter != nil {
		close(self.replayWaiter)
	}
	self.replayWaiter = make(chan bool)
	self.glock.Unlock()
	defer func() {
		self.glock.Lock()
		if self.replayWaiter != nil {
			close(self.replayWaiter)
			self.replayWaiter = nil
		}
		self.glock.Unlock()
	}()

	aof := self.aof
	self.replayAofIndex = aof.aofFileIndex
	for !self.closed {
		aofLock := <-self.replayQueue
		if aofLock == nil {
			return
		}
		err := aof.ReplayLock(aofLock)
		if err == nil && aofLock.AofIndex != self.replayAofIndex {
			self.glock.Lock()
			if aof.isWaitRewite {
				_ = aof.ExecuteConsistencyBarrierCommand(0)
				aof.isWaitRewite = false
				self.manager.slock.Log().Infof("Replication ready wait aof execute rewrite")
			}
			self.glock.Unlock()
		}
		self.replayAofIndex = aofLock.AofIndex
		self.state.replayCount++
	}
}

func (self *ReplicationClient) ProcessAofAppend() {
	self.glock.Lock()
	if self.appendWaiter != nil {
		close(self.appendWaiter)
	}
	self.appendWaiter = make(chan bool)
	self.glock.Unlock()
	defer func() {
		self.glock.Lock()
		if self.appendWaiter != nil {
			close(self.appendWaiter)
			self.appendWaiter = nil
		}
		self.glock.Unlock()
	}()

	aof := self.aof
	aofId := [16]byte{self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
		self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15]}
	aofLock := <-self.aofQueue
	for !self.closed {
		if aofLock == nil {
			aof.FlushWithLocked()
			self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
				self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
				aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15]
			return
		}
		if aof.AppendLock(aofLock) {
			self.glock.Lock()
			if self.replayAofIndex <= aof.aofFileIndex {
				aof.ExecuteConsistencyBarrierCommand(0)
				aof.isWaitRewite = false
				self.manager.slock.Log().Infof("Replication ready wait aof execute rewrite")
			}
			self.glock.Unlock()
		}
		self.state.appendCount++
		buf := aofLock.buf
		if len(buf) >= 64 {
			aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
				aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
				buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
		}

		select {
		case aofLock = <-self.aofQueue:
			continue
		default:
			if self.closed {
				aof.FlushWithLocked()
				self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
					self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
					aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15]
				return
			}
			aofFile := aof.aofFile
			if aofFile != nil && (aofFile.windex > 0 || aofFile.ackIndex > 0) {
				aof.aofGlock.Lock()
				err := aofFile.Flush()
				if err != nil {
					self.manager.slock.Log().Errorf("Replication flush file error %v", err)
				}
				aof.aofGlock.Unlock()
				self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
					self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
					aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15]
			}

			select {
			case aofLock = <-self.aofQueue:
				continue
			case <-time.After(200 * time.Millisecond):
				aof.FlushWithLocked()
				self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
					self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = aofId[0], aofId[1], aofId[2], aofId[3], aofId[4], aofId[5], aofId[6], aofId[7],
					aofId[8], aofId[9], aofId[10], aofId[11], aofId[12], aofId[13], aofId[14], aofId[15]
				aofLock = <-self.aofQueue
				if aofLock == nil {
					return
				}
			}
		}
	}
}

func (self *ReplicationClient) ProcessPushAofLock() {
	self.glock.Lock()
	if self.pushWaiter != nil {
		close(self.pushWaiter)
	}
	self.pushWaiter = make(chan bool)
	self.glock.Unlock()
	defer func() {
		self.glock.Lock()
		if self.pushWaiter != nil {
			close(self.pushWaiter)
			self.pushWaiter = nil
		}
		self.glock.Unlock()
	}()

	bufferQueue := self.manager.bufferQueue
	for !self.closed {
		aofLock := <-self.pushQueue
		if aofLock == nil {
			return
		}
		if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			_ = bufferQueue.Push(aofLock.buf, aofLock.data)
		} else {
			_ = bufferQueue.Push(aofLock.buf, nil)
		}
		self.state.pushCount++
		_ = self.manager.WakeupServerChannel()
	}
}

func (self *ReplicationClient) HandleAcked(ackLock *ReplicationAckLock) error {
	if ackLock.aofResult != 0 && ackLock.lockResult.Result == 0 {
		ackLock.lockResult.Result = protocol.RESULT_ERROR
	}
	err := ackLock.lockResult.Encode(ackLock.buf)
	if err != nil {
		return err
	}
	self.glock.Lock()
	if self.stream == nil {
		self.glock.Unlock()
		return errors.New("stream closed")
	}
	err = self.stream.WriteBytes(ackLock.buf)
	if err != nil {
		self.glock.Unlock()
		return err
	}
	if ackLock.lockResult.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
		err = self.stream.WriteBytes(ackLock.lockResult.Data.Data)
		if err != nil {
			self.glock.Unlock()
			return err
		}
	}
	self.state.ackCount++
	self.glock.Unlock()
	return nil
}

func (self *ReplicationClient) sleepWhenRetryConnect() error {
	self.glock.Lock()
	self.wakeupSignal = make(chan bool, 1)
	self.glock.Unlock()

	select {
	case <-self.wakeupSignal:
		return nil
	case <-time.After(5 * time.Second):
		self.glock.Lock()
		self.wakeupSignal = nil
		self.glock.Unlock()
		return nil
	}
}

func (self *ReplicationClient) WakeupRetryConnect() error {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
	return nil
}

type ReplicationServerState struct {
	pushCount    uint64
	sendCount    uint64
	sendDataSize uint64
	ackCount     uint64
}

type ReplicationServer struct {
	manager        *ReplicationManager
	glock          *sync.Mutex
	stream         *Stream
	protocol       *BinaryServerProtocol
	aof            *Aof
	raofLock       *AofLock
	waofLock       *AofLock
	bufferCursor   *ReplicationBufferQueueCursor
	state          *ReplicationServerState
	pulledState    uint32
	pulledWaiter   chan bool
	wakeupedBuffer bool
	closed         bool
	closedWaiter   chan bool
	sendedFiles    bool
}

func NewReplicationServer(manager *ReplicationManager, serverProtocol *BinaryServerProtocol) *ReplicationServer {
	waofLock := NewAofLock()
	state := &ReplicationServerState{0, 0, 0, 0}
	return &ReplicationServer{manager, &sync.Mutex{}, serverProtocol.stream, serverProtocol,
		manager.slock.GetAof(), NewAofLock(), waofLock, NewReplicationBufferQueueCursor(waofLock.buf),
		state, 0, make(chan bool, 1), false, false, make(chan bool, 1), false}
}

func (self *ReplicationServer) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	self.glock.Unlock()
	self.manager.slock.Log().Infof("Replication server %s close", self.protocol.RemoteAddr().String())
	return nil
}

func (self *ReplicationServer) handleInitSync(command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.manager.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, 0, "ERR_STATE", nil), nil
	}

	request := protobuf.SyncRequest{}
	err := proto.Unmarshal(command.Data, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_PROTO", nil), nil
	}

	if request.AofId == "" {
		err = self.manager.bufferQueue.Head(self.bufferCursor)
		if err != nil {
			self.waofLock.AofIndex = self.aof.aofFileIndex
			self.waofLock.AofOffset = self.aof.aofFileOffset
		} else {
			self.waofLock.buf = self.bufferCursor.buf
			err = self.waofLock.Decode()
			if err != nil {
				return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
			}
		}
		aofId := FormatAofId(self.waofLock.GetAofId())
		response := protobuf.SyncResponse{AofId: aofId}
		data, derr := proto.Marshal(&response)
		if derr != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		err = self.protocol.Write(protocol.NewCallResultCommand(command, 0, "", data))
		if err != nil {
			return nil, err
		}
		self.manager.slock.logger.Infof("Replication server recv client %s send files start by aofId %s", self.protocol.RemoteAddr().String(), aofId)
		err = self.waitStarted()
		return nil, err
	}

	self.manager.slock.logger.Infof("Replication server recv client %s sync require start by aofId %s", self.protocol.RemoteAddr().String(), request.AofId)
	initedAofId, err := ParseAofId(request.AofId)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_AOF_ID", nil), nil
	}
	if initedAofId[4] == 0 && initedAofId[5] == 0 && initedAofId[6] == 0 && initedAofId[7] == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_NOT_FOUND", nil), nil
	}
	var aofId string
	serr := self.manager.bufferQueue.Search(initedAofId, self.bufferCursor)
	if serr != nil {
		if initedAofId != self.manager.currentAofId {
			return protocol.NewCallResultCommand(command, 0, "ERR_NOT_FOUND", nil), nil
		}
		self.bufferCursor.currentAofId = self.manager.currentAofId
		self.bufferCursor.currentItem = nil
		self.bufferCursor.seq = self.manager.bufferQueue.seq - 1
		self.bufferCursor.writed = true
		aofId = FormatAofId(initedAofId)
	} else {
		self.waofLock.buf = self.bufferCursor.buf
		err = self.waofLock.Decode()
		if err != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		aofId = FormatAofId(self.waofLock.GetAofId())
	}

	response := protobuf.SyncResponse{AofId: aofId}
	data, err := proto.Marshal(&response)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	err = self.protocol.Write(protocol.NewCallResultCommand(command, 0, "", data))
	if err != nil {
		return nil, err
	}
	self.manager.slock.logger.Infof("Replication server handle client %s send start by aofId %s", self.protocol.RemoteAddr().String(), aofId)
	err = self.waitStarted()
	self.sendedFiles = true
	return nil, err
}

func (self *ReplicationServer) sendFiles() error {
	_ = self.aof.WaitRewriteAofFiles()
	self.aof.FlushWithLocked()

	appendFiles, rewriteFile, err := self.aof.FindAofFiles()
	if err != nil {
		return err
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	err, laofLock := self.aof.LoadAofFiles(aofFilenames, time.Now().Unix(), func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		if lock.AofIndex > self.waofLock.AofIndex && lock.AofOffset > self.waofLock.AofOffset {
			return false, nil
		}

		self.glock.Lock()
		err = self.stream.WriteBytes(lock.buf)
		if err != nil {
			self.glock.Unlock()
			return true, err
		}
		if lock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
			err = self.stream.WriteBytes(lock.data)
			if err != nil {
				self.glock.Unlock()
				return true, err
			}
		}
		self.glock.Unlock()
		self.state.pushCount++
		return true, nil
	})
	if err != nil {
		return err
	}

	err = self.sendFilesFinished()
	if err != nil {
		return err
	}
	if laofLock != nil {
		self.bufferCursor.currentAofId = laofLock.GetAofId()
	}
	if laofLock != nil && laofLock.AofIndex >= self.waofLock.AofIndex && laofLock.AofOffset >= self.waofLock.AofOffset && laofLock.CommandTime >= self.waofLock.CommandTime {
		self.manager.slock.logger.Infof("Replication server handle client %s send file finish, send queue by aofId %s",
			self.protocol.RemoteAddr().String(), FormatAofId(self.bufferCursor.currentAofId))
	} else {
		self.manager.slock.logger.Infof("Replication server handle client %s send file finish, send queue by aofId %s",
			self.protocol.RemoteAddr().String(), FormatAofId(self.waofLock.GetAofId()))
	}
	return self.manager.WakeupServerChannel()
}

func (self *ReplicationServer) waitStarted() error {
	buf := self.raofLock.buf
	for !self.closed {
		n, err := self.stream.ReadBytes(buf)
		if err != nil {
			return err
		}
		if n != 64 {
			return errors.New("read size error")
		}

		err = self.raofLock.Decode()
		if err != nil {
			return err
		}

		if self.raofLock.CommandType == protocol.COMMAND_INIT && self.raofLock.AofIndex == 0xffffffff &&
			self.raofLock.AofOffset == 0xffffffff && self.raofLock.CommandTime == 0xffffffffffffffff {
			return nil
		}
	}
	return io.EOF
}

func (self *ReplicationServer) sendFilesFinished() error {
	aofLock := NewAofLock()
	aofLock.CommandType = protocol.COMMAND_INIT
	aofLock.AofIndex = 0xffffffff
	aofLock.AofOffset = 0xffffffff
	aofLock.CommandTime = 0xffffffffffffffff
	err := aofLock.Encode()
	if err != nil {
		return err
	}
	self.glock.Lock()
	err = self.stream.WriteBytes(aofLock.buf)
	if err != nil {
		self.glock.Unlock()
		return err
	}
	self.glock.Unlock()
	self.sendedFiles = true
	return nil
}

func (self *ReplicationServer) SendProcess() error {
	atomic.AddUint32(&self.manager.serverActiveCount, 1)
	if !self.sendedFiles {
		err := self.sendFiles()
		if err != nil {
			atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
			self.manager.slock.logger.Infof("Replication server handle client %s send files error %v", self.protocol.RemoteAddr().String(), err)
			_ = self.Close()
			return err
		}
	}

	bufferQueue := self.manager.bufferQueue
	for !self.closed {
		if !self.bufferCursor.writed {
			self.glock.Lock()
			err := self.stream.WriteBytes(self.bufferCursor.buf)
			if err != nil {
				self.glock.Unlock()
				atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
				return err
			}
			if self.bufferCursor.data != nil {
				err = self.stream.WriteBytes(self.bufferCursor.data)
				if err != nil {
					self.glock.Unlock()
					atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
					return err
				}
				self.state.sendDataSize += uint64(len(self.bufferCursor.data))
			}
			self.glock.Unlock()
			self.bufferCursor.writed = true
			atomic.AddUint32(&self.bufferCursor.currentItem.pollIndex, 1)
			self.state.pushCount++
			self.state.sendCount++
		}

		err := bufferQueue.Pop(self.bufferCursor)
		if err != nil {
			if err != io.EOF {
				atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
				return err
			}

			if !atomic.CompareAndSwapUint32(&self.pulledState, 0, 2) {
				if atomic.CompareAndSwapUint32(&self.pulledState, 1, 0) {
					<-self.pulledWaiter
					continue
				}
			}
			atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
			if atomic.CompareAndSwapUint32(&self.manager.serverActiveCount, 0, 0) {
				self.manager.glock.Lock()
				if self.manager.serverFlushWaiter != nil {
					close(self.manager.serverFlushWaiter)
					self.manager.serverFlushWaiter = nil
				}
				self.manager.glock.Unlock()
			}
			err = bufferQueue.Pop(self.bufferCursor)
			if err != nil {
				if err != io.EOF {
					atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
					return err
				}
				<-self.pulledWaiter
				atomic.CompareAndSwapUint32(&self.pulledState, 1, 0)
			}
			atomic.AddUint32(&self.manager.serverActiveCount, 1)
			continue
		}
	}
	atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
	return nil
}

func (self *ReplicationServer) RecvProcess() error {
	lockResult := &protocol.LockResultCommand{}
	for !self.closed {
		buf, err := self.stream.ReadBytesSize(64)
		if err != nil {
			return err
		}
		if buf == nil || len(buf) != 64 {
			return errors.New("read size error")
		}

		err = lockResult.Decode(buf)
		if err != nil {
			return err
		}
		if lockResult.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			buf, err = self.stream.ReadBytesFrame()
			if err != nil {
				return err
			}
			lockResult.Data = protocol.NewLockResultCommandDataFromOriginBytes(buf)
		}
		err = self.aof.loadLockAck(lockResult)
		if err != nil {
			return err
		}
		self.state.ackCount++
	}
	return io.EOF
}

func (self *ReplicationServer) SendQuit() error {
	if self.stream == nil {
		return nil
	}
	aofLock := AofLock{CommandType: protocol.COMMAND_QUIT, buf: make([]byte, 64)}
	err := aofLock.Encode()
	if err != nil {
		return err
	}
	self.glock.Lock()
	err = self.stream.WriteBytes(aofLock.buf)
	if err != nil {
		self.glock.Unlock()
		return err
	}
	self.glock.Unlock()
	return nil
}

type ReplicationAckLock struct {
	buf        []byte
	lockResult *protocol.LockResultCommand
	aofResult  uint8
	locked     bool
	aofed      bool
}

func NewReplicationAckLock() *ReplicationAckLock {
	resultCommand := protocol.ResultCommand{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: 0, RequestId: [16]byte{}, Result: 0}
	lockResult := &protocol.LockResultCommand{ResultCommand: resultCommand, Flag: 0, DbId: 0, LockId: [16]byte{}, LockKey: [16]byte{},
		Count: 0, Lcount: 0, Lrcount: 0, Rcount: 0, Blank: protocol.RESULT_LOCK_COMMAND_BLANK_BYTERS}
	return &ReplicationAckLock{make([]byte, 64), lockResult, 0, false, false}
}

type ReplicationAckDB struct {
	manager           *ReplicationManager
	glock             *sync.Mutex
	ackGlocks         []*sync.Mutex
	commandAofs       []map[[16]byte][16]byte
	aofLocks          []map[[16]byte]*Lock
	ackLocks          []map[[16]byte]*ReplicationAckLock
	freeAckLocks      []*ReplicationAckLock
	freeAckLocksIndex uint32
	freeAckLocksMax   uint32
	ackMaxGlocks      uint16
	ackCount          uint8
	closed            bool
}

func NewReplicationAckDB(manager *ReplicationManager) *ReplicationAckDB {
	ackMaxGlocks := uint16(Config.DBConcurrent)
	if ackMaxGlocks == 0 {
		ackMaxGlocks = uint16(runtime.NumCPU()) * 2
	}
	ackGlocks := make([]*sync.Mutex, ackMaxGlocks)
	commandAofs := make([]map[[16]byte][16]byte, ackMaxGlocks)
	aofLocks := make([]map[[16]byte]*Lock, ackMaxGlocks)
	ackLocks := make([]map[[16]byte]*ReplicationAckLock, ackMaxGlocks)
	for i := uint16(0); i < ackMaxGlocks; i++ {
		ackGlocks[i] = &sync.Mutex{}
		commandAofs[i] = make(map[[16]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		aofLocks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
	}
	return &ReplicationAckDB{manager, &sync.Mutex{}, ackGlocks,
		commandAofs, aofLocks, ackLocks, make([]*ReplicationAckLock, REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE*int(ackMaxGlocks)),
		0, uint32(REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE * int(ackMaxGlocks)), ackMaxGlocks, 1, false}
}

func (self *ReplicationAckDB) getAckLock() *ReplicationAckLock {
	if self.freeAckLocksIndex > 0 {
		self.glock.Lock()
		if self.freeAckLocksIndex > 0 {
			self.freeAckLocksIndex--
			ackLock := self.freeAckLocks[self.freeAckLocksIndex]
			self.glock.Unlock()
			ackLock.locked = false
			ackLock.aofed = false
			return ackLock
		}
		self.glock.Unlock()
	}
	return NewReplicationAckLock()
}

func (self *ReplicationAckDB) freeAckLock(ackLock *ReplicationAckLock) {
	if self.freeAckLocksIndex < self.freeAckLocksMax {
		self.glock.Lock()
		if self.freeAckLocksIndex < self.freeAckLocksMax {
			self.freeAckLocks[self.freeAckLocksIndex] = ackLock
			self.freeAckLocksIndex++
		}
		self.glock.Unlock()
	}
}

func (self *ReplicationAckDB) Close() error {
	self.closed = true
	return nil
}

func (self *ReplicationAckDB) ProcessLeaderPushLock(glockIndex uint16, aofLock *AofLock) error {
	lock := aofLock.lock
	if lock == nil {
		return nil
	}
	self.ackGlocks[glockIndex].Lock()
	if self.manager.slock.state != STATE_LEADER {
		self.ackGlocks[glockIndex].Unlock()
		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, false)
		return nil
	}
	if _, ok := self.commandAofs[glockIndex][lock.command.RequestId]; ok {
		self.ackGlocks[glockIndex].Unlock()
		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, false)
		return nil
	}

	aofId := aofLock.GetAofId()
	self.commandAofs[glockIndex][lock.command.RequestId] = aofId
	self.aofLocks[glockIndex][aofId] = lock
	lock.ackCount = self.ackCount
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessLeaderPushUnLock(glockIndex uint16, aofLock *AofLock) error {
	lock := aofLock.lock
	if lock == nil {
		return nil
	}
	lockCommand := lock.command
	if lockCommand == nil {
		return nil
	}
	self.ackGlocks[glockIndex].Lock()
	if aofId, ok := self.commandAofs[glockIndex][lockCommand.RequestId]; ok {
		delete(self.commandAofs[glockIndex], lockCommand.RequestId)
		if _, ok = self.aofLocks[glockIndex][aofId]; ok {
			delete(self.aofLocks[glockIndex], aofId)
		}
		self.ackGlocks[glockIndex].Unlock()
		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, false)
		return nil
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessLeaderAcked(glockIndex uint16, aofLock *AofLock) error {
	aofId := aofLock.GetAofId()
	self.ackGlocks[glockIndex].Lock()
	if lock, ok := self.aofLocks[glockIndex][aofId]; ok {
		if aofLock.Result != 0 || lock.ackCount == 0xff {
			delete(self.aofLocks[glockIndex], aofId)
			if _, ok = self.commandAofs[glockIndex][lock.command.RequestId]; ok {
				delete(self.commandAofs[glockIndex], lock.command.RequestId)
			}
			self.ackGlocks[glockIndex].Unlock()

			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
			return nil
		}

		lock.ackCount--
		if lock.ackCount > 0 {
			self.ackGlocks[glockIndex].Unlock()
			return nil
		}
		delete(self.aofLocks[glockIndex], aofId)
		if _, ok = self.commandAofs[glockIndex][lock.command.RequestId]; ok {
			delete(self.commandAofs[glockIndex], lock.command.RequestId)
		}
		self.ackGlocks[glockIndex].Unlock()

		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, true)
		return nil
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessLeaderAofed(glockIndex uint16, aofLock *AofLock) error {
	aofId := aofLock.GetAofId()
	self.ackGlocks[glockIndex].Lock()
	if lock, ok := self.aofLocks[glockIndex][aofId]; ok {
		if aofLock.Result != 0 || lock.ackCount == 0xff {
			delete(self.aofLocks[glockIndex], aofId)
			if _, ok = self.commandAofs[glockIndex][lock.command.RequestId]; ok {
				delete(self.aofLocks[glockIndex], lock.command.RequestId)
			}
			self.ackGlocks[glockIndex].Unlock()

			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
			return nil
		}

		lock.ackCount--
		if lock.ackCount > 0 {
			self.ackGlocks[glockIndex].Unlock()
			return nil
		}
		delete(self.aofLocks[glockIndex], aofId)
		if _, ok = self.commandAofs[glockIndex][lock.command.RequestId]; ok {
			delete(self.commandAofs[glockIndex], lock.command.RequestId)
		}
		self.ackGlocks[glockIndex].Unlock()

		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, true)
		return nil
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessFollowerPushAckLock(glockIndex uint16, aofLock *AofLock) error {
	aofId := aofLock.GetAofId()
	self.ackGlocks[glockIndex].Lock()
	if ackLock, ok := self.ackLocks[glockIndex][aofId]; !ok {
		ackLock = self.getAckLock()
		self.ackLocks[glockIndex][aofId] = ackLock
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessFollowerPushAckUnLock(glockIndex uint16, aofLock *AofLock) error {
	aofId := aofLock.GetAofId()
	self.ackGlocks[glockIndex].Lock()
	if ackLock, ok := self.ackLocks[glockIndex][aofId]; ok {
		delete(self.ackLocks[glockIndex], aofId)
		self.ackGlocks[glockIndex].Unlock()
		self.freeAckLock(ackLock)
		return nil
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessFollowerAckLocked(glockIndex uint16, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	self.ackGlocks[glockIndex].Lock()
	ackLock, ok := self.ackLocks[glockIndex][command.RequestId]
	if !ok {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}

	ackLock.lockResult.CommandType = command.CommandType
	ackLock.lockResult.RequestId = command.RequestId
	if command.Flag&protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED != 0 && result == protocol.RESULT_LOCKED_ERROR {
		ackLock.lockResult.Result = 0
	} else {
		ackLock.lockResult.Result = result
	}
	ackLock.lockResult.Flag = 0
	ackLock.lockResult.DbId = command.DbId
	ackLock.lockResult.LockId = command.LockId
	ackLock.lockResult.LockKey = command.LockKey
	ackLock.lockResult.Lcount = lcount
	ackLock.lockResult.Count = command.Count
	ackLock.lockResult.Lrcount = lrcount
	ackLock.lockResult.Rcount = command.Rcount
	ackLock.locked = true
	if !ackLock.aofed {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}
	delete(self.ackLocks[glockIndex], command.RequestId)
	self.ackGlocks[glockIndex].Unlock()
	if self.manager.clientChannel != nil {
		err := self.manager.clientChannel.HandleAcked(ackLock)
		if err != nil {
			self.manager.slock.Log().Errorf("Replication client write ack error %v", err)
		}
	} else {
		self.manager.slock.Log().Errorf("Replication client write ack not open")
	}
	self.freeAckLock(ackLock)
	return nil
}

func (self *ReplicationAckDB) ProcessFollowerAckAofed(glockIndex uint16, aofLock *AofLock) error {
	aofId := aofLock.GetAofId()
	self.ackGlocks[glockIndex].Lock()
	ackLock, ok := self.ackLocks[glockIndex][aofId]
	if !ok {
		ackLock = self.getAckLock()
		self.ackLocks[glockIndex][aofId] = ackLock
	}

	ackLock.aofResult = aofLock.Result
	ackLock.aofed = true
	if !ackLock.locked {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}
	delete(self.ackLocks[glockIndex], aofId)
	self.ackGlocks[glockIndex].Unlock()
	if self.manager.clientChannel != nil {
		err := self.manager.clientChannel.HandleAcked(ackLock)
		if err != nil {
			self.manager.slock.Log().Errorf("Replication client write ack error %v", err)
		}
	} else {
		self.manager.slock.Log().Errorf("Replication client write ack not open")
	}
	self.freeAckLock(ackLock)
	return nil
}

func (self *ReplicationAckDB) SwitchToLeader() error {
	for i, ackLocks := range self.ackLocks {
		self.ackGlocks[i].Lock()
		for _, ackLock := range ackLocks {
			if self.manager.clientChannel != nil {
				ackLock.aofResult = protocol.RESULT_ERROR
				err := self.manager.clientChannel.HandleAcked(ackLock)
				if err != nil {
					self.manager.slock.Log().Errorf("Replication client write ack error %v", err)
				}
			}
			self.freeAckLock(ackLock)
		}
		self.ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

func (self *ReplicationAckDB) SwitchToFollower() error {
	for i, locks := range self.aofLocks {
		self.ackGlocks[i].Lock()
		for _, lock := range locks {
			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
		}
		self.commandAofs[i] = make(map[[16]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		self.aofLocks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

func (self *ReplicationAckDB) FlushDB() error {
	for i := uint16(0); i < self.ackMaxGlocks; i++ {
		self.ackGlocks[i].Lock()
		for _, lock := range self.aofLocks[i] {
			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
		}

		for _, ackLock := range self.ackLocks[i] {
			if self.manager.clientChannel != nil {
				ackLock.aofResult = protocol.RESULT_ERROR
				err := self.manager.clientChannel.HandleAcked(ackLock)
				if err != nil {
					self.manager.slock.Log().Errorf("Replication client write ack error %v", err)
				}
			}
			self.freeAckLock(ackLock)
		}

		self.commandAofs[i] = make(map[[16]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		self.aofLocks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

type ReplicationManager struct {
	slock               *SLock
	glock               *sync.Mutex
	bufferQueue         *ReplicationBufferQueue
	ackDbs              []*ReplicationAckDB
	clientChannel       *ReplicationClient
	serverChannels      []*ReplicationServer
	transparencyManager *TransparencyManager
	currentAofId        [16]byte
	leaderAddress       string
	serverCount         uint32
	serverActiveCount   uint32
	serverFlushWaiter   chan bool
	initedWaters        []chan bool
	closed              bool
	isLeader            bool
}

func NewReplicationManager() *ReplicationManager {
	transparencyManager := NewTransparencyManager()
	manager := &ReplicationManager{nil, &sync.Mutex{}, nil, make([]*ReplicationAckDB, 256),
		nil, make([]*ReplicationServer, 0), transparencyManager, [16]byte{}, "", 0,
		0, nil, make([]chan bool, 0), false, true}
	manager.bufferQueue = NewReplicationBufferQueue(manager, uint64(Config.AofRingBufferSize), uint64(Config.AofRingBufferMaxSize))
	return manager
}

func (self *ReplicationManager) GetCallMethods() map[string]BinaryServerProtocolCallHandler {
	handlers := make(map[string]BinaryServerProtocolCallHandler, 2)
	handlers["SYNC"] = self.commandHandleSyncCommand
	return handlers
}

func (self *ReplicationManager) GetHandlers() map[string]TextServerProtocolCommandHandler {
	handlers := make(map[string]TextServerProtocolCommandHandler, 2)
	return handlers
}

func (self *ReplicationManager) Init(leaderAddress string, aofId [16]byte) error {
	self.leaderAddress = leaderAddress
	self.currentAofId = aofId
	self.slock.Log().Infof("Replication aof ring buffer init size %d", int(Config.AofRingBufferSize))
	if self.slock.state == STATE_LEADER {
		self.slock.Log().Infof("Replication init leader by aofId %s", FormatAofId(self.currentAofId))
	} else {
		_ = self.transparencyManager.ChangeLeader(leaderAddress)
		self.slock.Log().Infof("Replication init follower %s by aofId %s", leaderAddress, FormatAofId(self.currentAofId))
	}
	return nil
}

func (self *ReplicationManager) Close() {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return
	}
	self.closed = true
	self.glock.Unlock()
	_ = self.bufferQueue.Close()

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.clientChannel = nil
	}

	_ = self.WakeupServerChannel()
	_ = self.WaitServerSynced()
	for _, channel := range self.serverChannels {
		_ = channel.Close()
		<-channel.closedWaiter
	}
	self.glock.Lock()
	self.serverChannels = self.serverChannels[:0]
	for _, waiter := range self.initedWaters {
		waiter <- false
	}
	self.initedWaters = self.initedWaters[:0]
	for i, db := range self.ackDbs {
		if db != nil {
			_ = db.Close()
			self.ackDbs[i] = nil
		}
	}
	self.glock.Unlock()
	_ = self.transparencyManager.Close()
	<-self.transparencyManager.closedWaiter
	self.slock.logger.Infof("Replication closed")
}

func (self *ReplicationManager) WaitServerSynced() error {
	self.glock.Lock()
	if atomic.CompareAndSwapUint32(&self.serverActiveCount, 0, 0) {
		var waitPulledCount = 0
		for _, channel := range self.serverChannels {
			if atomic.LoadUint32(&channel.pulledState) != 2 {
				waitPulledCount++
			}
		}
		if waitPulledCount == 0 {
			self.glock.Unlock()
			return nil
		}
	}

	serverFlushWaiter := make(chan bool, 1)
	go func() {
		select {
		case <-serverFlushWaiter:
			return
		case <-time.After(120 * time.Second):
			self.serverFlushWaiter = nil
			close(serverFlushWaiter)
		}
	}()
	self.serverFlushWaiter = serverFlushWaiter
	self.glock.Unlock()
	<-self.serverFlushWaiter
	return nil
}

func (self *ReplicationManager) commandHandleSyncCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.closed {
		return protocol.NewCallResultCommand(command, 0, "STATE_ERROR", nil), io.EOF
	}

	channel := NewReplicationServer(self, server_protocol)
	self.slock.logger.Infof("Replication server recv client %s sync", server_protocol.RemoteAddr().String())
	result, err := channel.handleInitSync(command)
	if err != nil {
		channel.closed = true
		close(channel.closedWaiter)
		if err != io.EOF {
			self.slock.logger.Errorf("Replication server handle client %s init sync error %s %v", server_protocol.RemoteAddr().String(), err)
		}
		return result, err
	}
	if result != nil {
		channel.closed = true
		close(channel.closedWaiter)
		self.slock.logger.Infof("Replication server handle client %s start sync fail", server_protocol.RemoteAddr().String())
		return result, nil
	}

	self.slock.logger.Infof("Replication server handle client %s start sync", server_protocol.RemoteAddr().String())
	_ = self.addServerChannel(channel)
	server_protocol.stream.streamType = STREAM_TYPE_AOF
	go func() {
		serr := channel.SendProcess()
		if serr != nil {
			if serr != io.EOF && !self.closed {
				self.slock.logger.Errorf("Replication handle client %s sync error %v", server_protocol.RemoteAddr().String(), serr)
			}
			_ = channel.Close()
		}
	}()
	err = channel.RecvProcess()
	channel.closed = true
	_ = self.WakeupServerChannel()
	_ = self.removeServerChannel(channel)
	close(channel.closedWaiter)
	if err != nil {
		if err != io.EOF && !self.closed {
			self.slock.logger.Errorf("Replication handle client %s process error %v", server_protocol.RemoteAddr().String(), err)
		}
		return nil, io.EOF
	}
	self.slock.logger.Infof("Replication server handle client %s closed", server_protocol.RemoteAddr().String())
	return nil, io.EOF
}

func (self *ReplicationManager) addServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	self.serverChannels = append(self.serverChannels, channel)
	self.serverCount = uint32(len(self.serverChannels))
	self.UpdateDBAckCount()
	self.bufferQueue.AddPoll(channel.bufferCursor)
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) removeServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	serverChannels := make([]*ReplicationServer, 0)
	for _, c := range self.serverChannels {
		if channel != c {
			serverChannels = append(serverChannels, c)
		}
	}
	self.serverChannels = serverChannels
	self.serverCount = uint32(len(serverChannels))
	self.UpdateDBAckCount()
	self.bufferQueue.RemovePoll(channel.bufferCursor)

	if atomic.CompareAndSwapUint32(&self.serverActiveCount, 0, 0) {
		if self.serverFlushWaiter != nil {
			close(self.serverFlushWaiter)
			self.serverFlushWaiter = nil
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) StartSync() error {
	if self.leaderAddress == "" {
		return errors.New("slaveof is empty")
	}

	channel := NewReplicationClient(self)
	self.isLeader = false
	self.clientChannel = channel
	go channel.Run()
	self.slock.logger.Infof("Replication start sync")
	return nil
}

func (self *ReplicationManager) GetAckDB(dbId uint8) *ReplicationAckDB {
	return self.ackDbs[dbId]
}

func (self *ReplicationManager) GetOrNewAckDB(dbId uint8) *ReplicationAckDB {
	db := self.ackDbs[dbId]
	if db != nil {
		return db
	}

	self.glock.Lock()
	if self.ackDbs[dbId] == nil {
		self.ackDbs[dbId] = NewReplicationAckDB(self)
		self.UpdateDBAckCount()
	}
	self.glock.Unlock()
	return self.ackDbs[dbId]
}

func (self *ReplicationManager) PushLock(glockIndex uint16, aofLock *AofLock) error {
	if aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && self.slock.state == STATE_LEADER && aofLock.lock != nil {
		db := self.GetOrNewAckDB(aofLock.DbId)
		switch aofLock.CommandType {
		case protocol.COMMAND_LOCK:
			err := db.ProcessLeaderPushLock(glockIndex, aofLock)
			if err != nil {
				return err
			}
		case protocol.COMMAND_UNLOCK:
			err := db.ProcessLeaderPushUnLock(glockIndex, aofLock)
			if err != nil {
				return err
			}
		}
	}

	buf := aofLock.buf
	if aofLock.AofFlag&AOF_FLAG_CONTAINS_DATA != 0 {
		err := self.bufferQueue.Push(buf, aofLock.data)
		if err != nil {
			if aofLock.CommandType == protocol.COMMAND_LOCK && aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && self.slock.state == STATE_LEADER && aofLock.lock != nil {
				db := self.slock.replicationManager.GetAckDB(aofLock.DbId)
				if db != nil {
					_ = db.ProcessLeaderPushUnLock(glockIndex, aofLock)
				}
			}
			return err
		}
	} else {
		err := self.bufferQueue.Push(buf, nil)
		if err != nil {
			if aofLock.CommandType == protocol.COMMAND_LOCK && aofLock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && self.slock.state == STATE_LEADER && aofLock.lock != nil {
				db := self.slock.replicationManager.GetAckDB(aofLock.DbId)
				if db != nil {
					_ = db.ProcessLeaderPushUnLock(glockIndex, aofLock)
				}
			}
			return err
		}
	}

	if len(buf) >= 64 {
		self.currentAofId[0], self.currentAofId[1], self.currentAofId[2], self.currentAofId[3], self.currentAofId[4], self.currentAofId[5], self.currentAofId[6], self.currentAofId[7],
			self.currentAofId[8], self.currentAofId[9], self.currentAofId[10], self.currentAofId[11], self.currentAofId[12], self.currentAofId[13], self.currentAofId[14], self.currentAofId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}
	return nil
}

func (self *ReplicationManager) WakeupServerChannel() error {
	if self.serverCount == 0 || atomic.LoadUint32(&self.serverActiveCount) == self.serverCount {
		return nil
	}
	for _, channel := range self.serverChannels {
		if atomic.CompareAndSwapUint32(&channel.pulledState, 2, 1) {
			channel.pulledWaiter <- true
		}
	}
	return nil
}

func (self *ReplicationManager) WaitInitSynced(waiter chan bool) {
	self.glock.Lock()
	self.initedWaters = append(self.initedWaters, waiter)
	self.glock.Unlock()
}

func (self *ReplicationManager) wakeupInitSyncedWaiters() {
	self.glock.Lock()
	for _, waiter := range self.initedWaters {
		waiter <- false
	}
	self.initedWaters = self.initedWaters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) clientSycnInited() {
	self.glock.Lock()
	self.slock.updateState(STATE_FOLLOWER)
	for _, waiter := range self.initedWaters {
		waiter <- true
	}
	self.initedWaters = self.initedWaters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) SwitchToLeader() error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	self.slock.logger.Infof("Replication start change to leader")
	for _, db := range self.ackDbs {
		if db != nil {
			_ = db.SwitchToLeader()
		}
	}
	self.slock.updateState(STATE_LEADER)
	self.leaderAddress = ""

	self.UpdateDBAckCount()
	self.glock.Unlock()
	self.slock.ReplicationUpdate()

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentAofId = clientChannel.currentAofId
		self.clientChannel = nil
	}
	self.isLeader = true
	self.slock.logger.Infof("Replication finish change to leader")
	return nil
}

func (self *ReplicationManager) SwitchToFollower(address string) error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leaderAddress == address && !self.isLeader {
		self.glock.Unlock()
		return nil
	}

	self.slock.logger.Infof("Replication start change to follower, leader %s", address)
	self.leaderAddress = address
	if address == "" {
		self.slock.updateState(STATE_FOLLOWER)
	} else {
		self.slock.updateState(STATE_SYNC)
	}
	self.glock.Unlock()

	for _, db := range self.slock.dbs {
		if db != nil {
			for i := uint16(0); i < db.managerMaxGlocks; i++ {
				db.managerGlocks[i].Lock()
				db.managerGlocks[i].Unlock()
			}
		}
	}
	_ = self.slock.aof.WaitFlushAofChannel()
	_ = self.WakeupServerChannel()
	_ = self.WaitServerSynced()
	for _, db := range self.ackDbs {
		if db != nil {
			_ = db.SwitchToFollower()
		}
	}
	self.slock.ReplicationUpdate()

	if self.leaderAddress == "" {
		if self.clientChannel != nil {
			clientChannel := self.clientChannel
			_ = clientChannel.End()
			<-clientChannel.closedWaiter
			self.currentAofId = clientChannel.currentAofId
			self.clientChannel = nil
		}
		self.isLeader = false
		self.slock.logger.Infof("Replication finish change to follower, leader empty")
		return nil
	}

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentAofId = clientChannel.currentAofId
		self.clientChannel = nil
	}
	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication finish change to follower, leader %s", address)
	return nil
}

func (self *ReplicationManager) ChangeLeader(address string) error {
	self.glock.Lock()
	if self.slock.state != STATE_FOLLOWER {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leaderAddress == address {
		self.glock.Unlock()
		return nil
	}

	self.slock.logger.Infof("Replication follower start change current leader %s", address)
	self.leaderAddress = address
	if self.leaderAddress == "" {
		self.slock.updateState(STATE_FOLLOWER)
	} else {
		self.slock.updateState(STATE_SYNC)
	}
	self.glock.Unlock()
	self.slock.ReplicationUpdate()

	if self.leaderAddress == "" {
		if self.clientChannel != nil {
			clientChannel := self.clientChannel
			_ = clientChannel.End()
			<-clientChannel.closedWaiter
			self.currentAofId = clientChannel.currentAofId
			self.clientChannel = nil
		}
		self.isLeader = false
		self.slock.logger.Infof("Replication follower finish change current empty leader")
		return nil
	}

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentAofId = clientChannel.currentAofId
		self.clientChannel = nil
	}
	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication follower finish change current leader %s", address)
	return nil
}

func (self *ReplicationManager) SuspendFollower() error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}
	self.slock.logger.Infof("Replication start change suspend follower")
	if self.leaderAddress != "" {
		self.leaderAddress = ""
		self.glock.Unlock()

		for _, db := range self.slock.dbs {
			if db != nil {
				for i := uint16(0); i < db.managerMaxGlocks; i++ {
					db.managerGlocks[i].Lock()
					db.managerGlocks[i].Unlock()
				}
			}
		}
		_ = self.slock.aof.WaitFlushAofChannel()
		_ = self.WakeupServerChannel()
		_ = self.WaitServerSynced()
		for _, db := range self.ackDbs {
			if db != nil {
				_ = db.SwitchToFollower()
			}
		}
		self.slock.ReplicationUpdate()
	} else {
		self.glock.Unlock()
	}

	for _, channel := range self.serverChannels {
		_ = channel.Close()
		<-channel.closedWaiter
	}
	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentAofId = clientChannel.currentAofId
		self.clientChannel = nil
	}

	self.isLeader = false
	self.slock.logger.Infof("Replication finish change spspend follower")
	return nil
}

func (self *ReplicationManager) DoServerChannelQuit() error {
	for _, channel := range self.serverChannels {
		err := channel.SendQuit()
		if err != nil {
			self.slock.logger.Warnf("Replication send quit server channel error %v", err)
			_ = channel.Close()
		}
		<-channel.closedWaiter
	}
	return nil
}

func (self *ReplicationManager) FlushDB() error {
	_ = self.slock.aof.WaitFlushAofChannel()

	for _, db := range self.slock.dbs {
		if db != nil {
			_ = db.FlushDB()
		}
	}
	if self.slock.state != STATE_LEADER {
		for _, db := range self.ackDbs {
			if db != nil {
				_ = db.FlushDB()
			}
		}
	}
	self.slock.Log().Infof("Replication flush all DB")
	return nil
}

func (self *ReplicationManager) UpdateDBAckCount() {
	ackCount := 0
	if self.slock.arbiterManager != nil {
		if Config.AofAckMode == 2 {
			ackCount = len(self.serverChannels) + 1
		} else {
			ackCount = self.slock.arbiterManager.GetMajorityMemberCount()
		}
	} else {
		if Config.AofAckMode == 1 {
			ackCount = (len(self.serverChannels)+1)/2 + 1
		} else {
			ackCount = len(self.serverChannels) + 1
		}
	}
	for _, db := range self.ackDbs {
		if db != nil {
			db.ackCount = uint8(ackCount)
		}
	}
}

func (self *ReplicationManager) GetCurrentAofID() [16]byte {
	if !self.isLeader && self.clientChannel != nil {
		return self.clientChannel.currentAofId
	}
	return self.currentAofId
}
