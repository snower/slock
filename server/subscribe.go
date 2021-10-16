package server

import (
	"errors"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var subscriberIdIndex uint64 = 0

type PublishLock struct {
	Magic       uint8
	Version     uint8
	CommandType uint8
	RequestId   [16]byte
	Result      uint8
	Flag        uint8
	DbId        uint8
	LockId      [16]byte
	LockKey     [16]byte
	Lcount      uint16
	Count       uint16
	Lrcount     uint8
	Rcount      uint8
	buf         []byte
}

func NewPublishLock() *PublishLock {
	return &PublishLock{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_PUBLISH, [16]byte{}, 0,
		0, 0, [16]byte{}, [16]byte{}, 0, 0, 0, 0, make([]byte, 64)}

}

func (self *PublishLock) Decode() error {
	buf := self.buf
	if len(buf) < 64 {
		return errors.New("buf too short")
	}

	self.Magic, self.Version, self.CommandType = uint8(buf[0]), uint8(buf[1]), uint8(buf[2])

	self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15] =
		buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

	self.Result, self.Flag, self.DbId = uint8(buf[19]), uint8(buf[20]), uint8(buf[21])

	self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
		self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15] =
		buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
		buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37]

	self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
		self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15] =
		buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
		buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53]

	self.Lcount, self.Count, self.Lrcount, self.Rcount = uint16(buf[54])|uint16(buf[55])<<8, uint16(buf[56])|uint16(buf[57])<<8, uint8(buf[58]), uint8(buf[59])

	return nil
}

func (self *PublishLock) Encode() error {
	buf := self.buf
	if len(buf) < 64 {
		return errors.New("buf too short")
	}

	buf[0], buf[1], buf[2] = byte(self.Magic), byte(self.Version), byte(self.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		self.RequestId[0], self.RequestId[1], self.RequestId[2], self.RequestId[3], self.RequestId[4], self.RequestId[5], self.RequestId[6], self.RequestId[7],
		self.RequestId[8], self.RequestId[9], self.RequestId[10], self.RequestId[11], self.RequestId[12], self.RequestId[13], self.RequestId[14], self.RequestId[15]

	buf[19], buf[20], buf[21] = uint8(self.Result), byte(self.Flag), byte(self.DbId)

	buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
		buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] =
		self.LockId[0], self.LockId[1], self.LockId[2], self.LockId[3], self.LockId[4], self.LockId[5], self.LockId[6], self.LockId[7],
		self.LockId[8], self.LockId[9], self.LockId[10], self.LockId[11], self.LockId[12], self.LockId[13], self.LockId[14], self.LockId[15]

	buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
		buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] =
		self.LockKey[0], self.LockKey[1], self.LockKey[2], self.LockKey[3], self.LockKey[4], self.LockKey[5], self.LockKey[6], self.LockKey[7],
		self.LockKey[8], self.LockKey[9], self.LockKey[10], self.LockKey[11], self.LockKey[12], self.LockKey[13], self.LockKey[14], self.LockKey[15]

	buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(self.Lcount), byte(self.Lcount>>8), byte(self.Count), byte(self.Count>>8), byte(self.Lrcount), byte(self.Rcount), 0x00, 0x00
	buf[62], buf[63] = 0x00, 0x00
	return nil
}

type SubscribeBuffer struct {
	buf    []byte
	rindex int
	windex int
	next   *SubscribeBuffer
}

type SubscribeClient struct {
	manager       *SubscribeManager
	glock         *sync.Mutex
	leaderAddress string
	subscriberId  uint64
	stream        *client.Stream
	protocol      *client.BinaryClientProtocol
	publishLock   *PublishLock
	closed        bool
	closedWaiter  chan bool
	wakeupSignal  chan bool
}

func NewSubscribeClient(manager *SubscribeManager) *SubscribeClient {
	return &SubscribeClient{manager, &sync.Mutex{}, "", 0, nil, nil,
		NewPublishLock(), false, make(chan bool, 1), nil}
}

func (self *SubscribeClient) Open(leaderAddress string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", leaderAddress, 2*time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	clientProtocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.protocol = clientProtocol
	self.closed = false
	return nil
}

func (self *SubscribeClient) Close() error {
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	_ = self.WakeupRetryConnect()
	self.manager.slock.logger.Errorf("Subscribe client %s close", self.leaderAddress)
	return nil
}

func (self *SubscribeClient) initSubscribe() error {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	command := protocol.NewSubscribeCommand(self.subscriberId, 0, lockKeyMask, 7200, 67108864)
	err := self.protocol.Write(command)
	if err != nil {
		return err
	}
	resultCommand, err := self.protocol.ReadCommand()
	if err != nil {
		return err
	}

	if subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand); ok {
		self.subscriberId = subscribeResultCommand.SubscribeId
		return nil
	}
	return errors.New("unknown command")
}

func (self *SubscribeClient) Run() {
	for !self.closed {
		self.manager.slock.logger.Infof("Subscribe client connect leader %s", self.leaderAddress)
		err := self.Open(self.manager.leaderAddress)
		if err != nil {
			self.manager.slock.logger.Errorf("Subscribe client connect leader %s error %v", self.leaderAddress, err)
			if self.protocol != nil {
				_ = self.protocol.Close()
			}
			self.glock.Lock()
			self.stream = nil
			self.protocol = nil
			self.glock.Unlock()
			if self.closed {
				break
			}
			_ = self.sleepWhenRetryConnect()
			continue
		}

		err = self.initSubscribe()
		if err != nil {
			if err != io.EOF {
				self.manager.slock.logger.Errorf("Subscribe client init sync error %s %v", self.leaderAddress, err)
			}
		} else {
			self.manager.slock.logger.Infof("Subscribe client connected leader %s", self.leaderAddress)
			err = self.Process()
			if err != nil {
				if err != io.EOF && !self.closed {
					self.manager.slock.logger.Errorf("Subscribe client sync leader %s error %v", self.leaderAddress, err)
				}
			}
		}

		if self.protocol != nil {
			_ = self.protocol.Close()
		}
		self.glock.Lock()
		self.stream = nil
		self.protocol = nil
		self.glock.Unlock()
		if self.closed {
			break
		}
		_ = self.sleepWhenRetryConnect()
	}

	close(self.closedWaiter)
	self.manager.client = nil
	self.manager.slock.logger.Infof("Subscribe client connect leader %s closed", self.leaderAddress)
}

func (self *SubscribeClient) Process() error {
	for !self.closed {
		buf := self.publishLock.buf
		n, err := self.stream.Read(buf)
		if err != nil {
			return err
		}

		if n < 64 {
			for n < 64 {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					return nerr
				}
				n += nn
			}
		}

		err = self.publishLock.Decode()
		if err != nil {
			return err
		}

		db := self.manager.slock.GetDB(self.publishLock.DbId)
		if db == nil {
			self.manager.slock.GetOrNewDB(self.publishLock.DbId)
		}

		publishId := uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
		err = db.subscribeChannels[publishId%uint64(db.managerMaxGlocks)].ClientPush(self.publishLock)
		if err != nil {
			return err
		}
		return nil
	}
	return io.EOF
}

func (self *SubscribeClient) sleepWhenRetryConnect() error {
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

func (self *SubscribeClient) WakeupRetryConnect() error {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
	return nil
}

type Subscriber struct {
	manager                    *SubscribeManager
	glock                      *sync.Mutex
	subscriberId               uint64
	publishIds                 [][2]uint64
	bufferHead                 *SubscribeBuffer
	bufferTail                 *SubscribeBuffer
	bufferSize                 int
	serverProtocol             ServerProtocol
	serverProtocolClosedTime   int64
	serverProtocolClosedWaiter chan bool
	expriedTime                uint32
	maxSize                    uint32
	pulled                     bool
	pullWaiter                 chan bool
	closed                     bool
	closedWaiter               chan bool
}

func NewSubscriber(manager *SubscribeManager, serverProtocol ServerProtocol, subscriberId uint64) *Subscriber {
	return &Subscriber{manager, &sync.Mutex{}, subscriberId, make([][2]uint64, 0),
		nil, nil, 0, serverProtocol, 0,
		serverProtocol.GetStream().closedWaiter, 0, 0,
		false, make(chan bool, 4), false, make(chan bool, 1)}
}

func (self *Subscriber) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	_ = self.manager.removeSubscriber(self)
	if self.pulled {
		self.pullWaiter <- false
	}
	self.glock.Unlock()

	<-self.closedWaiter
	self.glock.Lock()
	currentBuffer := self.bufferHead
	for currentBuffer != nil {
		nextBuffer := currentBuffer.next
		self.manager.freeBuffer(currentBuffer)
		currentBuffer = nextBuffer
	}
	self.bufferHead = nil
	self.bufferTail = nil
	self.bufferSize = 0
	self.serverProtocol = nil
	self.glock.Unlock()
	return nil
}

func (self *Subscriber) Run() {
	for !self.closed {
		self.glock.Lock()
		self.pulled = true
		self.glock.Unlock()

		timeout := 120
		if timeout > int(self.expriedTime) {
			timeout = int(self.expriedTime)
		}
		select {
		case <-self.pullWaiter:
			self.glock.Lock()
			self.pulled = false
			if self.serverProtocol == nil {
				self.processCheck()
			} else if self.bufferHead != self.bufferTail || self.bufferHead.rindex < self.bufferTail.windex {
				self.processLock()
			}
			self.glock.Unlock()
		case <-time.After(time.Duration(timeout) * time.Second):
			self.glock.Lock()
			self.processCheck()
			self.glock.Unlock()
		case <-self.serverProtocolClosedWaiter:
			self.processServerProcotolClose()
		}
	}

	self.serverProtocol = nil
	close(self.closedWaiter)
}

func (self *Subscriber) processLock() {
	for self.bufferHead != self.bufferTail || self.bufferHead.rindex < self.bufferTail.windex {
		buf := self.bufferHead.buf[self.bufferHead.rindex:self.bufferHead.windex]
		if self.serverProtocol == nil {
			return
		}
		stream := self.serverProtocol.GetStream()
		if stream == nil {
			self.processServerProcotolClose()
			return
		}
		self.glock.Unlock()

		tn := 0
		for tn < len(buf) {
			n, err := stream.Write(buf[tn:])
			tn += n
			if err != nil {
				self.glock.Lock()
				self.bufferHead.rindex += tn - (tn % 64)
				self.processServerProcotolClose()
				break
			}
		}

		if self.serverProtocol != nil {
			self.glock.Lock()
			self.bufferHead.rindex += tn
		}
		if self.bufferHead.rindex >= len(self.bufferHead.buf) {
			buffer := self.bufferHead
			self.bufferHead = self.bufferHead.next
			self.manager.freeBuffer(buffer)
			if self.bufferHead == nil {
				self.bufferTail = nil
			}
		}
	}
}

func (self *Subscriber) processCheck() {
	if self.serverProtocol != nil {
		stream := self.serverProtocol.GetStream()
		if stream == nil || stream.closed {
			self.processServerProcotolClose()
		}
	}

	if self.serverProtocolClosedTime != 0 && uint32(time.Now().Unix()-self.serverProtocolClosedTime) > self.expriedTime {
		go func() {
			_ = self.Close()
		}()
	}
}

func (self *Subscriber) processServerProcotolClose() {
	self.serverProtocol = nil
	self.serverProtocolClosedTime = time.Now().Unix()
	self.serverProtocolClosedWaiter = make(chan bool, 1)
	if self.expriedTime == 0 {
		go func() {
			_ = self.Close()
		}()
	}
}

func (self *Subscriber) Push(lock *PublishLock) error {
	if self.closed {
		return errors.New("closed")
	}

	self.glock.Lock()
	hKey := uint64(lock.LockKey[0]) | uint64(lock.LockKey[1])<<8 | uint64(lock.LockKey[2])<<16 | uint64(lock.LockKey[3])<<24 | uint64(lock.LockKey[4])<<32 | uint64(lock.LockKey[5])<<40 | uint64(lock.LockKey[6])<<48 | uint64(lock.LockKey[7])<<56
	lKey := uint64(lock.LockKey[8]) | uint64(lock.LockKey[9])<<8 | uint64(lock.LockKey[10])<<16 | uint64(lock.LockKey[11])<<24 | uint64(lock.LockKey[12])<<32 | uint64(lock.LockKey[13])<<40 | uint64(lock.LockKey[14])<<48 | uint64(lock.LockKey[15])<<56
	for _, mask := range self.publishIds {
		if mask[0]&hKey == 0 && mask[1]&lKey == 0 {
			continue
		}

		if self.bufferTail == nil || self.bufferTail.windex >= len(self.bufferTail.buf) {
			buffer := self.manager.getBuffer()
			if self.bufferTail != nil {
				self.bufferTail.next = buffer
			}
			self.bufferTail = buffer
			self.bufferSize += len(self.bufferTail.buf)
			if self.bufferHead == nil {
				self.bufferHead = self.bufferTail
			}

			if uint32(self.bufferSize) >= self.maxSize {
				buffer := self.bufferHead
				self.bufferHead = self.bufferHead.next
				self.manager.freeBuffer(buffer)
				if self.bufferHead == nil {
					self.bufferTail = nil
				}
			}
		}

		copy(self.bufferTail.buf[self.bufferTail.windex:], lock.buf)
		self.bufferTail.buf[self.bufferTail.windex+11] = byte(self.subscriberId)
		self.bufferTail.buf[self.bufferTail.windex+12] = byte(self.subscriberId >> 8)
		self.bufferTail.buf[self.bufferTail.windex+13] = byte(self.subscriberId >> 16)
		self.bufferTail.buf[self.bufferTail.windex+14] = byte(self.subscriberId >> 24)
		self.bufferTail.buf[self.bufferTail.windex+15] = byte(self.subscriberId >> 32)
		self.bufferTail.buf[self.bufferTail.windex+16] = byte(self.subscriberId >> 40)
		self.bufferTail.buf[self.bufferTail.windex+17] = byte(self.subscriberId >> 48)
		self.bufferTail.buf[self.bufferTail.windex+18] = byte(self.subscriberId >> 56)
		self.bufferTail.windex += 64

		if self.pulled {
			self.pullWaiter <- true
		}
		break
	}
	self.glock.Unlock()
	return nil
}

func (self *Subscriber) Update(serverProtocol ServerProtocol, subscriberType uint8, publishId [16]byte, expried uint32, maxSize uint32) error {
	hKey := uint64(publishId[0]) | uint64(publishId[1])<<8 | uint64(publishId[2])<<16 | uint64(publishId[3])<<24 | uint64(publishId[4])<<32 | uint64(publishId[5])<<40 | uint64(publishId[6])<<48 | uint64(publishId[7])<<56
	lKey := uint64(publishId[8]) | uint64(publishId[9])<<8 | uint64(publishId[10])<<16 | uint64(publishId[11])<<24 | uint64(publishId[12])<<32 | uint64(publishId[13])<<40 | uint64(publishId[14])<<48 | uint64(publishId[15])<<56
	lkm := [2]uint64{hKey, lKey}

	self.glock.Lock()
	self.serverProtocol = serverProtocol
	self.serverProtocolClosedTime = 0
	self.serverProtocolClosedWaiter = serverProtocol.GetStream().closedWaiter
	if self.pulled {
		self.pullWaiter <- true
	}

	if subscriberType == 0 {
		hasMask := false
		for _, mask := range self.publishIds {
			if mask == lkm {
				hasMask = true
				break
			}
		}
		if !hasMask {
			self.publishIds = append(self.publishIds, lkm)
		}
	} else {
		publishIds := make([][2]uint64, 0)
		for _, mask := range self.publishIds {
			if mask != lkm {
				publishIds = append(publishIds, mask)
				break
			}
		}
		self.publishIds = publishIds
		if len(self.publishIds) == 0 {
			go func() {
				_ = self.Close()
			}()
		}
	}
	self.glock.Unlock()
	return nil
}

type SubscribeChannel struct {
	manager       *SubscribeManager
	glock         *sync.Mutex
	lockDb        *LockDB
	channel       chan *PublishLock
	freeLocks     []*PublishLock
	freeLockIndex int32
	freeLockMax   int32
	closed        bool
	closedWaiter  chan bool
}

func (self *SubscribeChannel) Push(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.closed {
		return io.EOF
	}

	var publishLock *PublishLock = nil
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		publishLock = self.freeLocks[self.freeLockIndex]
	}
	self.glock.Unlock()

	if publishLock == nil {
		publishLock = NewPublishLock()
	}

	publishId := atomic.AddUint64(&self.manager.publishId, 1)
	publishLock.RequestId[0], publishLock.RequestId[1], publishLock.RequestId[2], publishLock.RequestId[3], publishLock.RequestId[4], publishLock.RequestId[5], publishLock.RequestId[6], publishLock.RequestId[7] =
		byte(publishId), byte(publishId>>8), byte(publishId>>16), byte(publishId>>24), byte(publishId>>32), byte(publishId>>40), byte(publishId>>48), byte(publishId>>56)
	publishLock.Result = result
	publishLock.Flag = 0
	publishLock.DbId = command.DbId
	publishLock.LockId = command.LockId
	publishLock.LockKey = command.LockKey
	publishLock.Lcount = lcount
	publishLock.Count = command.Count
	publishLock.Lrcount = lrcount
	publishLock.Rcount = command.Rcount
	self.channel <- publishLock
	return nil
}

func (self *SubscribeChannel) ClientPush(lock *PublishLock) error {
	if self.closed {
		return io.EOF
	}

	var publishLock *PublishLock = nil
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		publishLock = self.freeLocks[self.freeLockIndex]
	}
	self.glock.Unlock()

	if publishLock == nil {
		publishLock = NewPublishLock()
	}

	publishLock.RequestId = lock.RequestId
	publishLock.Result = lock.Result
	publishLock.Flag = lock.Flag
	publishLock.DbId = lock.DbId
	publishLock.LockId = lock.LockId
	publishLock.LockKey = lock.LockKey
	publishLock.Lcount = lock.Lcount
	publishLock.Count = lock.Count
	publishLock.Lrcount = lock.Lrcount
	publishLock.Rcount = lock.Rcount
	self.channel <- publishLock
	return nil
}

func (self *SubscribeChannel) Run() {
	exited := false
	self.manager.handeLockSubscribeChannel(self)
	for {
		select {
		case publishLock := <-self.channel:
			if publishLock != nil {
				self.handle(publishLock)
				continue
			}
			exited = self.closed
		default:
			self.manager.waitLockSubscribeChannel(self)
			if exited {
				self.manager.RemoveSubscribeChannel(self)
				close(self.closedWaiter)
				return
			}

			publishLock := <-self.channel
			self.manager.handeLockSubscribeChannel(self)
			if publishLock != nil {
				self.handle(publishLock)
				continue
			}
			exited = self.closed
		}
	}
}

func (self *SubscribeChannel) handle(publishLock *PublishLock) {
	for _, subscriber := range self.manager.subscribers {
		_ = subscriber.Push(publishLock)
	}

	self.glock.Lock()
	if self.freeLockIndex < self.freeLockMax {
		self.freeLocks[self.freeLockIndex] = publishLock
		self.freeLockIndex++
	}
	self.glock.Unlock()
}

type SubscribeManager struct {
	slock              *SLock
	glock              *sync.Mutex
	channels           []*SubscribeChannel
	channelCount       uint32
	channelActiveCount uint32
	channelFlushWaiter chan bool
	subscribers        map[uint64]*Subscriber
	fastSubscribers    []*Subscriber
	leaderAddress      string
	client             *SubscribeClient
	freeBuffers        *SubscribeBuffer
	publishId          uint64
	closed             bool
}

func NewSubscribeManager() *SubscribeManager {
	return &SubscribeManager{nil, &sync.Mutex{}, make([]*SubscribeChannel, 0), 0, 0,
		nil, make(map[uint64]*Subscriber, 64), nil, "",
		nil, nil, 0, false}
}

func (self *SubscribeManager) Close() {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return
	}
	self.closed = true
	self.glock.Unlock()

	if self.client != nil {
		_ = self.client.Close()
		<-self.client.closedWaiter
	}
	_ = self.WaitFlushSubscribeChannel()
	for _, subscriber := range self.fastSubscribers {
		_ = subscriber.Close()
	}
	self.freeBuffers = nil
	self.slock.logger.Infof("Subscribe closed")
}

func (self *SubscribeManager) NewSubscribeChannel(lockDb *LockDB) *SubscribeChannel {
	self.glock.Lock()
	subscribeChannel := &SubscribeChannel{self, &sync.Mutex{}, lockDb, make(chan *PublishLock, Config.AofQueueSize),
		make([]*PublishLock, Config.AofQueueSize+4), 0, int32(Config.AofQueueSize + 4),
		false, make(chan bool, 1)}
	self.channels = append(self.channels, subscribeChannel)
	self.channelCount++
	self.glock.Unlock()
	go subscribeChannel.Run()
	return subscribeChannel
}

func (self *SubscribeManager) CloseSubscribeChannel(aofChannel *SubscribeChannel) *SubscribeChannel {
	self.glock.Lock()
	aofChannel.channel <- nil
	aofChannel.closed = true
	self.glock.Unlock()
	return aofChannel
}

func (self *SubscribeManager) RemoveSubscribeChannel(aofChannel *SubscribeChannel) *SubscribeChannel {
	self.glock.Lock()
	channels := make([]*SubscribeChannel, 0)
	for _, c := range self.channels {
		if c != aofChannel {
			channels = append(channels, c)
		}
	}
	self.channels = channels
	self.channelCount = uint32(len(channels))
	self.glock.Unlock()
	return aofChannel
}

func (self *SubscribeManager) handeLockSubscribeChannel(_ *SubscribeChannel) {
	atomic.AddUint32(&self.channelActiveCount, 1)
}

func (self *SubscribeManager) waitLockSubscribeChannel(_ *SubscribeChannel) {
	atomic.AddUint32(&self.channelActiveCount, 0xffffffff)
	if !atomic.CompareAndSwapUint32(&self.channelActiveCount, 0, 0) {
		return
	}

	self.glock.Lock()
	if self.channelFlushWaiter != nil {
		close(self.channelFlushWaiter)
		self.channelFlushWaiter = nil
	}
	self.glock.Unlock()
}

func (self *SubscribeManager) WaitFlushSubscribeChannel() error {
	var channelFlushWaiter chan bool
	self.glock.Lock()
	if self.channelFlushWaiter == nil {
		channelFlushWaiter = make(chan bool, 1)
		self.channelFlushWaiter = channelFlushWaiter
	} else {
		channelFlushWaiter = self.channelFlushWaiter
	}

	if atomic.CompareAndSwapUint32(&self.channelActiveCount, 0, 0) {
		if channelFlushWaiter == self.channelFlushWaiter {
			self.channelFlushWaiter = nil
		}
		self.glock.Unlock()
		return nil
	}
	self.glock.Unlock()

	<-channelFlushWaiter
	return nil
}

func (self *SubscribeManager) handleSubscribeCommand(serverProtocol ServerProtocol, command *protocol.SubscribeCommand) (*protocol.SubscribeResultCommand, error) {
	self.glock.Lock()
	var subscriber *Subscriber
	if command.SubscribeId > 0 {
		if s, ok := self.subscribers[command.SubscribeId]; ok {
			subscriber = s
		}
	}

	if subscriber == nil {
		for {
			subscriberIdIndex++
			if _, ok := self.subscribers[subscriberIdIndex]; ok {
				continue
			}
			break
		}
		subscriber = NewSubscriber(self, serverProtocol, subscriberIdIndex)
		err := self.addSubscriber(subscriber)
		if err != nil {
			return protocol.NewSubscribeResultCommand(command, protocol.RESULT_ERROR, subscriber.subscriberId), nil
		}
	}

	if self.leaderAddress != "" {
		self.glock.Unlock()
		_ = self.openClient()
	} else {
		self.glock.Unlock()
	}
	err := subscriber.Update(serverProtocol, command.CommandType, command.LockKeyMask, command.Expried, command.MaxSize)
	if err != nil {
		return protocol.NewSubscribeResultCommand(command, protocol.RESULT_ERROR, subscriber.subscriberId), nil
	}
	return protocol.NewSubscribeResultCommand(command, protocol.RESULT_SUCCED, subscriber.subscriberId), nil
}

func (self *SubscribeManager) addSubscriber(subscriber *Subscriber) error {
	self.subscribers[subscriber.subscriberId] = subscriber
	subscribers := make([]*Subscriber, 0)
	for _, subscriber := range self.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	self.fastSubscribers = subscribers
	return nil
}

func (self *SubscribeManager) removeSubscriber(subscriber *Subscriber) error {
	self.glock.Lock()
	if _, ok := self.subscribers[subscriber.subscriberId]; ok {
		delete(self.subscribers, subscriber.subscriberId)
		subscribers := make([]*Subscriber, 0)
		for _, subscriber := range self.subscribers {
			subscribers = append(subscribers, subscriber)
		}
		self.fastSubscribers = subscribers
	}
	self.glock.Unlock()
	return nil
}

func (self *SubscribeManager) getBuffer() *SubscribeBuffer {
	self.glock.Lock()
	if self.freeBuffers == nil {
		self.glock.Unlock()
		return &SubscribeBuffer{make([]byte, 4096), 0, 0, nil}
	}

	buffer := self.freeBuffers
	self.freeBuffers = buffer.next
	buffer.rindex, buffer.windex = 0, 0
	buffer.next = nil
	self.glock.Unlock()
	return buffer
}

func (self *SubscribeManager) freeBuffer(buffer *SubscribeBuffer) {
	self.glock.Lock()
	buffer.next = self.freeBuffers
	self.freeBuffers = buffer
	self.glock.Unlock()
}

func (self *SubscribeManager) openClient() error {
	self.glock.Lock()
	if self.leaderAddress == "" || self.client != nil {
		self.glock.Unlock()
		return nil
	}
	self.client = NewSubscribeClient(self)
	self.glock.Unlock()

	go self.client.Run()
	return nil
}

func (self *SubscribeManager) ChangeLeader(address string) error {
	self.glock.Lock()
	if self.leaderAddress == address {
		self.glock.Unlock()
		return nil
	}
	self.slock.Log().Infof("Subscribe start change leader to %s", address)
	self.leaderAddress = address

	if self.client == nil {
		if self.leaderAddress != "" && len(self.fastSubscribers) > 0 {
			self.glock.Unlock()
			_ = self.openClient()
		} else {
			self.glock.Unlock()
		}
	} else {
		if self.leaderAddress == "" {
			self.glock.Unlock()
			_ = self.client.Close()
		} else if self.leaderAddress != self.client.leaderAddress {
			if self.client.protocol != nil {
				_ = self.client.protocol.Close()
			}
			self.glock.Unlock()
		} else {
			self.glock.Unlock()
		}
	}
	self.slock.Log().Infof("Subscribe finish change leader to %s", address)
	return nil
}
