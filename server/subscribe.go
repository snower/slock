package server

import (
	"errors"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var subscriberIdIndex uint32 = 0

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
	data        []byte
	buf         []byte
}

func NewPublishLock() *PublishLock {
	return &PublishLock{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_PUBLISH, [16]byte{}, 0,
		0, 0, [16]byte{}, [16]byte{}, 0, 0, 0, 0, nil, make([]byte, 64)}

}

func (self *PublishLock) GetCommandType() uint8 {
	return self.CommandType
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
	subscriberId  uint32
	stream        *client.Stream
	protocol      *client.BinaryClientProtocol
	publishLock   *PublishLock
	closed        bool
	closedWaiter  chan bool
	wakeupSignal  chan bool
	uninitWaiter  chan bool
}

func NewSubscribeClient(manager *SubscribeManager) *SubscribeClient {
	return &SubscribeClient{manager, &sync.Mutex{}, "", 0, nil, nil,
		NewPublishLock(), false, make(chan bool, 1), nil, nil}
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
	self.leaderAddress = leaderAddress
	self.closed = false
	return nil
}

func (self *SubscribeClient) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}
	self.closed = true
	self.glock.Unlock()

	if self.protocol != nil {
		err := self.uninitSubscribe()
		if err != nil {
			self.manager.slock.Log().Errorf("Subscribe client close unSubscribe error %v", err)
		}
		_ = self.protocol.Close()
	}
	_ = self.WakeupRetryConnect()
	self.manager.slock.logger.Infof("Subscribe client %s close", self.leaderAddress)
	return nil
}

func (self *SubscribeClient) initSubscribe() error {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	command := protocol.NewSubscribeCommand(1, self.subscriberId, 0, lockKeyMask, 30, 67108864)
	err := self.protocol.Write(command)
	if err != nil {
		return err
	}
	resultCommand, err := self.protocol.ReadCommand()
	if err != nil {
		return err
	}

	if subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand); ok {
		if subscribeResultCommand.Result != 0 {
			return errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
		}

		self.subscriberId = subscribeResultCommand.SubscribeId
		return nil
	}
	return errors.New("unknown command")
}

func (self *SubscribeClient) uninitSubscribe() error {
	self.glock.Lock()
	if self.protocol == nil {
		self.glock.Unlock()
		return errors.New("closed")
	}
	if self.uninitWaiter != nil {
		self.glock.Unlock()
		return errors.New("uniniting")
	}
	if self.subscriberId == 0 {
		self.glock.Unlock()
		return nil
	}
	self.glock.Unlock()

	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	command := protocol.NewSubscribeCommand(1, self.subscriberId, 1, lockKeyMask, 30, 67108864)
	err := self.protocol.Write(command)
	if err != nil {
		return err
	}

	self.glock.Lock()
	uninitWaiter := self.uninitWaiter
	if uninitWaiter == nil {
		self.uninitWaiter = make(chan bool, 1)
		uninitWaiter = self.uninitWaiter
	}
	self.glock.Unlock()
	<-uninitWaiter
	self.uninitWaiter = nil

	if self.subscriberId != 0 {
		return errors.New("unsubscriber fail")
	}
	return nil
}

func (self *SubscribeClient) handleUninitSubscribe(buf []byte) error {
	subscribeResultCommand := protocol.SubscribeResultCommand{}
	err := subscribeResultCommand.Decode(buf)
	if err != nil {
		return err
	}
	if subscribeResultCommand.CommandType != protocol.COMMAND_SUBSCRIBE {
		return errors.New("unkonwn command")
	}

	if subscribeResultCommand.Result == 0 {
		self.subscriberId = 0
	}
	self.glock.Lock()
	if self.uninitWaiter != nil {
		close(self.uninitWaiter)
		self.uninitWaiter = nil
	}
	self.glock.Unlock()
	return nil
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
			if self.uninitWaiter != nil {
				close(self.uninitWaiter)
				self.uninitWaiter = nil
			}
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
			self.manager.slock.logger.Infof("Subscribe client connected leader %s with id %d", self.leaderAddress, self.subscriberId)
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

	self.glock.Lock()
	if self.uninitWaiter != nil {
		close(self.uninitWaiter)
		self.uninitWaiter = nil
	}
	self.glock.Unlock()
	close(self.closedWaiter)
	self.manager.glock.Lock()
	if self.manager.client == self {
		self.manager.client = nil
	}
	self.manager.glock.Unlock()
	self.manager.slock.logger.Infof("Subscribe client connect leader %s closed", self.leaderAddress)
}

func (self *SubscribeClient) Process() error {
	for !self.closed {
		buf := self.publishLock.buf
		n, err := self.stream.ReadBytes(buf)
		if err != nil {
			return err
		}
		if n != 64 || len(buf) != 64 {
			return errors.New("read buf size error")
		}

		if self.publishLock.buf[2] != protocol.COMMAND_PUBLISH {
			_ = self.handleUninitSubscribe(self.publishLock.buf)
			continue
		}
		err = self.publishLock.Decode()
		if err != nil {
			return err
		}
		if self.publishLock.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			data, derr := self.stream.ReadBytesFrame()
			if derr != nil {
				return derr
			}
			self.publishLock.data = data
		}

		db := self.manager.slock.GetDB(self.publishLock.DbId)
		if db == nil {
			db = self.manager.slock.GetOrNewDB(self.publishLock.DbId)
		}
		publishId := uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
		err = db.subscribeChannels[publishId%uint64(db.managerMaxGlocks)].ClientPush(self.publishLock)
		if err != nil {
			return err
		}
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
	clientId                   uint32
	subscriberId               uint32
	lockKeyMasks               [][2]uint64
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

func NewSubscriber(manager *SubscribeManager, serverProtocol ServerProtocol, clientId uint32, subscriberId uint32) *Subscriber {
	subscriber := &Subscriber{manager, &sync.Mutex{}, clientId, subscriberId, make([][2]uint64, 0),
		nil, nil, 0, serverProtocol, 0,
		serverProtocol.GetStream().closedWaiter, 0, 0,
		false, make(chan bool, 1), false, make(chan bool, 1)}
	go subscriber.Run()
	return subscriber
}

func (self *Subscriber) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	if self.serverProtocol != nil {
		stream := self.serverProtocol.GetStream()
		self.serverProtocol = nil
		if stream != nil {
			self.glock.Unlock()
			_ = stream.Close()
			self.glock.Lock()
		}
	}
	_ = self.manager.removeSubscriber(self)
	if self.pulled {
		close(self.pullWaiter)
		self.pulled = false
	}
	self.glock.Unlock()

	<-self.closedWaiter
	self.glock.Lock()
	currentBuffer := self.bufferHead
	for currentBuffer != nil {
		nextBuffer := currentBuffer.next
		self.manager.freeBuffer(currentBuffer)
		self.bufferSize -= len(currentBuffer.buf)
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
		if self.expriedTime > 0 && timeout > int(self.expriedTime) {
			timeout = int(self.expriedTime)
		}
		select {
		case <-self.pullWaiter:
			self.glock.Lock()
			if self.serverProtocol == nil {
				self.processCheck()
			} else {
				self.processLock()
			}
			self.glock.Unlock()
		case <-time.After(time.Duration(timeout) * time.Second):
			self.glock.Lock()
			if !self.pulled {
				<-self.pullWaiter
			}
			self.processCheck()
			self.glock.Unlock()
		case <-self.serverProtocolClosedWaiter:
			self.glock.Lock()
			if !self.pulled {
				<-self.pullWaiter
			}
			self.glock.Unlock()
			self.processServerProcotolClose()
		}
	}

	self.serverProtocol = nil
	close(self.closedWaiter)
}

func (self *Subscriber) processLock() {
	for self.bufferHead != nil && self.bufferHead.rindex < self.bufferHead.windex {
		serverProtocol := self.serverProtocol
		if serverProtocol == nil {
			return
		}
		stream := serverProtocol.GetStream()
		if stream == nil {
			self.processServerProcotolClose()
			return
		}
		self.glock.Unlock()

		serverProtocol.Lock()
		for {
			n, err := stream.Write(self.bufferHead.buf[self.bufferHead.rindex:self.bufferHead.windex])
			if err != nil {
				serverProtocol.Unlock()
				self.glock.Lock()
				go func() {
					_ = self.Close()
					self.manager.slock.Log().Errorf("Subscribe subscriber write stream error %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
				}()
				return
			} else {
				self.bufferHead.rindex += n
				if self.bufferHead.rindex >= self.bufferHead.windex {
					serverProtocol.Unlock()
					self.glock.Lock()
					break
				}
			}
		}

		if self.bufferHead == self.bufferTail && self.bufferHead.rindex == self.bufferHead.windex {
			self.bufferHead.rindex = 0
			self.bufferHead.windex = 0
		} else {
			if self.bufferHead.rindex >= len(self.bufferHead.buf) {
				buffer := self.bufferHead
				self.bufferHead = self.bufferHead.next
				self.manager.freeBuffer(buffer)
				self.bufferSize -= len(buffer.buf)
				if self.bufferHead == nil {
					self.bufferTail = nil
				}
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

	if self.closed {
		return
	}
	if self.serverProtocolClosedTime != 0 && uint32(time.Now().Unix()-self.serverProtocolClosedTime) > self.expriedTime {
		go func() {
			_ = self.Close()
			self.manager.slock.Log().Infof("Subscribe subscriber expried %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
		}()
	}
}

func (self *Subscriber) processServerProcotolClose() {
	if self.serverProtocol == nil {
		return
	}
	self.serverProtocol = nil
	self.serverProtocolClosedTime = time.Now().Unix()
	self.serverProtocolClosedWaiter = make(chan bool, 1)
	if self.expriedTime == 0 {
		go func() {
			_ = self.Close()
			self.manager.slock.Log().Infof("Subscribe subscriber stream closed %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
		}()
	}
}

func (self *Subscriber) Push(lock *PublishLock) error {
	if self.closed {
		return errors.New("closed")
	}

	hKey := uint64(lock.LockKey[0]) | uint64(lock.LockKey[1])<<8 | uint64(lock.LockKey[2])<<16 | uint64(lock.LockKey[3])<<24 | uint64(lock.LockKey[4])<<32 | uint64(lock.LockKey[5])<<40 | uint64(lock.LockKey[6])<<48 | uint64(lock.LockKey[7])<<56
	lKey := uint64(lock.LockKey[8]) | uint64(lock.LockKey[9])<<8 | uint64(lock.LockKey[10])<<16 | uint64(lock.LockKey[11])<<24 | uint64(lock.LockKey[12])<<32 | uint64(lock.LockKey[13])<<40 | uint64(lock.LockKey[14])<<48 | uint64(lock.LockKey[15])<<56
	for _, mask := range self.lockKeyMasks {
		if mask[0]&hKey == 0 && mask[1]&lKey == 0 {
			continue
		}

		versionId := uint32(0)
		if self.manager.slock.arbiterManager != nil {
			versionId = self.manager.slock.arbiterManager.version
		}
		lock.buf[11], lock.buf[12], lock.buf[13], lock.buf[14] = byte(versionId), byte(versionId>>8), byte(versionId>>16), byte(versionId>>24)
		lock.buf[15], lock.buf[16], lock.buf[17], lock.buf[18] = byte(self.subscriberId), byte(self.subscriberId>>8), byte(self.subscriberId>>16), byte(self.subscriberId>>24)

		self.glock.Lock()
		err := self.appendBufferData(lock.buf)
		if err != nil {
			go func() {
				_ = self.Close()
				self.manager.slock.Log().Errorf("Subscribe subscriber buffer fulled error %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
			}()
			self.glock.Unlock()
			return err
		}
		if lock.Flag&protocol.LOCK_FLAG_CONTAINS_DATA != 0 {
			err = self.appendBufferData(lock.data)
			if err != nil {
				go func() {
					_ = self.Close()
					self.manager.slock.Log().Errorf("Subscribe subscriber buffer fulled error %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
				}()
				self.glock.Unlock()
				return err
			}
		}

		if self.pulled {
			self.pullWaiter <- true
			self.pulled = false
		}
		self.glock.Unlock()
		return nil
	}
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
		self.pulled = false
	}

	if subscriberType == 0 {
		hasMask := false
		for _, mask := range self.lockKeyMasks {
			if mask == lkm {
				hasMask = true
				break
			}
		}
		if !hasMask {
			self.lockKeyMasks = append(self.lockKeyMasks, lkm)
		}
		self.expriedTime = expried
		self.maxSize = maxSize
	} else {
		publishIds := make([][2]uint64, 0)
		for _, mask := range self.lockKeyMasks {
			if mask != lkm {
				publishIds = append(publishIds, mask)
				break
			}
		}
		self.lockKeyMasks = publishIds
		if len(self.lockKeyMasks) == 0 {
			go func() {
				_ = self.Close()
				self.manager.slock.Log().Infof("Subscribe subscriber cancal closed %d %d %d", self.subscriberId, self.expriedTime, self.maxSize)
			}()
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *Subscriber) appendBufferData(buf []byte) error {
	dataLen, n := len(buf), 0
	for n < dataLen {
		if self.bufferTail == nil || self.bufferTail.windex >= len(self.bufferTail.buf) {
			if self.maxSize > 0 && uint32(self.bufferSize) >= self.maxSize {
				return errors.New("fulled")
			}
			buffer := self.manager.getBuffer()
			if self.bufferTail != nil {
				self.bufferTail.next = buffer
			}
			self.bufferTail = buffer
			self.bufferSize += len(self.bufferTail.buf)
			if self.bufferHead == nil {
				self.bufferHead = self.bufferTail
			}
		}

		bufLen := len(self.bufferTail.buf) - self.bufferTail.windex
		if bufLen >= dataLen-n {
			copy(self.bufferTail.buf[self.bufferTail.windex:], buf[n:])
			self.bufferTail.windex += dataLen - n
			return nil
		}
		copy(self.bufferTail.buf[self.bufferTail.windex:], buf[n:n+bufLen])
		self.bufferTail.windex += bufLen
		n += bufLen
	}
	return nil
}

type SubscribePublishLockQueue struct {
	buffer []*PublishLock
	rindex int
	windex int
	blen   int
	next   *SubscribePublishLockQueue
}

type SubscribeChannel struct {
	manager                 *SubscribeManager
	glock                   *sync.Mutex
	lockDb                  *LockDB
	lockDbGlockIndex        uint16
	lockDbGlock             *PriorityMutex
	queueHead               *SubscribePublishLockQueue
	queueTail               *SubscribePublishLockQueue
	queueCount              int
	queueWaiter             chan bool
	queueGlock              *sync.Mutex
	freeLocks               []*PublishLock
	freeLockIndex           int
	freeLockMax             int
	lockDbGlockAcquiredSize int
	lockDbGlockAcquired     bool
	queuePulled             bool
	closed                  bool
	closedWaiter            chan bool
}

func NewSubscribeChannel(manager *SubscribeManager, lockDb *LockDB, lockDbGlockIndex uint16, lockDbGlock *PriorityMutex) *SubscribeChannel {
	freeLockMax := int(Config.AofQueueSize) / 128
	return &SubscribeChannel{manager, &sync.Mutex{}, lockDb, lockDbGlockIndex, lockDbGlock, nil, nil,
		0, make(chan bool, 1), &sync.Mutex{}, make([]*PublishLock, freeLockMax),
		0, freeLockMax, freeLockMax * 4, false, false,
		false, make(chan bool, 1)}
}

func (self *SubscribeChannel) pushPublishLock(publishLock *PublishLock) {
	if self.queueTail == nil {
		self.queueTail = self.manager.getLockQueue()
		self.queueHead = self.queueTail
	} else if self.queueTail.windex >= self.queueTail.blen {
		self.queueTail.next = self.manager.getLockQueue()
		self.queueTail = self.queueTail.next
	}
	self.queueTail.buffer[self.queueTail.windex] = publishLock
	self.queueTail.windex++
	self.queueCount++
	if !self.lockDbGlockAcquired && self.queueCount > self.lockDbGlockAcquiredSize {
		self.lockDbGlockAcquired = self.lockDbGlock.LowSetPriority()
	}
	if self.queuePulled {
		self.queueWaiter <- true
		self.queuePulled = false
	}
}

func (self *SubscribeChannel) pullPublishLock() *PublishLock {
	if self.queueHead == nil {
		return nil
	}
	if self.queueHead == self.queueTail && self.queueHead.rindex == self.queueHead.windex {
		return nil
	}
	publishLock := self.queueHead.buffer[self.queueHead.rindex]
	self.queueHead.buffer[self.queueHead.rindex] = nil
	self.queueHead.rindex++
	self.queueCount--
	if self.queueHead.rindex == self.queueHead.windex {
		if self.queueHead == self.queueTail {
			self.queueHead.rindex, self.queueHead.windex = 0, 0
		} else {
			queue := self.queueHead
			self.queueHead = queue.next
			self.manager.freeLockQueue(queue)
			if self.queueHead == nil {
				self.queueTail = nil
			}
		}
	}

	if self.lockDbGlockAcquired && self.queueCount < self.lockDbGlockAcquiredSize {
		self.lockDbGlock.LowUnSetPriority()
		self.lockDbGlockAcquired = false
	}
	return publishLock
}

func (self *SubscribeChannel) Push(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8, data []byte) error {
	if self.closed {
		return io.EOF
	}
	if self.manager.slock.state != STATE_LEADER {
		return nil
	}
	if len(self.manager.fastSubscribers) == 0 {
		return nil
	}

	var publishLock *PublishLock = nil
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		publishLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
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
	if data != nil {
		publishLock.data = data
		publishLock.Flag |= protocol.LOCK_FLAG_CONTAINS_DATA
	}

	self.queueGlock.Lock()
	self.pushPublishLock(publishLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *SubscribeChannel) ClientPush(lock *PublishLock) error {
	if self.closed {
		return io.EOF
	}
	if len(self.manager.fastSubscribers) == 0 {
		return nil
	}

	var publishLock *PublishLock = nil
	self.glock.Lock()
	if self.freeLockIndex > 0 {
		self.freeLockIndex--
		publishLock = self.freeLocks[self.freeLockIndex]
		self.glock.Unlock()
	} else {
		self.glock.Unlock()
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
	publishLock.data = lock.data

	self.queueGlock.Lock()
	self.pushPublishLock(publishLock)
	self.queueGlock.Unlock()
	return nil
}

func (self *SubscribeChannel) Run() {
	self.manager.handeLockSubscribeChannel(self)
	for {
		self.queueGlock.Lock()
		publishLock := self.pullPublishLock()
		for publishLock != nil {
			self.queueGlock.Unlock()
			self.handle(publishLock)
			self.queueGlock.Lock()
			publishLock = self.pullPublishLock()
		}
		self.queuePulled = true
		self.queueGlock.Unlock()

		self.manager.waitLockSubscribeChannel(self)
		if self.closed {
			self.queueGlock.Lock()
			self.queuePulled = false
			if self.lockDbGlockAcquired {
				self.lockDbGlock.LowUnSetPriority()
				self.lockDbGlockAcquired = false
			}
			self.queueGlock.Unlock()
			self.manager.RemoveSubscribeChannel(self)
			close(self.closedWaiter)
			return
		}

		<-self.queueWaiter
		self.manager.handeLockSubscribeChannel(self)
	}
}

func (self *SubscribeChannel) handle(publishLock *PublishLock) {
	err := publishLock.Encode()
	if err == nil {
		for _, subscriber := range self.manager.fastSubscribers {
			err = subscriber.Push(publishLock)
			if err != nil {
				self.manager.slock.Log().Errorf("Subscribe subscriber push error %d %d %d", subscriber.subscriberId, subscriber.expriedTime, subscriber.maxSize)
			}
		}
	}

	self.glock.Lock()
	publishLock.data = nil
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
	subscribers        map[uint32]*Subscriber
	fastSubscribers    []*Subscriber
	leaderAddress      string
	client             *SubscribeClient
	freeBuffers        []*SubscribeBuffer
	freeBufferGlock    *sync.Mutex
	freeBufferIndex    int
	freeLockQueues     []*SubscribePublishLockQueue
	freeLockQueueGlock *sync.Mutex
	freeLockQueueIndex int
	publishId          uint64
	closed             bool
}

func NewSubscribeManager() *SubscribeManager {
	return &SubscribeManager{nil, &sync.Mutex{}, make([]*SubscribeChannel, 0), 0, 0,
		nil, make(map[uint32]*Subscriber, 64), nil, "",
		nil, make([]*SubscribeBuffer, 256), &sync.Mutex{}, 0,
		make([]*SubscribePublishLockQueue, 256), &sync.Mutex{}, 0,
		0, false}
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
		closedWaiter := self.client.closedWaiter
		_ = self.client.Close()
		<-closedWaiter
	}
	_ = self.WaitFlushSubscribeChannel()
	for _, subscriber := range self.fastSubscribers {
		_ = subscriber.Close()
	}
	self.freeBuffers = nil
	self.slock.logger.Infof("Subscribe closed")
}

func (self *SubscribeManager) NewSubscribeChannel(lockDb *LockDB, lockDbGlockIndex uint16, lockDbGlock *PriorityMutex) *SubscribeChannel {
	self.glock.Lock()
	subscribeChannel := NewSubscribeChannel(self, lockDb, lockDbGlockIndex, lockDbGlock)
	self.channels = append(self.channels, subscribeChannel)
	self.channelCount++
	self.glock.Unlock()
	go subscribeChannel.Run()
	return subscribeChannel
}

func (self *SubscribeManager) CloseSubscribeChannel(aofChannel *SubscribeChannel) *SubscribeChannel {
	aofChannel.queueGlock.Lock()
	aofChannel.closed = true
	if aofChannel.queuePulled {
		aofChannel.queueWaiter <- false
		aofChannel.queuePulled = false
	}
	aofChannel.queueGlock.Unlock()
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
		queueCount := 0
		for _, channel := range self.channels {
			channel.queueGlock.Lock()
			queueCount += channel.queueCount
			channel.queueGlock.Unlock()
		}

		if queueCount == 0 {
			if channelFlushWaiter == self.channelFlushWaiter {
				self.channelFlushWaiter = nil
			}
			self.glock.Unlock()
			return nil
		}
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
			if s.clientId != command.ClientId {
				self.glock.Unlock()
				return protocol.NewSubscribeResultCommand(command, protocol.RESULT_ERROR, s.subscriberId), nil
			}
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
		subscriber = NewSubscriber(self, serverProtocol, command.ClientId, subscriberIdIndex)
		err := self.addSubscriber(subscriber)
		if err != nil {
			self.glock.Unlock()
			return protocol.NewSubscribeResultCommand(command, protocol.RESULT_ERROR, subscriber.subscriberId), nil
		}
	}

	if self.leaderAddress != "" && self.client == nil {
		self.glock.Unlock()
		_ = self.openClient()
	} else {
		self.glock.Unlock()
	}
	err := subscriber.Update(serverProtocol, command.SubscribeType, command.LockKeyMask, command.Expried, command.MaxSize)
	if err != nil {
		if len(subscriber.lockKeyMasks) == 0 {
			_ = subscriber.Close()
		}
		return protocol.NewSubscribeResultCommand(command, protocol.RESULT_ERROR, subscriber.subscriberId), nil
	}
	if command.SubscribeType == 0 {
		self.slock.Log().Infof("Subscribe add subscribe %s %d %x %d %d", serverProtocol.GetStream().RemoteAddr().String(),
			subscriber.subscriberId, command.LockKeyMask, command.Expried, command.MaxSize)
	} else {
		self.slock.Log().Infof("Subscribe remove subscribe %s %d %x %d %d", serverProtocol.GetStream().RemoteAddr().String(),
			subscriber.subscriberId, command.LockKeyMask, command.Expried, command.MaxSize)
	}
	return protocol.NewSubscribeResultCommand(command, protocol.RESULT_SUCCED, subscriber.subscriberId), nil
}

func (self *SubscribeManager) addSubscriber(subscriber *Subscriber) error {
	self.subscribers[subscriber.subscriberId] = subscriber
	subscribers := make([]*Subscriber, 0, len(self.subscribers))
	for _, s := range self.subscribers {
		subscribers = append(subscribers, s)
	}
	self.fastSubscribers = subscribers
	return nil
}

func (self *SubscribeManager) removeSubscriber(subscriber *Subscriber) error {
	self.glock.Lock()
	if _, ok := self.subscribers[subscriber.subscriberId]; ok {
		delete(self.subscribers, subscriber.subscriberId)
		subscribers := make([]*Subscriber, 0, len(self.subscribers))
		for _, s := range self.subscribers {
			subscribers = append(subscribers, s)
		}
		self.fastSubscribers = subscribers
	}
	self.glock.Unlock()
	return nil
}

func (self *SubscribeManager) getBuffer() *SubscribeBuffer {
	self.freeBufferGlock.Lock()
	if self.freeBufferIndex > 0 {
		self.freeBufferIndex--
		buffer := self.freeBuffers[self.freeBufferIndex]
		self.freeBufferGlock.Unlock()
		return buffer
	}
	self.freeBufferGlock.Unlock()
	return &SubscribeBuffer{make([]byte, 4096), 0, 0, nil}
}

func (self *SubscribeManager) freeBuffer(buffer *SubscribeBuffer) {
	self.freeBufferGlock.Lock()
	if self.freeBufferIndex >= 256 {
		self.freeBufferGlock.Unlock()
		return
	}

	buffer.rindex, buffer.windex = 0, 0
	buffer.next = nil
	self.freeBuffers[self.freeBufferIndex] = buffer
	self.freeBufferIndex++
	self.freeBufferGlock.Unlock()
}

func (self *SubscribeManager) getLockQueue() *SubscribePublishLockQueue {
	self.freeLockQueueGlock.Lock()
	if self.freeLockQueueIndex > 0 {
		self.freeLockQueueIndex--
		queue := self.freeLockQueues[self.freeLockQueueIndex]
		self.freeLockQueueGlock.Unlock()
		return queue
	}

	bufSize := int(Config.AofQueueSize) / 64
	queue := &SubscribePublishLockQueue{make([]*PublishLock, bufSize), 0, 0, bufSize, nil}
	self.freeLockQueueGlock.Unlock()
	return queue
}

func (self *SubscribeManager) freeLockQueue(queue *SubscribePublishLockQueue) {
	self.freeLockQueueGlock.Lock()
	if self.freeLockQueueIndex >= 256 {
		self.freeLockQueueGlock.Unlock()
		return
	}

	queue.rindex, queue.windex = 0, 0
	queue.next = nil
	self.freeLockQueues[self.freeLockQueueIndex] = queue
	self.freeLockQueueIndex++
	self.freeLockQueueGlock.Unlock()
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
			c := self.client
			self.client = nil
			self.glock.Unlock()
			go func() {
				_ = c.Close()
			}()
			_ = self.openClient()
		} else {
			self.glock.Unlock()
		}
	}
	self.slock.Log().Infof("Subscribe finish change leader to %s", address)
	return nil
}
