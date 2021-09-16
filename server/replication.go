package server

import (
	"encoding/hex"
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

type ReplicationBufferQueue struct {
	buf 				[]byte
	segment_count 		int
	segment_size		int
	current_index		uint64
	max_index			uint64
	require_duplicated	bool
}

func NewReplicationBufferQueue() *ReplicationBufferQueue  {
	buf_size := 1024 * 1024
	max_index := uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff) % uint64(buf_size / 64)
	return &ReplicationBufferQueue{make([]byte, buf_size), buf_size / 64, 64, 0, max_index, false}
}

func (self *ReplicationBufferQueue) Reduplicated() *ReplicationBufferQueue {
	buf_size := self.segment_count * 2 * self.segment_size
	if uint(buf_size) > Config.AofRingBufferMaxSize {
		self.require_duplicated = false
		return self
	}

	max_index := uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff) % uint64(buf_size / 64)
	buffer_queue := &ReplicationBufferQueue{make([]byte, buf_size), buf_size / 64, 64, self.current_index, max_index, false}
	copy(buffer_queue.buf, self.buf)
	self.require_duplicated = false
	return buffer_queue
}

func (self *ReplicationBufferQueue) Push(buf []byte) error {
	if len(buf) != self.segment_size {
		return errors.New("buf size error")
	}

	current_index := atomic.LoadUint64(&self.current_index)
	current_size := int(current_index % uint64(self.segment_count)) * self.segment_size
	copy(self.buf[current_size:], buf)
	atomic.AddUint64(&self.current_index, 1)
	if current_index + 1 < self.max_index {
		return nil
	}

	for {
		if atomic.CompareAndSwapUint64(&self.current_index, current_index + 1, uint64(self.segment_count)) {
			break
		}
		current_index = atomic.LoadUint64(&self.current_index)
		if current_index < self.max_index {
			return nil
		}
	}
	return nil
}

func (self *ReplicationBufferQueue) Pop(segment_index uint64, buf []byte) error {
	current_index := atomic.LoadUint64(&self.current_index)
	for {
		if current_index == 0 || segment_index == current_index {
			return io.EOF
		}

		if segment_index/uint64(self.segment_count) != current_index/uint64(self.segment_count) {
			return errors.New("segment out of buf")
		}

		current_size := int(segment_index%uint64(self.segment_count)) * self.segment_size
		copy(buf, self.buf[current_size:current_size+self.segment_size])

		if self.current_index-current_index <= uint64(self.segment_count) {
			return nil
		}
		current_index = atomic.LoadUint64(&self.current_index)
	}
}

func (self *ReplicationBufferQueue) Head(buf []byte) (uint64, error) {
	current_index := atomic.LoadUint64(&self.current_index)
	for {
		if current_index == 0 {
			return 0, errors.New("buffer is empty")
		}

		current_size := int((current_index-1)%uint64(self.segment_count)) * self.segment_size
		copy(buf, self.buf[current_size:current_size+self.segment_size])

		if self.current_index-current_index <= uint64(self.segment_count) {
			return current_index-1, nil
		}
		current_index = atomic.LoadUint64(&self.current_index)
	}
}

func (self *ReplicationBufferQueue) Search(aof_id uint64, aof_buf []byte) (uint64, error) {
	current_index := atomic.LoadUint64(&self.current_index)
	if current_index == 0 {
		return 0, errors.New("search error")
	}

	start_index := uint64(0)
	segment_count := self.segment_count
	if current_index > uint64(self.segment_count) {
		start_index = current_index - uint64(self.segment_count) - 1
	} else {
		segment_count = int(current_index)
	}

	for i := 0; i < segment_count; i++ {
		current_size := int((start_index + uint64(i)) % uint64(self.segment_count)) * self.segment_size
		buf := self.buf[current_size: current_size + self.segment_size]
		current_aof_id := uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
		if current_aof_id == aof_id {
			copy(aof_buf, buf)
			current_aof_id := uint64(aof_buf[3]) | uint64(aof_buf[4])<<8 | uint64(aof_buf[5])<<16 | uint64(aof_buf[6])<<24 | uint64(aof_buf[7])<<32 | uint64(aof_buf[8])<<40 | uint64(aof_buf[9])<<48 | uint64(aof_buf[10])<<56
			if current_aof_id == aof_id {
				return start_index + uint64(i), nil
			}
		}
	}
	return 0, errors.New("search error")
}

type ReplicationClientChannel struct {
	manager 			*ReplicationManager
	stream 				*client.Stream
	protocol 			*client.TextClientProtocol
	aof 				*Aof
	aof_lock 			*AofLock
	connected_leader 	bool
	closed_waiter		chan bool
	closed 				bool
	recved_files		bool
}

func (self *ReplicationClientChannel) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", addr, 5 * time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	client_protocol := client.NewTextClientProtocol(stream)
	self.stream = stream
	self.protocol = client_protocol
	self.closed = false
	return nil
}

func (self *ReplicationClientChannel) Close() error {
	self.closed = true
	if self.protocol != nil {
		self.protocol.Close()
	}
	self.stream = nil
	self.protocol = nil
	return nil
}

func (self *ReplicationClientChannel) SendInitSyncCommand() (*protocol.TextResponseCommand, error) {
	args := []string{"SYNC"}
	if self.aof_lock != nil {
		args = append(args, fmt.Sprintf("%x", self.aof_lock.GetRequestId()))
		self.manager.slock.logger.Infof("Replication Send Start Sync %s", args[1])
	} else {
		self.manager.slock.logger.Infof("Replication Send Start Sync")
	}

	command := protocol.TextRequestCommand{Parser: self.protocol.GetParser(), Args: args}
	werr := self.protocol.Write(&command)
	if werr != nil {
		return nil, werr
	}

	result_command, rerr := self.protocol.Read()
	if rerr != nil {
		return nil, rerr
	}

	text_client_command := result_command.(*protocol.TextResponseCommand)
	if text_client_command.ErrorType != "" {
		if self.aof_lock != nil && text_client_command.ErrorType == "ERR_NOT_FOUND" {
			self.manager.slock.logger.Infof("Replication Resend File Sync")
			if self.aof_lock != nil {
				self.aof_lock = nil
			}
			self.recved_files = false
			return self.SendInitSyncCommand()
		}
		return nil, errors.New(text_client_command.ErrorType + " " + text_client_command.Message)
	}

	if len(text_client_command.Results) <= 0 {
		return nil, errors.New("unknown lastest requestid")
	}

	self.manager.slock.logger.Infof("Replication Recv Start Sync %s", text_client_command.Results[0])
	return text_client_command, nil
}

func (self *ReplicationClientChannel) InitSync() error {
	command, err := self.SendInitSyncCommand()
	if err != nil {
		return err
	}

	if self.aof_lock != nil {
		self.recved_files = true
		self.manager.slock.logger.Infof("Replication Recv Waiting")
		return nil
	}

	v, err := hex.DecodeString(command.Results[0])
	if err != nil {
		return err
	}
	inited_aof_id := uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 | uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
	aof_file_index := uint32(0)
	if inited_aof_id > 0 {
		aof_file_index = uint32(inited_aof_id >> 32) - 1
	}
	err = self.aof.Reset(aof_file_index)
	if err != nil {
		return err
	}
	self.aof_lock = &AofLock{buf: make([]byte, 64)}
	return self.HandleFiles()
}

func (self *ReplicationClientChannel) HandleFiles() error {
	defer func() {
		self.aof.is_rewriting = false
		if self.aof.rewrited_waiter != nil {
			self.aof.rewrited_waiter <- true
		}
	}()
	self.aof.is_rewriting = true

	var aof_file *AofFile = nil
	aof_index := uint32(0)
	for ; !self.closed; {
		err := self.ReadLock()
		if err != nil {
			return err
		}

		if self.aof_lock.AofIndex >= 0xffffffff && self.aof_lock.AofId >= 0xffffffff {
			if aof_file != nil {
				aof_file.Flush()
				err := aof_file.Close()
				if err != nil {
					return err
				}
			}
			self.recved_files = true
			self.manager.slock.logger.Infof("Replication Recv Files finish")
			return nil
		}

		if self.aof_lock.AofIndex != aof_index || aof_file == nil {
			if aof_file != nil {
				aof_file.Flush()
				err := aof_file.Close()
				if err != nil {
					return err
				}
			}

			aof_file, err = self.aof.OpenAofFile(aof_index)
			if err != nil {
				return err
			}
			aof_index = self.aof_lock.AofIndex
			if aof_index == 0 {
				self.manager.slock.logger.Infof("Replication Recv File rewrite.aof")
			} else {
				self.manager.slock.logger.Infof(fmt.Sprintf("Replication Recv File %s.%d", "append.aof", aof_index))
			}
		}

		err = self.aof.LoadLock(self.aof_lock)
		if err != nil {
			return err
		}
		err = aof_file.WriteLock(self.aof_lock)
		if err != nil {
			return err
		}
	}
	return io.EOF
}

func (self *ReplicationClientChannel) Handle() error {
	for ; !self.closed; {
		err := self.ReadLock()
		if err != nil {
			return err
		}

		err = self.aof.LoadLock(self.aof_lock)
		if err != nil {
			return err
		}
		self.aof.AppendLock(self.aof_lock)
	}
	return io.EOF
}

func (self *ReplicationClientChannel) ReadLock() error {
	buf := self.aof_lock.buf
	n, err := self.stream.Read(buf)
	if err != nil {
		return err
	}

	if n < 64 {
		for ; n < 64; {
			nn, nerr := self.stream.Read(buf[n:])
			if nerr != nil {
				return nerr
			}
			n += nn
		}
	}

	err = self.aof_lock.Decode()
	if err != nil {
		return err
	}
	return nil
}

type ReplicationServerChannel struct {
	manager 			*ReplicationManager
	stream 				*Stream
	protocol 			*TextServerProtocol
	aof 				*Aof
	raof_lock 			*AofLock
	waof_lock 			*AofLock
	buffer_index		uint64
	closed_waiter		chan bool
	closed 				bool
	sended_files		bool
}

func (self *ReplicationServerChannel) Close() error {
	self.closed = true
	if self.protocol != nil {
		self.protocol.Close()
	}
	self.stream = nil
	self.protocol = nil
	self.aof = nil
	self.raof_lock = nil
	self.waof_lock = nil
	return nil
}

func (self *ReplicationServerChannel) InitSync(args []string) error {
	if self.manager.slock.state != STATE_LEADER {
		self.protocol.stream.WriteBytes(self.protocol.parser.BuildResponse(false, "ERR state error", nil))
		return errors.New("state error")
	}

	if len(args) == 1 {
		buffer_index, err := self.manager.buffer_queue.Head(self.waof_lock.buf)
		if err != nil {
			self.waof_lock.AofIndex = self.aof.aof_file_index
			self.waof_lock.AofId = 0
		} else {
			err := self.waof_lock.Decode()
			if err != nil {
				return err
			}
		}
		request_id := fmt.Sprintf("%x", self.waof_lock.GetRequestId())
		err = self.protocol.stream.WriteBytes(self.protocol.parser.BuildResponse(true, "", []string{request_id}))
		if err != nil {
			return err
		}
		self.buffer_index = buffer_index
		self.manager.slock.logger.Infof("Replication Client Send Files Start %s %s", self.protocol.RemoteAddr().String(), request_id)
		go (func() {
			err := self.SendFiles()
			if err != nil {
				self.manager.slock.logger.Infof("Replication Client Send Files Error: %s %v", self.protocol.RemoteAddr().String(), err)
				self.Close()
				return
			}
		})()
		return nil
	}

	v, err := hex.DecodeString(args[1])
	if err != nil {
		self.protocol.stream.WriteBytes(self.protocol.parser.BuildResponse(false, "ERR decode request id error", nil))
		return err
	}
	inited_aof_id := uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 | uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
	buffer_index, serr := self.manager.buffer_queue.Search(inited_aof_id, self.waof_lock.buf)
	if serr != nil {
		self.protocol.stream.WriteBytes(self.protocol.parser.BuildResponse(false, "ERR_NOT_FOUND unknown request id", nil))
		return serr
	}

	err = self.waof_lock.Decode()
	if err != nil {
		return err
	}
	request_id := fmt.Sprintf("%x", self.waof_lock.GetRequestId())
	err = self.protocol.stream.WriteBytes(self.protocol.parser.BuildResponse(true, "", []string{request_id}))
	if err != nil {
		return err
	}
	self.buffer_index = buffer_index + 1
	self.sended_files = true
	self.manager.slock.logger.Infof("Replication Client Send Start %s %s", self.protocol.RemoteAddr().String(), request_id)
	return nil
}

func (self *ReplicationServerChannel) SendFiles() error {
	self.aof.Flush()

	append_files, rewrite_file, err := self.aof.FindAofFiles()
	if err != nil {
		return err
	}

	aof_filenames := make([]string, 0)
	if rewrite_file != "" {
		aof_filenames = append(aof_filenames, rewrite_file)
	}
	aof_filenames = append(aof_filenames, append_files...)
	err = self.aof.LoadAofFiles(aof_filenames, func (filename string, aof_file *AofFile, lock *AofLock, first_lock bool) (bool, error) {
		if lock.AofIndex >= self.waof_lock.AofIndex && lock.AofId >= self.waof_lock.AofId {
			return false, nil
		}

		err := self.stream.WriteBytes(lock.buf)
		if err != nil {
			return true, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	err = self.SendFilesFinish()
	if err != nil {
		return err
	}
	self.manager.slock.logger.Infof("Replication Client Send File finish %s", self.protocol.RemoteAddr().String())
	return self.manager.WakeupServerChannel()
}

func (self *ReplicationServerChannel) SendFilesFinish() error {
	self.waof_lock.AofIndex = 0xffffffff
	self.waof_lock.AofId = 0xffffffff
	err := self.waof_lock.Encode()
	if err != nil {
		return err
	}
	err = self.stream.WriteBytes(self.waof_lock.buf)
	if err != nil {
		return err
	}
	self.sended_files = true
	return nil
}

func (self *ReplicationServerChannel) HandleSendFilesQueue() error {
	bufs := make([][]byte, 0)
	for ; !self.closed && !self.sended_files; {
		buf := make([]byte, 64)
		err := self.manager.buffer_queue.Pop(self.buffer_index, buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.manager.server_channel_wait_count, 1)
				<- self.manager.server_channel_waiter
				continue
			}
			return err
		}

		self.buffer_index++
		bufs = append(bufs, buf)
	}

	if !self.closed {
		now := time.Now().Unix()
		for _, buf := range bufs {
			copy(self.waof_lock.buf, buf)
			err := self.waof_lock.Decode()
			if err != nil {
				return err
			}
			if self.waof_lock.ExpriedFlag & 0x4000 == 0 {
				if int64(self.waof_lock.CommandTime + uint64(self.waof_lock.ExpriedTime)) <= now {
					continue
				}
			}

			err = self.stream.WriteBytes(buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *ReplicationServerChannel) Handle() error {
	if !self.sended_files {
		err := self.HandleSendFilesQueue()
		if err != nil {
			return err
		}
	}

	for ; !self.closed; {
		if float64(self.manager.buffer_queue.current_index - self.buffer_index) > float64(self.manager.buffer_queue.segment_count) * 0.8 {
			self.manager.buffer_queue.require_duplicated = true
		}

		err := self.manager.buffer_queue.Pop(self.buffer_index, self.waof_lock.buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.manager.server_channel_wait_count, 1)
				<- self.manager.server_channel_waiter
				continue
			}
			return err
		}

		err = self.stream.WriteBytes(self.waof_lock.buf)
		if err != nil {
			return err
		}
		self.buffer_index++
	}
	return io.EOF
}

func (self *ReplicationServerChannel) Process() error {
	buf := self.raof_lock.buf
	for ; !self.closed; {
		n, err := self.stream.Read(buf)
		if err != nil {
			return err
		}

		if n < 64 {
			for ; n < 64; {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					return nerr
				}
				n += nn
			}
		}

		err = self.raof_lock.Decode()
		if err != nil {
			return err
		}
	}
	return io.EOF
}

type ReplicationManager struct {
	slock 						*SLock
	glock 						*sync.Mutex
	buffer_queue 				*ReplicationBufferQueue
	client_channel 				*ReplicationClientChannel
	server_channels 			[]*ReplicationServerChannel
	leader_address				string
	server_channel_wait_count 	uint32
	server_channel_waiter 		chan bool
	inited_waters				[]chan bool
	closed 						bool
}

func NewReplicationManager(address string) *ReplicationManager {
	return &ReplicationManager{nil, &sync.Mutex{}, NewReplicationBufferQueue(),
		nil, make([]*ReplicationServerChannel, 0), address, 0,
		make(chan bool, 8), make([]chan bool, 0), false}
}

func (self *ReplicationManager) GetHandlers() map[string]TextServerProtocolCommandHandler{
	handlers := make(map[string]TextServerProtocolCommandHandler, 64)
	handlers["SYNC"] = self.CommandHandleSyncCommand
	return handlers
}

func (self *ReplicationManager) Close() {
	self.glock.Lock()
	self.closed = true
	if self.client_channel != nil {
		self.client_channel.Close()
		self.glock.Unlock()
		<- self.client_channel.closed_waiter
		self.glock.Lock()
		self.client_channel = nil
	}

	self.WakeupServerChannel()
	for _, channel := range self.server_channels {
		channel.Close()
		self.glock.Unlock()
		<- channel.closed_waiter
		self.glock.Lock()
	}
	self.server_channels = self.server_channels[:0]

	for _, waiter := range self.inited_waters {
		waiter <- false
	}
	self.inited_waters = self.inited_waters[:0]
	self.glock.Unlock()
	self.slock.logger.Infof("Replication Closed")
}

func (self *ReplicationManager) CommandHandleSyncCommand(server_protocol *TextServerProtocol, args []string) error {
	channel := &ReplicationServerChannel{self, server_protocol.stream, server_protocol,
		self.slock.GetAof(), &AofLock{buf: make([]byte, 64)}, &AofLock{buf: make([]byte, 64)},
		0, make(chan bool, 1),
		false, false}
	self.slock.logger.Infof("Replication Client Sync %s", server_protocol.RemoteAddr().String())
	err := channel.InitSync(args)
	if err != nil {
		channel.closed = true
		channel.closed_waiter <- true
		if err != io.EOF {
			self.slock.logger.Errorf("Replication Client Start Sync Error: %s %v", server_protocol.RemoteAddr().String(), err)
		}
		return io.EOF
	}

	self.AddServerChannel(channel)
	go func() {
		err := channel.Handle()
		if err != nil {
			if err != io.EOF && !self.closed {
				self.slock.logger.Errorf("Replication Client Sync Error: %s %v", server_protocol.RemoteAddr().String(), err)
			}
		}
		channel.Close()
	}()
	err = channel.Process()
	channel.closed = true
	self.WakeupServerChannel()
	self.RemoveServerChannel(channel)
	channel.closed_waiter <- true
	if err != nil {
		if err != io.EOF && !self.closed {
			self.slock.logger.Errorf("Replication Client Process Error: %s %v", server_protocol.RemoteAddr().String(), err)
		}
		self.slock.logger.Infof("Replication Client Close %s", server_protocol.RemoteAddr().String())
		return io.EOF
	}
	return nil
}

func (self *ReplicationManager) AddServerChannel(channel *ReplicationServerChannel) error {
	self.glock.Lock()
	self.server_channels = append(self.server_channels, channel)
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) RemoveServerChannel(channel *ReplicationServerChannel) error {
	self.glock.Lock()
	server_channels := make([]*ReplicationServerChannel, 0)
	for _, c := range self.server_channels {
		if channel != c {
			server_channels = append(server_channels, c)
		}
	}
	self.server_channels = server_channels
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) StartSync() error {
	if self.leader_address == "" {
		return errors.New("slaveof is empty")
	}

	channel := &ReplicationClientChannel{self, nil, nil, self.slock.GetAof(),
		nil, true, make(chan bool, 1), false, false}

	self.slock.logger.Infof("Replication Connect Leader: %s", self.leader_address)
	err := channel.Open(self.leader_address)
	if err != nil {
		return err
	}
	self.client_channel = channel

	go func() {
		for ; !self.closed && self.slock.state != STATE_LEADER; {
			err = channel.InitSync()
			if err != nil {
				if err != io.EOF {
					self.slock.logger.Errorf("Replication Init Sync Error: %v", err)
				}
			} else {
				self.InitSynced()
				err = channel.Handle()
				if err != nil {
					if err != io.EOF && !self.closed {
						self.slock.logger.Errorf("Replication Sync Error: %v", err)
					}
				}
			}

			channel.Close()
			self.glock.Lock()
			self.client_channel = nil
			for _, waiter := range self.inited_waters {
				waiter <- false
			}
			self.inited_waters = self.inited_waters[:0]
			self.glock.Unlock()

			for ; !self.closed && self.slock.state != STATE_LEADER; {
				time.Sleep(5 * time.Second)
				self.slock.logger.Infof("Replication Connect Leader: %s", self.leader_address)
				err := channel.Open(self.leader_address)
				if err == nil {
					self.client_channel = channel
					break
				}
				self.slock.logger.Errorf("Replication Reconnect Leader Error: %v", err)
				channel.Close()
				self.glock.Lock()
				for _, waiter := range self.inited_waters {
					waiter <- false
				}
				self.inited_waters = self.inited_waters[:0]
				self.glock.Unlock()
			}
		}

		channel.closed = true
		channel.closed_waiter <- true
	}()
	return nil
}

func (self *ReplicationManager) PushLock(lock *AofLock) error {
	if self.buffer_queue.require_duplicated {
		self.buffer_queue = self.buffer_queue.Reduplicated()
	}

	err := self.buffer_queue.Push(lock.buf)
	if err != nil {
		return err
	}

	return self.WakeupServerChannel()
}

func (self *ReplicationManager) WakeupServerChannel() error {
	if atomic.CompareAndSwapUint32(&self.server_channel_wait_count, 0, 0) {
		return nil
	}

	for {
		for i := uint32(0); i < self.server_channel_wait_count; i++ {
			self.server_channel_waiter <- true
			atomic.AddUint32(&self.server_channel_wait_count, 0xffffffff)
		}

		if atomic.CompareAndSwapUint32(&self.server_channel_wait_count, 0, 0) {
			break
		}
	}
	return nil
}

func (self *ReplicationManager) WaitInitSynced(waiter chan bool) {
	self.glock.Lock()
	self.inited_waters = append(self.inited_waters, waiter)
	self.glock.Unlock()
}

func (self *ReplicationManager) InitSynced() {
	self.glock.Lock()
	self.slock.UpdateState(STATE_FOLLOWER)
	for _, waiter := range self.inited_waters {
		waiter <- true
	}
	self.inited_waters = self.inited_waters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) SwitchToLeader() error {
	if self.slock.state != STATE_FOLLOWER {
		return errors.New("state error")
	}
	self.slock.UpdateState(STATE_LEADER)
	if self.client_channel != nil {
		self.client_channel.Close()
		self.client_channel = nil
	}
	return nil
}

func (self *ReplicationManager) SwitchToFollower() error {
	self.slock.UpdateState(STATE_FOLLOWER)
	defer self.slock.glock.Unlock()
	self.slock.glock.Lock()

	err := self.slock.server.CloseStreams()
	if err != nil {
		return err
	}
	for _, db := range self.slock.dbs {
		if db != nil {
			db.Close()
		}
	}
	err = self.StartSync()
	if err != nil {
		return err
	}
	return nil
}