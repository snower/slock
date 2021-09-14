package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/client"
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
	require_duplicated	bool
}

func NewReplicationBufferQueue() *ReplicationBufferQueue  {
	buf_size := 1024 * 1024
	return &ReplicationBufferQueue{make([]byte, buf_size), buf_size / 64, 64, 0, false}
}

func (self *ReplicationBufferQueue) Reduplicated() *ReplicationBufferQueue {
	buf_size := self.segment_count * 2 * self.segment_size
	buffer_queue := &ReplicationBufferQueue{make([]byte, buf_size), buf_size / 64, 64, self.current_index, false}
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
	return nil
}

func (self *ReplicationBufferQueue) Pop(segment_index uint64, buf []byte) error {
	current_index := atomic.LoadUint64(&self.current_index)
	for {
		if segment_index == current_index-1 {
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
			return current_index, nil
		}
		current_index = atomic.LoadUint64(&self.current_index)
	}
}

func (self *ReplicationBufferQueue) Search(aof_id uint64, aof_buf []byte) (uint64, error) {
	current_index := atomic.LoadUint64(&self.current_index)
	start_index := uint64(0)
	segment_count := self.segment_count
	if current_index > uint64(self.segment_count) {
		start_index = current_index - 1 - uint64(self.segment_count)
	} else {
		segment_count = int(current_index)
	}

	for i := 0; i < segment_count; i++ {
		current_size := int((start_index + uint64(i)) % uint64(self.segment_count)) * self.segment_size
		buf := self.buf[start_index: current_size + self.segment_size]
		current_aof_id := uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
		if current_aof_id == aof_id {
			copy(aof_buf, buf)
			return start_index + uint64(i), nil
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
	inited_aof_id 		uint64
	connected_leader 	bool
	closed 				bool
}

func (self *ReplicationClientChannel) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	client_protocol := client.NewTextClientProtocol(stream)
	self.stream = stream
	self.protocol = client_protocol
	return nil
}

func (self *ReplicationClientChannel) Close() error {
	self.closed = true
	err := self.protocol.Close()
	self.manager = nil
	self.stream = nil
	self.protocol = nil
	self.aof_lock = nil
	return err
}

func (self *ReplicationClientChannel) SendInitSyncCommand() error {
	args := []string{"SYNC"}
	if self.aof_lock != nil {
		args = append(args, fmt.Sprintf("%x", self.aof_lock.GetRequestId()))
	}
	command := client.TextClientCommand{self.protocol.GetParser(), 0, "", args, ""}
	werr := self.protocol.Write(&command)
	if werr != nil {
		return werr
	}

	result_command, rerr := self.protocol.Read()
	if rerr != nil {
		return rerr
	}

	text_client_command := result_command.(*client.TextClientCommand)
	if text_client_command.ErrorType != "" {
		if self.aof_lock != nil && text_client_command.ErrorType == "ERR_NOT_FOUND" {
			if self.aof_lock != nil {
				self.aof_lock = nil
			}
			return self.SendInitSyncCommand()
		}
		return errors.New(text_client_command.ErrorType + " " + text_client_command.Message)
	}

	if len(text_client_command.Args) <= 0 {
		return errors.New("unknown lastest requestid")
	}

	v, err := hex.DecodeString(text_client_command.Args[0])
	if err != nil {
		return err
	}
	self.inited_aof_id = uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 | uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
	self.aof_lock = &AofLock{buf: make([]byte, 64)}
	return nil
}

func (self *ReplicationClientChannel) InitSync() error {
	err := self.SendInitSyncCommand()
	if err != nil {
		return err
	}
	err = self.aof.Reset()
	if err != nil {
		return err
	}

	for ; !self.closed; {
		err := self.HandleRead()
		if err != nil {
			return err
		}

		err = self.HandleLock()
		if err != nil {
			return err
		}

		aof_id := uint64(self.aof_lock.AofIndex) << 32 | uint64(self.aof_lock.AofId)
		if aof_id >= self.inited_aof_id {
			return nil
		}
	}
	return io.EOF
}

func (self *ReplicationClientChannel) Handle() error {
	for ; !self.closed; {
		err := self.HandleRead()
		if err != nil {
			return err
		}

		err = self.HandleLock()
		if err != nil {
			return err
		}
	}
	return io.EOF
}

func (self *ReplicationClientChannel) HandleRead() error {
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

func (self *ReplicationClientChannel) HandleLock() error {
	self.aof.AppendLock(self.aof_lock)
	return self.aof.LoadLock(self.aof_lock)
}

type ReplicationServerChannel struct {
	manager 			*ReplicationManager
	stream 				*Stream
	protocol 			*TextServerProtocol
	aof 				*Aof
	aof_lock 			*AofLock
	buffer_index		uint64
	closed 				bool
}

func (self *ReplicationServerChannel) Close() error {
	self.closed = true
	err := self.protocol.Close()
	self.manager = nil
	self.stream = nil
	self.protocol = nil
	self.aof = nil
	self.aof_lock = nil
	return err
}

func (self *ReplicationServerChannel) InitSync(args []string) error {
	if self.manager.slock.state != STATE_LEADER {
		self.protocol.stream.WriteBytes(self.protocol.parser.Build(false, "ERR state error", nil))
		return errors.New("state error")
	}

	if len(args) == 1 {
		_, err := self.manager.buffer_queue.Head(self.aof_lock.buf)
		request_id := "00000000000000000000000000000000"
		if err == nil {
			request_id = fmt.Sprintf("%x", self.aof_lock.GetRequestId())
		}
		err = self.protocol.stream.WriteBytes(self.protocol.parser.Build(true, "", []string{request_id}))
		if err != nil {
			return err
		}
		return nil
	}

	v, err := hex.DecodeString(args[1])
	if err != nil {
		self.protocol.stream.WriteBytes(self.protocol.parser.Build(false, "ERR decode request id error", nil))
		return err
	}
	inited_aof_id := uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 | uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
	buffer_index, serr := self.manager.buffer_queue.Search(inited_aof_id, self.aof_lock.buf)
	if serr != nil {
		self.protocol.stream.WriteBytes(self.protocol.parser.Build(false, "ERR_NOT_FOUND unknown request id", nil))
		return serr
	}

	self.buffer_index = buffer_index
	request_id := fmt.Sprintf("%x", self.aof_lock.GetRequestId())
	err = self.protocol.stream.WriteBytes(self.protocol.parser.Build(true, "", []string{request_id}))
	if err != nil {
		return err
	}

	return self.SendFiles()
}

func (self *ReplicationServerChannel) SendFiles() error {
	buffer_index, err := self.manager.buffer_queue.Head(self.aof_lock.buf)
	if err != nil {
		return err
	}
	self.buffer_index = buffer_index
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
		if lock.AofIndex == self.aof_lock.AofIndex && lock.AofId == self.aof_lock.AofId {
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

	return nil
}

func (self *ReplicationServerChannel) Handle() error {
	for ; !self.closed; {
		if float64(self.manager.buffer_queue.current_index - self.buffer_index) > float64(self.manager.buffer_queue.segment_count) * 0.8 {
			self.manager.buffer_queue.require_duplicated = true
		}

		err := self.manager.buffer_queue.Pop(self.buffer_index, self.aof_lock.buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.manager.server_channel_wait_count, 1)
				<- self.manager.server_channel_waiter
				continue
			}
			return err
		}

		err = self.stream.WriteBytes(self.aof_lock.buf)
		if err != nil {
			return err
		}
	}
	return nil
}

type ReplicationManager struct {
	slock 						*SLock
	glock 						*sync.Mutex
	buffer_queue 				*ReplicationBufferQueue
	client_channel 				*ReplicationClientChannel
	server_channels 			[]*ReplicationServerChannel
	server_channel_wait_count 	uint32
	server_channel_waiter 		chan bool
	closed 						bool
}

func NewReplicationManager() *ReplicationManager {
	return &ReplicationManager{nil, &sync.Mutex{}, NewReplicationBufferQueue(),
		nil, make([]*ReplicationServerChannel, 0), 0,
		make(chan bool, 8), false}
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
		self.client_channel = nil
	}

	self.WakeupServerChannel()
	for _, channel := range self.server_channels {
		channel.Close()
	}
	self.server_channels = self.server_channels[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) CommandHandleSyncCommand(server_protocol *TextServerProtocol, args []string) error {
	channel := &ReplicationServerChannel{self, server_protocol.stream, server_protocol,
		self.slock.GetAof(), &AofLock{buf: make([]byte, 64)}, 0, false}
	err := channel.InitSync(args)
	if err != nil {
		return io.EOF
	}

	self.glock.Lock()
	self.server_channels = append(self.server_channels, channel)
	self.glock.Unlock()
	err = channel.Handle()
	if err != nil {
		return io.EOF
	}
	return nil
}

func (self *ReplicationManager) StartSync() error {
	if Config.SlaveOf == "" {
		return errors.New("slaveof is empty")
	}

	channel := &ReplicationClientChannel{self, nil, nil, self.slock.GetAof(),
		nil, 0, true, false}
	err := channel.Open(Config.SlaveOf)
	if err != nil {
		return err
	}
	self.client_channel = channel

	go func() {
		for ; !self.closed; {
			err = channel.InitSync()
			if err == nil {
				self.slock.UpdateState(STATE_FOLLOWER)
				err = channel.Handle()
				if err == io.EOF {
					return
				}
			}

			channel.Close()
			time.Sleep(30 * time.Second)
			for ; !self.closed; {
				err := channel.Open(Config.SlaveOf)
				if err == nil {
					self.client_channel = channel
					break
				}
				time.Sleep(30 * time.Second)
			}
		}
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