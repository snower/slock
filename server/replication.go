package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/server/protobuf"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationBufferQueue struct {
	manager 			*ReplicationManager
	glock 				*sync.RWMutex
	buf 				[]byte
	segment_count 		int
	segment_size		int
	current_index		uint64
	max_index			uint64
	require_duplicated	bool
}

func NewReplicationBufferQueue(manager *ReplicationManager, buf_size int) *ReplicationBufferQueue  {
	max_index := uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff) % uint64(buf_size / 64)
	return &ReplicationBufferQueue{manager, &sync.RWMutex{}, make([]byte, buf_size), buf_size / 64, 64, 0, max_index, false}
}

func (self *ReplicationBufferQueue) Reduplicated() *ReplicationBufferQueue {
	buf_size := self.segment_count * 2 * self.segment_size
	if uint(buf_size) > Config.AofRingBufferMaxSize {
		self.require_duplicated = false
		return self
	}

	buf := make([]byte, buf_size)
	self.glock.Lock()
	copy(buf, self.buf)
	copy(buf[self.segment_count * self.segment_size:], self.buf)
	self.buf = buf
	self.segment_count = buf_size / 64
	self.max_index = uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff) % uint64(buf_size / 64)
	self.require_duplicated = false
	self.glock.Unlock()
	self.manager.slock.Log().Infof("Replication Aof Ring Buffer Reduplicated %d To %d", buf_size / 2, buf_size)
	return self
}

func (self *ReplicationBufferQueue) Push(buf []byte) error {
	self.glock.Lock()
	current_size := int(self.current_index % uint64(self.segment_count)) * self.segment_size
	copy(self.buf[current_size:], buf)
	self.current_index++
	if self.current_index >= self.max_index {
		self.current_index = uint64(self.segment_count)
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationBufferQueue) Pop(segment_index uint64, buf []byte) error {
	self.glock.RLock()
	if self.current_index == 0 || segment_index == self.current_index {
		self.glock.RUnlock()
		return io.EOF
	}

	if self.current_index - segment_index > uint64(self.segment_count) {
		self.glock.RUnlock()
		return errors.New("segment out of buf")
	}

	current_size := int(segment_index % uint64(self.segment_count)) * self.segment_size
	copy(buf, self.buf[current_size: current_size + self.segment_size])
	self.glock.RUnlock()
	return nil
}

func (self *ReplicationBufferQueue) Head(buf []byte) (uint64, error) {
	self.glock.RLock()
	if self.current_index == 0 {
		self.glock.RUnlock()
		return 0, errors.New("buffer is empty")
	}

	current_size := int((self.current_index - 1) % uint64(self.segment_count)) * self.segment_size
	copy(buf, self.buf[current_size: current_size+self.segment_size])
	self.glock.RUnlock()
	return self.current_index - 1, nil
}

func (self *ReplicationBufferQueue) Search(aof_id [16]byte, aof_buf []byte) (uint64, error) {
	self.glock.RLock()
	if self.current_index == 0 {
		self.glock.RUnlock()
		return 0, errors.New("search error")
	}

	start_index := uint64(0)
	segment_count := self.segment_count
	if self.current_index > uint64(self.segment_count) {
		start_index = self.current_index - uint64(self.segment_count) - 1
	} else {
		segment_count = int(self.current_index)
	}

	for i := 0; i < segment_count; i++ {
		current_size := int((start_index + uint64(i)) % uint64(self.segment_count)) * self.segment_size
		buf := self.buf[current_size: current_size + self.segment_size]
		current_aof_id := [16]byte{buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]}
		if current_aof_id == aof_id {
			copy(aof_buf, buf)
			current_aof_id := [16]byte{buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]}
			if current_aof_id == aof_id {
				self.glock.RUnlock()
				return start_index + uint64(i), nil
			}
		}
	}

	self.glock.RUnlock()
	return 0, errors.New("search error")
}

type ReplicationClient struct {
	manager 			*ReplicationManager
	glock 				*sync.Mutex
	stream 				*client.Stream
	protocol 			*client.BinaryClientProtocol
	aof 				*Aof
	aof_lock 			*AofLock
	current_request_id	[16]byte
	rbufs				[][]byte
	rbuf_index			int
	rbuf_channel		chan []byte
	wbuf				[]byte
	loaded_count		uint64
	wakeup_signal		chan bool
	closed_waiter		chan bool
	closed 				bool
	connected_leader 	bool
	recved_files		bool
}

func NewReplicationClient(manager *ReplicationManager) *ReplicationClient {
	channel := &ReplicationClient{manager, &sync.Mutex{}, nil, nil, manager.slock.GetAof(),
		nil, [16]byte{}, make([][]byte, 16), 0, make(chan []byte, 8),
		make([]byte, 64), 0, nil, make(chan bool, 1), false, true, false}
	for i := 0; i < 16; i++ {
		channel.rbufs[i] = make([]byte, 64)
	}
	return channel
}

func (self *ReplicationClient) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", addr, 2 * time.Second)
	if err != nil {
		return err
	}
	stream := client.NewStream(conn)
	client_protocol := client.NewBinaryClientProtocol(stream)
	self.stream = stream
	self.protocol = client_protocol
	self.closed = false
	return nil
}

func (self *ReplicationClient) Close() error {
	self.closed = true
	if self.protocol != nil {
		self.protocol.Close()
	}
	self.stream = nil
	self.protocol = nil
	self.WakeupRetryConnect()
	return nil
}

func (self *ReplicationClient) Run() {
	self.current_request_id = self.manager.current_request_id
	for ; !self.closed; {
		self.manager.slock.logger.Infof("Replication Connect Leader: %s", self.manager.leader_address)
		err := self.Open(self.manager.leader_address)
		if err != nil {
			self.manager.slock.logger.Errorf("Replication Reconnect Leader Error: %v", err)
			if self.protocol != nil {
				self.protocol.Close()
			}
			self.stream = nil
			self.protocol = nil
			self.manager.WakeupInitSyncedWaiters()
			if self.closed {
				break
			}
			self.SleepWhenRetryConnect()
			continue
		}

		err = self.InitSync()
		if err != nil {
			if err != io.EOF {
				self.manager.slock.logger.Errorf("Replication Init Sync Error: %v", err)
			}
		} else {
			self.manager.ClientSycnInited()
			self.manager.slock.logger.Infof("Replication Connected Leader: %s", self.manager.leader_address)
			err = self.Process()
			if err != nil {
				if err != io.EOF && !self.closed {
					self.manager.slock.logger.Errorf("Replication Sync Error %v", err)
				}
			}
		}

		if self.protocol != nil {
			self.protocol.Close()
		}
		self.stream = nil
		self.protocol = nil
		self.manager.WakeupInitSyncedWaiters()
		if self.closed {
			break
		}
		self.SleepWhenRetryConnect()
	}

	close(self.closed_waiter)
	self.manager.client_channel = nil
	self.manager.current_request_id = self.current_request_id
	self.manager.slock.logger.Infof("Replication Close Connection Leader %s", self.manager.leader_address)
}

func (self *ReplicationClient) SendSyncCommand() (*protobuf.SyncResponse, error) {
	request_id := fmt.Sprintf("%x", self.current_request_id)
	if request_id != "00000000000000000000000000000000" {
		if self.aof_lock == nil {
			self.aof_lock = NewAofLock()
		}
		self.manager.slock.logger.Infof("Replication Send Start Sync %s", request_id)
	} else {
		request_id = ""
		self.manager.slock.logger.Infof("Replication Send Start Sync")
	}

	request := protobuf.SyncRequest{AofId:request_id}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	command := protocol.NewCallCommand("SYNC", data)
	werr := self.protocol.Write(command)
	if werr != nil {
		return nil, werr
	}

	result_command, rerr := self.protocol.Read()
	if rerr != nil {
		return nil, rerr
	}

	call_result_command, ok := result_command.(*protocol.CallResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}

	if call_result_command.Result != 0 || call_result_command.ErrType != "" {
		if call_result_command.Result == 0 && call_result_command.ErrType == "ERR_NOT_FOUND" {
			self.current_request_id = [16]byte{}
			self.manager.slock.logger.Infof("Replication Resend File Sync")
			self.aof_lock = nil
			self.recved_files = false
			return self.SendSyncCommand()
		}
		return nil, errors.New(call_result_command.ErrType)
	}

	response := protobuf.SyncResponse{}
	err = response.Unmarshal(call_result_command.Data)
	if err != nil {
		return nil, errors.New("unknown lastest requestid")
	}
	self.manager.slock.logger.Infof("Replication Recv Start Sync %s", response.AofId)
	return &response, nil
}

func (self *ReplicationClient) InitSync() error {
	sync_response, err := self.SendSyncCommand()
	if err != nil {
		return err
	}

	if self.aof_lock != nil {
		err = self.SendStarted()
		if err != nil {
			return err
		}
		self.recved_files = true
		self.manager.slock.logger.Infof("Replication Recv Waiting From %x", self.current_request_id)
		return nil
	}

	buf, err := hex.DecodeString(sync_response.AofId)
	if err != nil {
		return err
	}
	aof_file_index := uint32(buf[4]) | uint32(buf[5])<<8 | uint32(buf[6])<<16 | uint32(buf[7])<<24
	if aof_file_index > 0 {
		aof_file_index = aof_file_index - 1
	}
	err = self.aof.Reset(aof_file_index)
	if err != nil {
		return err
	}
	err = self.manager.FlushDB()
	if err != nil {
		return err
	}

	self.aof_lock = NewAofLock()
	err = self.SendStarted()
	if err != nil {
		return err
	}

	self.current_request_id[0], self.current_request_id[1], self.current_request_id[2], self.current_request_id[3], self.current_request_id[4], self.current_request_id[5], self.current_request_id[6], self.current_request_id[7],
		self.current_request_id[8], self.current_request_id[9], self.current_request_id[10], self.current_request_id[11], self.current_request_id[12], self.current_request_id[13], self.current_request_id[14], self.current_request_id[15] = buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
		buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]
	return self.RecvFiles()
}

func (self *ReplicationClient) SendStarted() error {
	aof_lock := NewAofLock()
	aof_lock.CommandType = protocol.COMMAND_INIT
	aof_lock.AofIndex = 0xffffffff
	aof_lock.AofId = 0xffffffff
	err := aof_lock.Encode()
	if err != nil {
		return err
	}
	self.glock.Lock()
	err = self.stream.WriteBytes(aof_lock.buf)
	self.glock.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationClient) RecvFiles() error {
	defer func() {
		self.aof.glock.Lock()
		self.aof.is_rewriting = false
		if self.aof.rewrited_waiter != nil {
			close(self.aof.rewrited_waiter)
			self.aof.rewrited_waiter = nil
		}
		self.aof.glock.Unlock()
	}()
	self.aof.WaitRewriteAofFiles()
	self.aof.glock.Lock()
	self.aof.is_rewriting = true
	self.aof.glock.Unlock()

	var aof_file *AofFile = nil
	aof_index := uint32(0)
	for ; !self.closed; {
		err := self.ReadLock()
		if err != nil {
			return err
		}

		if self.aof_lock.CommandType == protocol.COMMAND_INIT && self.aof_lock.AofIndex == 0xffffffff && self.aof_lock.AofId == 0xffffffff {
			if aof_file != nil {
				aof_file.Flush()
				err := aof_file.Close()
				if err != nil {
					return err
				}
			}
			self.recved_files = true
			self.manager.slock.logger.Infof("Replication Recv Files finish When %x", self.current_request_id)
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

		if self.aof_lock.AofFlag & 0x1000 != 0 {
			self.aof_lock.AofFlag &= 0xEFFF
		}
		err = self.aof.LoadLock(self.aof_lock)
		if err != nil {
			return err
		}
		err = aof_file.WriteLock(self.aof_lock)
		if err != nil {
			return err
		}
		self.manager.buffer_queue.Push(self.aof_lock.buf)
		self.loaded_count++

		buf := self.aof_lock.buf
		self.current_request_id[0], self.current_request_id[1], self.current_request_id[2], self.current_request_id[3], self.current_request_id[4], self.current_request_id[5], self.current_request_id[6], self.current_request_id[7],
			self.current_request_id[8], self.current_request_id[9], self.current_request_id[10], self.current_request_id[11], self.current_request_id[12], self.current_request_id[13], self.current_request_id[14], self.current_request_id[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}
	return io.EOF
}

func (self *ReplicationClient) Process() error {
	go self.ReadProcess()

	for ; !self.closed; {
		err := self.GetLock()
		if err != nil {
			return err
		}

		err = self.aof.LoadLock(self.aof_lock)
		if err != nil {
			return err
		}
		self.aof.AppendLock(self.aof_lock)
		self.manager.buffer_queue.Push(self.aof_lock.buf)
		self.loaded_count++

		buf := self.aof_lock.buf
		self.current_request_id[0], self.current_request_id[1], self.current_request_id[2], self.current_request_id[3], self.current_request_id[4], self.current_request_id[5], self.current_request_id[6], self.current_request_id[7],
			self.current_request_id[8], self.current_request_id[9], self.current_request_id[10], self.current_request_id[11], self.current_request_id[12], self.current_request_id[13], self.current_request_id[14], self.current_request_id[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}
	return io.EOF
}

func (self *ReplicationClient) ReadLock() error {
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

func (self *ReplicationClient) GetLock() error {
	for {
		select {
		case buf := <- self.rbuf_channel:
			if buf == nil {
				return io.EOF
			}

			self.aof_lock.buf = buf
			err := self.aof_lock.Decode()
			if err != nil {
				return err
			}
			return nil
		default:
			if self.closed {
				return io.EOF
			}

			self.aof.aof_glock.Lock()
			self.aof.Flush()
			self.aof.aof_glock.Unlock()
			buf := <- self.rbuf_channel
			if buf == nil {
				return io.EOF
			}

			self.aof_lock.buf = buf
			err := self.aof_lock.Decode()
			if err != nil {
				return err
			}
			return nil
		}
	}
}

func (self *ReplicationClient) ReadProcess() {
	for ; !self.closed; {
		buf := self.rbufs[self.rbuf_index]
		n, err := self.stream.Read(buf)
		if err != nil {
			self.rbuf_channel <- nil
			return
		}

		if n < 64 {
			for ; n < 64; {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					self.rbuf_channel <- nil
					return
				}
				n += nn
			}
		}

		self.rbuf_channel <- buf
		self.rbuf_index++
		if self.rbuf_index >= len(self.rbufs) {
			self.rbuf_index = 0
		}
	}

	self.rbuf_channel <- nil
}

func (self *ReplicationClient) HandleAcked(ack_lock *ReplicationAckLock) error {
	self.glock.Lock()
	if self.stream == nil {
		self.glock.Unlock()
		return errors.New("stream closed")
	}

	if ack_lock.aof_result != 0 && ack_lock.lock_result.Result == 0 {
		ack_lock.lock_result.Result = protocol.RESULT_ERROR
	}

	err := ack_lock.lock_result.Encode(self.wbuf)
	if err != nil {
		self.glock.Unlock()
		return err
	}
	err = self.stream.WriteBytes(self.wbuf)
	self.glock.Unlock()
	return err
}

func (self *ReplicationClient) SleepWhenRetryConnect() error {
	self.glock.Lock()
	self.wakeup_signal = make(chan bool, 1)
	self.glock.Unlock()

	select {
	case <- self.wakeup_signal:
		return nil
	case <- time.After(5 * time.Second):
		self.glock.Lock()
		self.wakeup_signal = nil
		self.glock.Unlock()
		return nil
	}
}

func (self *ReplicationClient) WakeupRetryConnect() error {
	self.glock.Lock()
	if self.wakeup_signal != nil {
		close(self.wakeup_signal)
		self.wakeup_signal = nil
	}
	self.glock.Unlock()
	return nil
}

type ReplicationServer struct {
	manager 			*ReplicationManager
	stream 				*Stream
	protocol 			*BinaryServerProtocol
	aof 				*Aof
	raof_lock 			*AofLock
	waof_lock 			*AofLock
	current_request_id	[16]byte
	buffer_index		uint64
	pulled				uint32
	pulled_waiter		chan bool
	closed 				bool
	closed_waiter		chan bool
	sended_files		bool
}

func NewReplicationServer(manager *ReplicationManager, server_protocol *BinaryServerProtocol) *ReplicationServer {
	return &ReplicationServer{manager, server_protocol.stream, server_protocol,
		manager.slock.GetAof(), NewAofLock(), NewAofLock(),
		[16]byte{}, 0, 0, make(chan bool, 1),
		false, make(chan bool, 1), false}
}

func (self *ReplicationServer) Close() error {
	self.closed = true
	if self.protocol != nil {
		self.protocol.Close()
	}
	return nil
}

func (self *ReplicationServer) HandleInitSync(command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.manager.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, 0, "ERR_STATE", nil), nil
	}

	request := protobuf.SyncRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0,  "ERR_PROTO", nil), nil
	}

	if request.AofId == "" {
		buffer_index, err := self.manager.buffer_queue.Head(self.waof_lock.buf)
		if err != nil {
			self.waof_lock.AofIndex = self.aof.aof_file_index
			self.waof_lock.AofId = 0
		} else {
			err := self.waof_lock.Decode()
			if err != nil {
				return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
			}
		}
		request_id := fmt.Sprintf("%x", self.waof_lock.GetRequestId())
		response := protobuf.SyncResponse{AofId:request_id}
		data, err := response.Marshal()
		if err != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		err = self.protocol.Write(protocol.NewCallResultCommand(command, 0,  "", data))
		if err != nil {
			return nil, err
		}
		self.buffer_index = buffer_index
		self.manager.slock.logger.Infof("Replication Client Send Files Start %s %s", self.protocol.RemoteAddr().String(), request_id)

		err = self.WaitStarted()
		if err != nil {
			return nil, err
		}
		go (func() {
			err := self.SendFiles()
			if err != nil {
				self.manager.slock.logger.Infof("Replication Client Send Files Error: %s %v", self.protocol.RemoteAddr().String(), err)
				self.Close()
				return
			}
		})()
		return nil, nil
	}

	self.manager.slock.logger.Infof("Replication Client Require Start %s %s", self.protocol.RemoteAddr().String(), request.AofId)
	buf, err := hex.DecodeString(request.AofId)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_AOF_ID", nil), nil
	}
	inited_aof_id := [16]byte{buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]}
	buffer_index, serr := self.manager.buffer_queue.Search(inited_aof_id, self.waof_lock.buf)
	if serr != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_NOT_FOUND", nil), nil
	}

	err = self.waof_lock.Decode()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	request_id := fmt.Sprintf("%x", self.waof_lock.GetRequestId())
	response := protobuf.SyncResponse{AofId:request_id}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	err = self.protocol.Write(protocol.NewCallResultCommand(command, 0, "", data))

	if err != nil {
		return nil, err
	}
	self.buffer_index = buffer_index + 1
	self.sended_files = true
	self.manager.slock.logger.Infof("Replication Client Send Start %s %s", self.protocol.RemoteAddr().String(), request_id)

	err = self.WaitStarted()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (self *ReplicationServer) SendFiles() error {
	self.aof.WaitRewriteAofFiles()
	self.aof.aof_glock.Lock()
	self.aof.Flush()
	self.aof.aof_glock.Unlock()

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
		self.current_request_id = lock.GetRequestId()
		return true, nil
	})
	if err != nil {
		return err
	}

	err = self.SendFilesFinished()
	if err != nil {
		return err
	}
	self.manager.slock.logger.Infof("Replication Client Send File finish %s", self.protocol.RemoteAddr().String())
	return self.manager.WakeupServerChannel()
}

func (self *ReplicationServer) WaitStarted() error {
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

		if self.raof_lock.CommandType == protocol.COMMAND_INIT && self.raof_lock.AofIndex == 0xffffffff && self.raof_lock.AofId == 0xffffffff {
			return nil
		}
	}
	return io.EOF
}

func (self *ReplicationServer) SendFilesFinished() error {
	aof_lock := NewAofLock()
	aof_lock.CommandType = protocol.COMMAND_INIT
	aof_lock.AofIndex = 0xffffffff
	aof_lock.AofId = 0xffffffff
	err := aof_lock.Encode()
	if err != nil {
		return err
	}
	err = self.stream.WriteBytes(aof_lock.buf)
	if err != nil {
		return err
	}
	self.sended_files = true
	return nil
}

func (self *ReplicationServer) SendFilesQueue() error {
	bufs := make([][]byte, 0)
	atomic.AddUint32(&self.manager.server_active_count, 1)
	for ; !self.closed && !self.sended_files; {
		buf := make([]byte, 64)
		err := self.manager.buffer_queue.Pop(self.buffer_index, buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.pulled, 1)
				atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
				<- self.pulled_waiter
				atomic.AddUint32(&self.manager.server_active_count, 1)
				continue
			}
			atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
			return err
		}

		self.buffer_index++
		if self.buffer_index >= self.manager.buffer_queue.max_index {
			self.buffer_index = uint64(self.manager.buffer_queue.segment_count)
		}
		bufs = append(bufs, buf)
	}
	atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)

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
			self.current_request_id = self.waof_lock.GetRequestId()
		}
	}
	return nil
}

func (self *ReplicationServer) SendProcess() error {
	if !self.sended_files {
		err := self.SendFilesQueue()
		if err != nil {
			return err
		}
	}

	atomic.AddUint32(&self.manager.server_active_count, 1)
	for ; !self.closed; {
		if float64(self.manager.buffer_queue.current_index - self.buffer_index) > float64(self.manager.buffer_queue.segment_count) * 0.8 {
			self.manager.buffer_queue.require_duplicated = true
		}

		buf := self.waof_lock.buf
		err := self.manager.buffer_queue.Pop(self.buffer_index, buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.pulled, 1)
				atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
				if atomic.CompareAndSwapUint32(&self.manager.server_active_count, 0, 0) {
					self.manager.glock.Lock()
					if self.manager.server_flush_waiter != nil {
						close(self.manager.server_flush_waiter)
						self.manager.server_flush_waiter = nil
					}
					self.manager.glock.Unlock()
				}
				<- self.pulled_waiter
				atomic.AddUint32(&self.manager.server_active_count, 1)
				continue
			}
			atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
			return err
		}

		err = self.stream.WriteBytes(buf)
		if err != nil {
			atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
			return err
		}

		self.current_request_id[0], self.current_request_id[1], self.current_request_id[2], self.current_request_id[3], self.current_request_id[4], self.current_request_id[5], self.current_request_id[6], self.current_request_id[7],
			self.current_request_id[8], self.current_request_id[9], self.current_request_id[10], self.current_request_id[11], self.current_request_id[12], self.current_request_id[13], self.current_request_id[14], self.current_request_id[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
		self.buffer_index++
		if self.buffer_index >= self.manager.buffer_queue.max_index {
			self.buffer_index = uint64(self.manager.buffer_queue.segment_count)
		}
	}
	atomic.AddUint32(&self.manager.server_active_count, 0xffffffff)
	return nil
}

func (self *ReplicationServer) RecvProcess() error {
	buf := self.raof_lock.buf
	lock_result := &protocol.LockResultCommand{}
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

		err = lock_result.Decode(buf)
		if err != nil {
			return err
		}

		err = self.aof.LoadLockAck(lock_result)
		if err != nil {
			return err
		}
	}
	return io.EOF
}

type ReplicationAckLock struct {
	lock_result 	protocol.LockResultCommand
	aof_result		uint8
	locked			bool
	aofed			bool
}

func NewReplicationAckLock() *ReplicationAckLock {
	result_command := protocol.ResultCommand{ Magic:protocol.MAGIC, Version:protocol.VERSION, CommandType:0, RequestId:[16]byte{}, Result:0}
	lock_result := protocol.LockResultCommand{ResultCommand:result_command, Flag:0, DbId:0, LockId:[16]byte{}, LockKey:[16]byte{},
		Count:0, Lcount:0, Lrcount:0, Rcount:0, Blank:protocol.RESULT_LOCK_COMMAND_BLANK_BYTERS}
	return &ReplicationAckLock{lock_result, 0, false, false}
}

type ReplicationAckDB struct {
	manager 					*ReplicationManager
	glock						*sync.Mutex
	locks 						map[[16]byte]*Lock
	requests					map[[2][16]byte][16]byte
	ack_locks					map[[16]byte]*ReplicationAckLock
	free_ack_locks				[]*ReplicationAckLock
	free_ack_locks_index        uint32
	ack_count					uint8
	closed   					bool
}

func NewReplicationAckDB(manager *ReplicationManager) *ReplicationAckDB {
	return &ReplicationAckDB{manager, &sync.Mutex{}, make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE),
			make(map[[2][16]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE), make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE),
		make([]*ReplicationAckLock, REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE), 0, 1, false}
}

func (self *ReplicationAckDB) Close() error {
	self.closed = true
	return nil
}

func (self *ReplicationAckDB) PushLock(lock *AofLock) error {
	if lock.CommandType == protocol.COMMAND_LOCK {
		self.glock.Lock()
		if self.manager.slock.state != STATE_LEADER {
			if lock.lock == nil {
				self.glock.Unlock()
				return nil
			}

			self.glock.Unlock()
			lock_manager := lock.lock.manager
			lock_manager.lock_db.DoAckLock(lock.lock, false)
			return nil
		}
		
		lock_key := [2][16]byte{lock.LockKey, lock.LockId}
		if request_id, ok := self.requests[lock_key]; ok {
			if _, ok := self.locks[request_id]; ok {
				delete(self.locks, request_id)
			}
		}

		request_id := lock.GetRequestId()
		self.locks[request_id] = lock.lock
		self.requests[lock_key] = request_id
		self.glock.Unlock()
		return nil
	}

	lock_key := [2][16]byte{lock.LockKey, lock.LockId}
	self.glock.Lock()
	if request_id, ok := self.requests[lock_key]; ok {
		delete(self.requests, lock_key)
		if lock, ok := self.locks[request_id]; ok {
			delete(self.locks, request_id)
			self.glock.Unlock()

			lock_manager := lock.manager
			lock_manager.lock_db.DoAckLock(lock, false)
			return nil
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) Process(aof_lock *AofLock) error {
	request_id := aof_lock.GetRequestId()
	self.glock.Lock()
	if lock, ok := self.locks[request_id]; ok {
		if aof_lock.Result != 0 {
			delete(self.locks, request_id)
			lock_key := [2][16]byte{aof_lock.LockKey, aof_lock.LockId}
			if _, ok := self.requests[lock_key]; ok {
				delete(self.requests, lock_key)
			}
			self.glock.Unlock()

			lock_manager := lock.manager
			lock_manager.lock_db.DoAckLock(lock, false)
			return nil
		}

		lock.ack_count++
		if lock.ack_count < self.ack_count {
			self.glock.Unlock()
			return nil
		}

		delete(self.locks, request_id)
		lock_key := [2][16]byte{aof_lock.LockKey, aof_lock.LockId}
		if _, ok := self.requests[lock_key]; ok {
			delete(self.requests, lock_key)
		}
		self.glock.Unlock()

		lock_manager := lock.manager
		lock_manager.lock_db.DoAckLock(lock, true)
		return nil
	}

	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAofed(aof_lock *AofLock) error {
	request_id := aof_lock.GetRequestId()
	self.glock.Lock()
	if lock, ok := self.locks[request_id]; ok {
		if aof_lock.Result != 0 {
			delete(self.locks, request_id)
			lock_key := [2][16]byte{lock.command.LockKey, lock.command.LockId}
			if _, ok := self.requests[lock_key]; ok {
				delete(self.requests, lock_key)
			}
			self.glock.Unlock()

			lock_manager := lock.manager
			lock_manager.lock_db.DoAckLock(lock, false)
			return nil
		}

		lock.ack_count++
		if lock.ack_count < self.ack_count {
			self.glock.Unlock()
			return nil
		}

		delete(self.locks, request_id)
		lock_key := [2][16]byte{lock.command.LockKey, lock.command.LockId}
		if _, ok := self.requests[lock_key]; ok {
			delete(self.requests, lock_key)
		}
		self.glock.Unlock()

		lock_manager := lock.manager
		lock_manager.lock_db.DoAckLock(lock, true)
		return nil
	}

	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAcked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	self.glock.Lock()
	ack_lock, ok := self.ack_locks[command.RequestId]
	if !ok {
		if self.free_ack_locks_index > 0 {
			self.free_ack_locks_index--
			ack_lock = self.free_ack_locks[self.free_ack_locks_index]
			ack_lock.aofed = false
		} else {
			ack_lock = NewReplicationAckLock()
		}
		self.ack_locks[command.RequestId] = ack_lock
	}

	ack_lock.lock_result.CommandType = command.CommandType
	ack_lock.lock_result.RequestId = command.RequestId
	ack_lock.lock_result.Result = result
	ack_lock.lock_result.Flag = 0
	ack_lock.lock_result.DbId = command.DbId
	ack_lock.lock_result.LockId = command.LockId
	ack_lock.lock_result.LockKey = command.LockKey
	ack_lock.lock_result.Lcount = lcount
	ack_lock.lock_result.Count = command.Count
	ack_lock.lock_result.Lrcount = lrcount
	ack_lock.lock_result.Rcount = command.Rcount
	ack_lock.locked = true

	if !ack_lock.aofed {
		self.glock.Unlock()
		return nil
	}

	delete(self.ack_locks, command.RequestId)
	self.glock.Unlock()
	if self.manager.client_channel != nil {
		self.manager.client_channel.HandleAcked(ack_lock)
	}
	self.glock.Lock()
	if self.free_ack_locks_index < REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE {
		self.free_ack_locks[self.free_ack_locks_index] = ack_lock
		self.free_ack_locks_index++
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAckAofed(aof_lock *AofLock) error {
	request_id := aof_lock.GetRequestId()
	self.glock.Lock()
	ack_lock, ok := self.ack_locks[request_id]
	if !ok {
		if self.free_ack_locks_index > 0 {
			self.free_ack_locks_index--
			ack_lock = self.free_ack_locks[self.free_ack_locks_index]
			ack_lock.locked = false
		} else {
			ack_lock = NewReplicationAckLock()
		}
		self.ack_locks[request_id] = ack_lock
	}

	ack_lock.aof_result = aof_lock.Result
	ack_lock.aofed = true

	if !ack_lock.locked {
		self.glock.Unlock()
		return nil
	}

	delete(self.ack_locks, request_id)
	self.glock.Unlock()
	if self.manager.client_channel != nil {
		self.manager.client_channel.HandleAcked(ack_lock)
	}
	self.glock.Lock()
	if self.free_ack_locks_index < REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE {
		self.free_ack_locks[self.free_ack_locks_index] = ack_lock
		self.free_ack_locks_index++
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) SwitchToLeader() error {
	self.glock.Lock()
	for _, ack_lock := range self.ack_locks {
		if self.manager.client_channel != nil {
			self.manager.client_channel.HandleAcked(ack_lock)
		}
		if self.free_ack_locks_index < REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE {
			self.free_ack_locks[self.free_ack_locks_index] = ack_lock
			self.free_ack_locks_index++
		}
	}
	self.ack_locks = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) SwitchToFollower() error {
	self.glock.Lock()
	for _, lock := range self.locks {
		lock_manager := lock.manager
		lock_manager.lock_db.DoAckLock(lock, true)
	}
	self.locks = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
	self.requests = make(map[[2][16]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
	self.glock.Unlock()
	return nil
}

type ReplicationManager struct {
	slock 						*SLock
	glock 						*sync.Mutex
	buffer_queue 				*ReplicationBufferQueue
	ack_dbs						[]*ReplicationAckDB
	client_channel 				*ReplicationClient
	server_channels 			[]*ReplicationServer
	transparency_manager		*TransparencyManager
	current_request_id			[16]byte
	leader_address				string
	server_count				uint32
	server_active_count 		uint32
	server_flush_waiter			chan bool
	inited_waters				[]chan bool
	closed 						bool
	is_leader 					bool
}

func NewReplicationManager() *ReplicationManager {
	transparency_manager := NewTransparencyManager()
	manager := &ReplicationManager{nil, &sync.Mutex{}, nil, make([]*ReplicationAckDB, 256),
		nil, make([]*ReplicationServer, 0), transparency_manager,[16]byte{}, "", 0,
		0, nil, make([]chan bool, 0), false, true}
	manager.buffer_queue = NewReplicationBufferQueue(manager, int(Config.AofRingBufferSize))
	return manager
}

func (self *ReplicationManager) GetCallMethods() map[string]BinaryServerProtocolCallHandler{
	handlers := make(map[string]BinaryServerProtocolCallHandler, 2)
	handlers["SYNC"] = self.CommandHandleSyncCommand
	return handlers
}

func (self *ReplicationManager) GetHandlers() map[string]TextServerProtocolCommandHandler{
	handlers := make(map[string]TextServerProtocolCommandHandler, 2)
	return handlers
}

func (self *ReplicationManager) Init(leader_address string) error {
	self.leader_address = leader_address
	if self.slock.state == STATE_LEADER {
		self.current_request_id = self.slock.aof.GetCurrentAofID()
		self.slock.Log().Infof("Replication Init Leader %x", self.current_request_id)
	} else {
		self.current_request_id = [16]byte{}
		self.slock.Log().Infof("Replication Init Follower %s %x", leader_address, self.current_request_id)
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

	if self.client_channel != nil {
		client_channel := self.client_channel
		client_channel.Close()
		<- client_channel.closed_waiter
		self.client_channel = nil
	}

	self.WakeupServerChannel()
	self.WaitServerSynced()
	for _, channel := range self.server_channels {
		channel.Close()
		<- channel.closed_waiter
	}
	self.glock.Lock()
	self.server_channels = self.server_channels[:0]
	for _, waiter := range self.inited_waters {
		waiter <- false
	}
	self.inited_waters = self.inited_waters[:0]
	for i, db := range self.ack_dbs {
		if db != nil {
			db.Close()
			self.ack_dbs[i] = nil
		}
	}
	self.glock.Unlock()
	self.transparency_manager.Close()
	<- self.transparency_manager.closed_waiter
	self.slock.logger.Infof("Replication Closed")
}

func (self *ReplicationManager) WaitServerSynced() error {
	self.glock.Lock()
	if atomic.CompareAndSwapUint32(&self.server_active_count, 0, 0) {
		self.glock.Unlock()
		return nil
	}

	server_flush_waiter := make(chan bool, 1)
	go func() {
		select {
		case <- server_flush_waiter:
			return
		case <- time.After(30 * time.Second):
			self.server_flush_waiter = nil
			close(server_flush_waiter)
		}
	}()
	self.server_flush_waiter = server_flush_waiter
	self.glock.Unlock()
	<- self.server_flush_waiter
	return nil
}

func (self *ReplicationManager) CommandHandleSyncCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.closed {
		return protocol.NewCallResultCommand(command, 0, "STATE_ERROR", nil), io.EOF
	}

	channel := NewReplicationServer(self, server_protocol)
	self.slock.logger.Infof("Replication Client Sync %s", server_protocol.RemoteAddr().String())
	result, err := channel.HandleInitSync(command)
	if err != nil {
		channel.closed = true
		close(channel.closed_waiter)
		if err != io.EOF {
			self.slock.logger.Errorf("Replication Client Start Sync Error: %s %v", server_protocol.RemoteAddr().String(), err)
		}
		return result, err
	}

	if result != nil {
		channel.closed = true
		close(channel.closed_waiter)
		self.slock.logger.Infof("Replication Client Start Sync Fail")
		return result, nil
	}

	self.slock.logger.Infof("Replication Accept Client Start Sync %s", server_protocol.RemoteAddr().String())
	self.AddServerChannel(channel)
	server_protocol.stream.stream_type = STREAM_TYPE_AOF
	go func() {
		err := channel.SendProcess()
		if err != nil {
			if err != io.EOF && !self.closed {
				self.slock.logger.Errorf("Replication Client Sync Error: %s %v", server_protocol.RemoteAddr().String(), err)
			}
			channel.Close()
		}
	}()
	err = channel.RecvProcess()
	channel.closed = true
	self.WakeupServerChannel()
	self.RemoveServerChannel(channel)
	close(channel.closed_waiter)
	if err != nil {
		if err != io.EOF && !self.closed {
			self.slock.logger.Errorf("Replication Client Process Error: %s %v", server_protocol.RemoteAddr().String(), err)
		}
		self.slock.logger.Infof("Replication Client Close %s", server_protocol.RemoteAddr().String())
	}
	return nil, io.EOF
}

func (self *ReplicationManager) AddServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	self.server_channels = append(self.server_channels, channel)
	self.server_count = uint32(len(self.server_channels))

	ack_count := len(self.server_channels) + 1
	for _, db := range self.ack_dbs {
		if db != nil {
			db.ack_count = uint8(ack_count)
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) RemoveServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	server_channels := make([]*ReplicationServer, 0)
	for _, c := range self.server_channels {
		if channel != c {
			server_channels = append(server_channels, c)
		}
	}
	self.server_channels = server_channels
	self.server_count = uint32(len(server_channels))

	ack_count := len(self.server_channels) + 1
	for _, db := range self.ack_dbs {
		if db != nil {
			db.ack_count = uint8(ack_count)
		}
	}

	if atomic.CompareAndSwapUint32(&self.server_active_count, 0, 0) {
		if self.server_flush_waiter != nil {
			close(self.server_flush_waiter)
			self.server_flush_waiter = nil
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) StartSync() error {
	if self.leader_address == "" {
		return errors.New("slaveof is empty")
	}

	channel := NewReplicationClient(self)
	self.is_leader = false
	self.client_channel = channel
	go channel.Run()
	return nil
}

func (self *ReplicationManager) GetAckDB(db_id uint8) *ReplicationAckDB {
	return self.ack_dbs[db_id]
}

func (self *ReplicationManager) GetOrNewAckDB(db_id uint8) *ReplicationAckDB {
	db := self.ack_dbs[db_id]
	if db != nil {
		return db
	}

	self.glock.Lock()
	if self.ack_dbs[db_id] == nil {
		self.ack_dbs[db_id] = NewReplicationAckDB(self)
		self.ack_dbs[db_id].ack_count = uint8(len(self.server_channels) + 1)
	}
	self.glock.Unlock()
	return self.ack_dbs[db_id]
}

func (self *ReplicationManager) PushLock(lock *AofLock) error {
	if self.buffer_queue.require_duplicated {
		self.buffer_queue = self.buffer_queue.Reduplicated()
	}

	if lock.AofFlag & 0x1000 != 0 && lock.lock != nil {
		db := self.GetOrNewAckDB(lock.DbId)
		err := db.PushLock(lock)
		if err != nil {
			return err
		}
	}

	buf := lock.buf
	err := self.buffer_queue.Push(buf)
	if err != nil {
		return err
	}

	self.current_request_id[0], self.current_request_id[1], self.current_request_id[2], self.current_request_id[3], self.current_request_id[4], self.current_request_id[5], self.current_request_id[6], self.current_request_id[7],
		self.current_request_id[8], self.current_request_id[9], self.current_request_id[10], self.current_request_id[11], self.current_request_id[12], self.current_request_id[13], self.current_request_id[14], self.current_request_id[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	return self.WakeupServerChannel()
}

func (self *ReplicationManager) WakeupServerChannel() error {
	if atomic.CompareAndSwapUint32(&self.server_active_count, self.server_count, self.server_count) {
		return nil
	}

	for _, channel := range self.server_channels {
		if atomic.CompareAndSwapUint32(&channel.pulled, 1, 1) {
			channel.pulled_waiter <- true
			atomic.AddUint32(&channel.pulled, 0xffffffff)
		}
	}
	return nil
}

func (self *ReplicationManager) WaitInitSynced(waiter chan bool) {
	self.glock.Lock()
	self.inited_waters = append(self.inited_waters, waiter)
	self.glock.Unlock()
}

func (self *ReplicationManager) WakeupInitSyncedWaiters() {
	self.glock.Lock()
	for _, waiter := range self.inited_waters {
		waiter <- false
	}
	self.inited_waters = self.inited_waters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) ClientSycnInited() {
	self.glock.Lock()
	self.slock.UpdateState(STATE_FOLLOWER)
	for _, waiter := range self.inited_waters {
		waiter <- true
	}
	self.inited_waters = self.inited_waters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) SwitchToLeader() error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	self.slock.logger.Infof("Replication Start Change To Leader")
	for _, db := range self.ack_dbs {
		if db != nil {
			db.SwitchToLeader()
		}
	}
	self.slock.UpdateState(STATE_LEADER)
	self.leader_address = ""
	self.glock.Unlock()

	if self.client_channel != nil {
		client_channel := self.client_channel
		client_channel.Close()
		<- client_channel.closed_waiter
		self.current_request_id = client_channel.current_request_id
		self.client_channel = nil
	}
	self.is_leader = true
	self.slock.logger.Infof("Replication Finish Change To Leader")
	return nil
}

func (self *ReplicationManager) SwitchToFollower(address string) error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leader_address == address && !self.is_leader {
		self.glock.Unlock()
		return nil
	}

	self.slock.logger.Infof("Replication Start Change To Follower")
	self.leader_address = address
	if address == "" {
		self.slock.UpdateState(STATE_FOLLOWER)
	} else {
		self.slock.UpdateState(STATE_SYNC)
	}
	self.glock.Unlock()

	for _, db := range self.slock.dbs {
		if db != nil {
			for i := int8(0); i < db.manager_max_glocks; i++ {
				db.manager_glocks[i].Lock()
				db.manager_glocks[i].Unlock()
			}
		}
	}
	self.slock.aof.WaitFlushAofChannel()
	self.WakeupServerChannel()
	self.WaitServerSynced()
	for _, channel := range self.server_channels {
		channel.Close()
		<- channel.closed_waiter
	}

	for _, db := range self.ack_dbs {
		if db != nil {
			db.SwitchToFollower()
		}
	}

	if self.client_channel != nil {
		client_channel := self.client_channel
		client_channel.Close()
		<- client_channel.closed_waiter
		self.current_request_id = client_channel.current_request_id
		self.client_channel = nil
	}

	if self.leader_address == "" {
		self.is_leader = false
		self.slock.logger.Infof("Replication Finish Change To Follower")
		return nil
	}

	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication Finish Change To Follower")
	return nil
}

func (self *ReplicationManager) ChangeLeader(address string) error {
	self.glock.Lock()
	if self.slock.state != STATE_FOLLOWER {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leader_address == address {
		self.glock.Unlock()
		return nil
	}

	self.leader_address = address
	if self.leader_address == "" {
		self.slock.UpdateState(STATE_FOLLOWER)
	} else {
		self.slock.UpdateState(STATE_SYNC)
	}
	self.glock.Unlock()

	if self.client_channel != nil {
		client_channel := self.client_channel
		client_channel.Close()
		<- client_channel.closed_waiter
		self.current_request_id = client_channel.current_request_id
		self.client_channel = nil
	}

	if self.leader_address == "" {
		self.is_leader = false
		self.slock.logger.Infof("Replication Change To Empty Leader")
		return nil
	}

	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication Change To Leader %s", address)
	return nil
}

func (self *ReplicationManager) FlushDB() error {
	for _, db := range self.slock.dbs {
		if db != nil {
			db.FlushDB()
		}
	}
	self.buffer_queue.current_index = 0
	self.slock.Log().Infof("Replication Flush All DB")
	return nil
}

func (self *ReplicationManager) GetCurrentAofID() [16]byte {
	if !self.is_leader && self.client_channel != nil {
		return self.client_channel.current_request_id
	}
	return self.current_request_id
}