package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ARBITER_ROLE_UNKNOWN = 0
const ARBITER_ROLE_LEADER = 1
const ARBITER_ROLE_FOLLOWER = 2
const ARBITER_ROLE_ARBITER = 3

const ARBITER_MEMBER_STATUS_UNOPEN = 1
const ARBITER_MEMBER_STATUS_UNINIT = 2
const ARBITER_MEMBER_STATUS_CONNECTED = 3
const ARBITER_MEMBER_STATUS_OFFLINE = 4
const ARBITER_MEMBER_STATUS_ONLINE = 5

type ArbiterStore struct {
	filename string
}

func NewArbiterStore() *ArbiterStore {
	return &ArbiterStore{""}
}

func (self *ArbiterStore) Init(manager *ArbiterManager) error {
	dataDir, err := filepath.Abs(Config.DataDir)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter config meta data dir error %v", err)
		return err
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		manager.slock.Log().Errorf("Arbiter config meta data dir error %v", err)
		return err
	}

	self.filename = filepath.Join(dataDir, "meta.pb")
	manager.slock.Log().Infof("Arbiter config meta file %s", self.filename)
	return nil
}

func (self *ArbiterStore) Load(manager *ArbiterManager) error {
	if self.filename == "" {
		err := self.Init(manager)
		if err != nil {
			return err
		}
	}

	file, err := os.OpenFile(self.filename, os.O_RDONLY, 0644)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter open meta file error %v", err)
		return err
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter read meta file error %v", err)
		_ = file.Close()
		return err
	}
	data, err = self.readHeader(data)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter read meta file header error %v", err)
		_ = file.Close()
		return err
	}

	replset := protobuf.ReplSet{}
	err = replset.Unmarshal(data)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter read meta file decode data error %v", err)
		_ = file.Close()
		return err
	}

	members := make([]*ArbiterMember, 0)
	for _, rplm := range replset.Members {
		member := NewArbiterMember(manager, rplm.Host, rplm.Weight, rplm.Arbiter)
		if rplm.Host == replset.Owner {
			member.isSelf = true
			manager.ownMember = member
		}
		members = append(members, member)
	}
	if manager.ownMember == nil {
		_ = file.Close()
		return errors.New("unknown own member info")
	}
	manager.members = members
	manager.gid = replset.Gid
	manager.version = replset.Version
	manager.vertime = replset.Vertime
	manager.voter.commitId = replset.CommitId
	_ = file.Close()
	manager.slock.Log().Infof("Arbiter load meta file Name:%s Gid:%s Version:%d CommitId:%d MemberCount:%d", replset.Name,
		replset.Gid, replset.Version, replset.CommitId, len(replset.Members))
	return nil
}

func (self *ArbiterStore) Save(manager *ArbiterManager) error {
	if self.filename == "" {
		err := self.Init(manager)
		if err != nil {
			return err
		}
	}

	file, err := os.OpenFile(self.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter open meta file error %v", err)
		return err
	}

	err = self.writeHeader(file)
	if err != nil {
		manager.slock.Log().Errorf("Arbiter write meta file header error %v", err)
		_ = file.Close()
		return err
	}

	members := make([]*protobuf.ReplSetMember, 0)
	for _, member := range manager.members {
		rplm := &protobuf.ReplSetMember{Host: member.host, Weight: member.weight, Arbiter: member.arbiter, Role: uint32(member.role)}
		members = append(members, rplm)
	}

	owner := ""
	if manager.ownMember != nil {
		owner = manager.ownMember.host
	}
	replset := protobuf.ReplSet{Name: manager.name, Gid: manager.gid, Version: manager.version, Vertime: manager.vertime,
		Owner: owner, Members: members, CommitId: manager.voter.commitId}
	data, err := replset.Marshal()
	if err != nil {
		manager.slock.Log().Errorf("Arbiter write meta file encode data error %v", err)
		_ = file.Close()
		return err
	}

	n, werr := file.Write(data)
	if werr != nil {
		manager.slock.Log().Errorf("Arbiter write meta file error %v", err)
		_ = file.Close()
		return werr
	}
	if n != len(data) {
		manager.slock.Log().Errorf("Arbiter write meta file data length error")
		_ = file.Close()
		return errors.New("write data error")
	}
	err = file.Close()
	if err != nil {
		manager.slock.Log().Errorf("Arbiter close meta file error %v", err)
	}
	return nil
}

func (self *ArbiterStore) readHeader(buf []byte) ([]byte, error) {
	if len(buf) < 11 {
		return nil, errors.New("File is not Meta FIle")
	}

	if string(buf[:7]) != "SLOCKPB" {
		return nil, errors.New("File is not Meta File")
	}

	version := uint16(buf[7]) | uint16(buf[8])<<8
	if version != 0x0001 {
		return nil, errors.New("Meta File Unknown Version")
	}

	headerLen := uint16(buf[9]) | uint16(buf[10])<<8
	if headerLen != 0x0000 {
		return nil, errors.New("Meta File Header Len Error")
	}
	return buf[headerLen+11:], nil
}

func (self *ArbiterStore) writeHeader(file *os.File) error {
	buf := make([]byte, 11)
	buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6] = 'S', 'L', 'O', 'C', 'K', 'P', 'B'
	buf[7], buf[8], buf[9], buf[10] = 0x01, 0x00, 0x00, 0x00

	n, werr := file.Write(buf)
	if werr != nil {
		return werr
	}
	if n != len(buf) {
		return errors.New("write header error")
	}
	return nil
}

type ArbiterClient struct {
	member       *ArbiterMember
	glock        *sync.Mutex
	stream       *client.Stream
	protocol     *client.BinaryClientProtocol
	rchannel     chan protocol.CommandDecode
	closed       bool
	closedWaiter chan bool
	wakeupSignal chan bool
}

func NewArbiterClient(member *ArbiterMember) *ArbiterClient {
	return &ArbiterClient{member, &sync.Mutex{}, nil, nil,
		make(chan protocol.CommandDecode, 8), false, make(chan bool, 1), nil}
}

func (self *ArbiterClient) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	self.member.manager.slock.Log().Infof("Arbiter client connect %s", addr)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		self.member.manager.slock.Log().Warnf("Arbiter client connect %s error %v", addr, err)
		return err
	}
	stream := client.NewStream(conn)
	clientProtocol := client.NewBinaryClientProtocol(stream)

	self.glock.Lock()
	self.stream = stream
	self.protocol = clientProtocol
	err = self.handleInit()
	if err != nil {
		_ = clientProtocol.Close()
		self.protocol = nil
		self.stream = nil
		self.glock.Unlock()
		return err
	}
	close(self.rchannel)
	self.rchannel = make(chan protocol.CommandDecode, 8)
	self.glock.Unlock()
	return nil
}

func (self *ArbiterClient) Close() error {
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	_ = self.WakeupRetryConnect()
	self.member.manager.slock.Log().Warnf("Arbiter client connect %s close", self.member.host)
	return nil
}

func (self *ArbiterClient) handleInit() error {
	request := protobuf.ArbiterConnectRequest{FromHost: self.member.manager.ownMember.host, ToHost: self.member.host}
	data, err := request.Marshal()
	if err != nil {
		return err
	}

	callCommand := protocol.NewCallCommand("REPL_CONNECT", data)
	err = self.protocol.Write(callCommand)
	if err != nil {
		return err
	}
	resultCommand, err := self.protocol.Read()
	if err != nil {
		return err
	}

	callResultCommand, ok := resultCommand.(*protocol.CallResultCommand)
	if !ok {
		return errors.New("unkonwn command")
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		if callResultCommand.ErrType == "ERR_NOT_MEMBER" {
			self.member.manager.slock.Log().Warnf("Arbiter client connect %s init recv NOT_MEMBER error, will quit the members",
				self.member.host)
			go func() {
				self.member.manager.glock.Lock()
				if self.member.role == ARBITER_ROLE_LEADER {
					_ = self.member.manager.QuitLeader()
				}
				self.member.manager.glock.Unlock()
				_ = self.member.manager.QuitMember()
			}()
			return errors.New("init error")
		}
		self.member.manager.slock.Log().Warnf("Arbiter client connect %s init error %d:%s", self.member.host,
			callResultCommand.Result, callResultCommand.ErrType)
		return errors.New("init error")
	}

	self.member.manager.slock.Log().Infof("Arbiter client connect %s init succed", self.member.host)
	return nil
}

func (self *ArbiterClient) Run() {
	_ = self.member.clientOffline(self)
	for !self.closed {
		if self.protocol == nil {
			err := self.Open(self.member.host)
			if err != nil {
				_ = self.sleepWhenRetryConnect()
				continue
			}
		}

		self.member.manager.slock.Log().Infof("Arbiter client connect %s connected", self.member.host)
		_ = self.member.clientOnline(self)
		for !self.closed {
			command, err := self.protocol.Read()
			if err != nil {
				self.rchannel <- nil
				if self.protocol != nil {
					_ = self.protocol.Close()
				}
				self.protocol = nil
				self.stream = nil
				_ = self.member.clientOffline(self)
				break
			}
			self.rchannel <- command
		}
	}

	close(self.rchannel)
	close(self.closedWaiter)
	self.glock.Lock()
	self.protocol = nil
	self.stream = nil
	self.glock.Unlock()
	self.member.manager.slock.Log().Infof("Arbiter client connect %s closed", self.member.host)
	if self.member != nil {
		self.member.client = nil
	}
}

func (self *ArbiterClient) Request(command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.closed {
		return nil, errors.New("client closed")
	}

	self.glock.Lock()
	if self.protocol == nil {
		self.glock.Unlock()
		return nil, errors.New("client unconnected")
	}

	rchannel := self.rchannel
	err := self.protocol.Write(command)
	if err != nil {
		self.glock.Unlock()
		return nil, err
	}

	for {
		result := <-rchannel
		if result == nil {
			self.glock.Unlock()
			return nil, errors.New("read command error")
		}

		callResultCommand, ok := result.(*protocol.CallResultCommand)
		if !ok {
			self.glock.Unlock()
			return nil, errors.New("unknown command")
		}

		if callResultCommand.RequestId != command.RequestId {
			continue
		}
		self.glock.Unlock()
		return callResultCommand, nil
	}
}

func (self *ArbiterClient) sleepWhenRetryConnect() error {
	self.member.glock.Lock()
	self.wakeupSignal = make(chan bool, 1)
	self.member.glock.Unlock()

	select {
	case <-self.wakeupSignal:
		return nil
	case <-time.After(5 * time.Second):
		self.member.glock.Lock()
		self.wakeupSignal = nil
		self.member.glock.Unlock()
		return nil
	}
}

func (self *ArbiterClient) WakeupRetryConnect() error {
	self.member.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.member.glock.Unlock()
	return nil
}

type ArbiterServer struct {
	member       *ArbiterMember
	stream       *Stream
	protocol     *BinaryServerProtocol
	closed       bool
	closedWaiter chan bool
}

func NewArbiterServer(protocol *BinaryServerProtocol) *ArbiterServer {
	return &ArbiterServer{nil, protocol.stream, protocol, false, make(chan bool, 1)}
}

func (self *ArbiterServer) Close() error {
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	if self.member != nil {
		self.member.manager.slock.Log().Infof("Arbiter server connect from %s close", self.member.host)
	}
	return nil
}

func (self *ArbiterServer) handleInit(manager *ArbiterManager, request *protobuf.ArbiterConnectRequest) (*protobuf.ArbiterConnectResponse, error) {
	if manager.ownMember != nil {
		if manager.ownMember.host != request.ToHost {
			return nil, io.EOF
		}
	}

	err := self.Attach(manager, request.FromHost)
	if err != nil {
		return &protobuf.ArbiterConnectResponse{ErrMessage: "unknown member"}, nil
	}
	return &protobuf.ArbiterConnectResponse{ErrMessage: ""}, nil
}

func (self *ArbiterServer) Attach(manager *ArbiterManager, fromHost string) error {
	var currentMember *ArbiterMember = nil
	for _, member := range manager.members {
		if member.host == fromHost {
			currentMember = member
			break
		}
	}

	if currentMember == nil {
		return errors.New("unknown member")
	}

	if currentMember.server != nil {
		if currentMember.server.protocol == self.protocol {
			return nil
		}

		server := currentMember.server
		if !server.closed {
			_ = server.Close()
		}
	}
	currentMember.server = self
	go self.Run()
	self.member = currentMember
	if currentMember.client != nil {
		_ = currentMember.client.WakeupRetryConnect()
	}
	_ = manager.voter.WakeupRetryVote()
	self.member.manager.slock.Log().Infof("Arbiter server accept client %s connected", currentMember.host)
	return nil
}

func (self *ArbiterServer) Run() {
	if !self.stream.closed {
		<-self.stream.closedWaiter
	}
	self.closed = true
	self.protocol = nil
	self.stream = nil
	close(self.closedWaiter)
	if self.member != nil {
		self.member.manager.slock.Log().Infof("Arbiter server connect from %s closed", self.member.host)
		self.member.server = nil
	}
}

type ArbiterMember struct {
	manager      *ArbiterManager
	glock        *sync.Mutex
	client       *ArbiterClient
	server       *ArbiterServer
	host         string
	weight       uint32
	arbiter      uint32
	role         uint8
	status       uint8
	lastUpdated  int64
	lastDelay    int64
	lastError    int
	aofId        [16]byte
	isSelf       bool
	abstianed    bool
	closed       bool
	closedWaiter chan bool
	wakeupSignal chan bool
}

func NewArbiterMember(manager *ArbiterManager, host string, weight uint32, arbiter uint32) *ArbiterMember {
	return &ArbiterMember{manager, &sync.Mutex{}, nil, nil, host, weight, arbiter, ARBITER_ROLE_UNKNOWN,
		ARBITER_MEMBER_STATUS_UNOPEN, 0, 0, 0, [16]byte{}, false,
		false, false, make(chan bool, 1), nil}
}

func (self *ArbiterMember) Open() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return errors.New("closed")
	}
	self.glock.Unlock()

	if !self.isSelf {
		self.client = NewArbiterClient(self)
		err := self.client.Open(self.host)
		if err == nil {
			self.status = ARBITER_MEMBER_STATUS_CONNECTED
		} else {
			self.status = ARBITER_MEMBER_STATUS_UNINIT
		}
	} else {
		self.status = ARBITER_MEMBER_STATUS_ONLINE
	}
	return nil
}

func (self *ArbiterMember) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}
	self.closed = true
	self.glock.Unlock()

	if self.client != nil {
		closedWaiter := self.client.closedWaiter
		_ = self.client.Close()
		<-closedWaiter
		self.client = nil
	}

	if self.server != nil {
		closedWaiter := self.server.closedWaiter
		_ = self.server.Close()
		<-closedWaiter
		self.server = nil
	}
	self.Wakeup()
	self.manager.slock.Log().Infof("Arbiter member %s close", self.host)
	return nil
}

func (self *ArbiterMember) UpdateStatus() error {
	if self.isSelf {
		self.aofId = self.manager.GetCurrentAofID()
		self.lastUpdated = time.Now().UnixNano()
		return nil
	}

	if self.status != ARBITER_MEMBER_STATUS_ONLINE {
		return errors.New("not online error")
	}

	now := time.Now().UnixNano()
	request := protobuf.ArbiterStatusRequest{}
	data, err := request.Marshal()
	if err != nil {
		return err
	}

	callCommand := protocol.NewCallCommand("REPL_STATUS", data)
	callResultCommand, err := self.client.Request(callCommand)
	if err != nil {
		self.lastError++
		if self.lastError >= 3 {
			_ = self.client.protocol.Close()
		}
		return err
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		self.lastError++
		if self.lastError >= 3 {
			_ = self.client.protocol.Close()
		}
		self.manager.slock.Log().Warnf("Arbiter member %s update status error %v", self.host, err)
		return errors.New(fmt.Sprintf("call error %d %s", callResultCommand.Result, callResultCommand.ErrType))
	}

	response := protobuf.ArbiterStatusResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return err
	}

	self.aofId = self.manager.DecodeAofId(response.AofId)
	self.role = uint8(response.Role)
	self.lastUpdated = time.Now().UnixNano()
	self.lastDelay = self.lastUpdated - now
	self.lastError = 0
	return nil
}

func (self *ArbiterMember) clientOnline(client *ArbiterClient) error {
	if self.client != client {
		return nil
	}

	self.status = ARBITER_MEMBER_STATUS_ONLINE
	_ = self.manager.memberStatusUpdated(self)
	_ = self.manager.voter.WakeupRetryVote()
	return nil
}

func (self *ArbiterMember) clientOffline(client *ArbiterClient) error {
	if self.client != client {
		return nil
	}

	self.status = ARBITER_MEMBER_STATUS_OFFLINE
	_ = self.manager.memberStatusUpdated(self)
	return nil
}

func (self *ArbiterMember) Run() {
	if !self.isSelf {
		go func() {
			if self.client == nil {
				return
			}
			self.client.Run()
		}()
	} else {
		self.status = ARBITER_MEMBER_STATUS_ONLINE
		self.lastUpdated = time.Now().UnixNano()
	}

	for !self.closed {
		self.glock.Lock()
		self.wakeupSignal = make(chan bool, 1)
		self.glock.Unlock()

		select {
		case <-self.wakeupSignal:
			continue
		case <-time.After(5 * time.Second):
			self.glock.Lock()
			self.wakeupSignal = nil
			self.glock.Unlock()
			if self.client != nil {
				_ = self.UpdateStatus()
			}
		}
	}
	close(self.closedWaiter)
	self.manager.slock.Log().Infof("Arbiter member %s closed", self.host)
}

func (self *ArbiterMember) Wakeup() {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
}

func (self *ArbiterMember) DoVote() (*protobuf.ArbiterVoteResponse, error) {
	if self.isSelf {
		if self.abstianed {
			return nil, errors.New("stop vote")
		}

		aofId := self.manager.EncodeAofId(self.manager.GetCurrentAofID())
		return &protobuf.ArbiterVoteResponse{ErrMessage: "", Host: self.host, Weight: self.weight,
			Arbiter: self.arbiter, AofId: aofId, Role: uint32(self.role)}, nil
	}
	if self.status != ARBITER_MEMBER_STATUS_ONLINE {
		return nil, errors.New("not online")
	}

	request := protobuf.ArbiterVoteRequest{}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	callCommand := protocol.NewCallCommand("REPL_VOTE", data)
	callResultCommand, err := self.client.Request(callCommand)
	if err != nil {
		return nil, err
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		if callResultCommand.ErrType == "ERR_UNINIT" && self.manager.ownMember != nil {
			self.manager.DoAnnouncement()
		}
		return nil, errors.New(fmt.Sprintf("except code %d:%s", callResultCommand.Result, callResultCommand.ErrType))
	}

	response := protobuf.ArbiterVoteResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}

	self.aofId = self.manager.DecodeAofId(response.AofId)
	self.role = uint8(response.Role)
	self.manager.slock.Log().Infof("Arbiter member %s do vote succed", self.host)
	return &response, nil
}

func (self *ArbiterMember) DoProposal(proposalId uint64, host string, aofId [16]byte) (*protobuf.ArbiterProposalResponse, error) {
	if self.isSelf {
		return &protobuf.ArbiterProposalResponse{ErrMessage: ""}, nil
	}
	if self.status != ARBITER_MEMBER_STATUS_ONLINE {
		return nil, errors.New("not online")
	}

	request := protobuf.ArbiterProposalRequest{ProposalId: proposalId, AofId: self.manager.EncodeAofId(aofId), Host: host}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	callCommand := protocol.NewCallCommand("REPL_PROPOSAL", data)
	callResultCommand, err := self.client.Request(callCommand)
	if err != nil {
		return nil, err
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		if callResultCommand.ErrType == "ERR_PROPOSALID" {
			response := protobuf.ArbiterProposalResponse{}
			err = response.Unmarshal(callResultCommand.Data)
			if err == nil {
				self.manager.voter.proposalId = response.ProposalId
			}
		}
		return nil, errors.New(fmt.Sprintf("except code %d:%s", callResultCommand.Result, callResultCommand.ErrType))
	}

	response := protobuf.ArbiterProposalResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}

	self.manager.slock.Log().Infof("Arbiter member %s do vote succed", self.host)
	return &response, nil
}

func (self *ArbiterMember) DoCommit(proposalId uint64, host string, aofId [16]byte) (*protobuf.ArbiterCommitResponse, error) {
	if self.isSelf {
		return &protobuf.ArbiterCommitResponse{ErrMessage: ""}, nil
	}
	if self.status != ARBITER_MEMBER_STATUS_ONLINE {
		return nil, errors.New("not online")
	}

	request := protobuf.ArbiterCommitRequest{ProposalId: proposalId, AofId: self.manager.EncodeAofId(aofId), Host: host}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	callCommand := protocol.NewCallCommand("REPL_COMMIT", data)
	callResultCommand, err := self.client.Request(callCommand)
	if err != nil {
		return nil, err
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		return nil, errors.New(fmt.Sprintf("except code %d:%s", callResultCommand.Result, callResultCommand.ErrType))
	}

	response := protobuf.ArbiterCommitResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}

	self.manager.slock.Log().Infof("Arbiter member %s do commit succed", self.host)
	return &response, nil
}

func (self *ArbiterMember) DoAnnouncement() (*protobuf.ArbiterAnnouncementResponse, error) {
	if self.isSelf {
		return &protobuf.ArbiterAnnouncementResponse{ErrMessage: ""}, nil
	}

	if self.status != ARBITER_MEMBER_STATUS_ONLINE {
		return nil, errors.New("not online")
	}

	members := make([]*protobuf.ReplSetMember, 0)
	for _, member := range self.manager.members {
		rplm := &protobuf.ReplSetMember{Host: member.host, Weight: member.weight, Arbiter: member.arbiter, Role: uint32(member.role)}
		members = append(members, rplm)
	}

	replset := protobuf.ReplSet{Name: self.manager.name, Gid: self.manager.gid, Version: self.manager.version, Vertime: self.manager.vertime,
		Owner: self.host, Members: members, CommitId: self.manager.voter.commitId}
	request := protobuf.ArbiterAnnouncementRequest{FromHost: self.manager.ownMember.host, ToHost: self.host, Replset: &replset}
	data, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	callCommand := protocol.NewCallCommand("REPL_ANNOUNCEMENT", data)
	callResultCommand, err := self.client.Request(callCommand)
	if err != nil {
		return nil, err
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		self.manager.slock.Log().Errorf("Arbiter member %s do announcement error %d:%s", self.host, callResultCommand.Result, callResultCommand.ErrType)
		return nil, errors.New(fmt.Sprintf("except code %d:%s", callResultCommand.Result, callResultCommand.ErrType))
	}

	response := protobuf.ArbiterAnnouncementResponse{}
	err = response.Unmarshal(callResultCommand.Data)
	if err != nil {
		return nil, err
	}

	self.manager.slock.Log().Infof("Arbiter member %s do announcement succed", self.host)
	return &response, nil
}

type ArbiterVoter struct {
	manager          *ArbiterManager
	glock            *sync.Mutex
	commitId         uint64
	proposalId       uint64
	proposalHost     string
	proposalFromHost string
	voteHost         string
	voteAofId        [16]byte
	voting           bool
	closed           bool
	closedWaiter     chan bool
	wakeupSignal     chan bool
}

func NewArbiterVoter() *ArbiterVoter {
	return &ArbiterVoter{nil, &sync.Mutex{}, 0, 0, "", "", "", [16]byte{},
		false, false, make(chan bool, 1), nil}
}

func (self *ArbiterVoter) Close() error {
	self.glock.Lock()
	self.closed = true
	if !self.voting {
		close(self.closedWaiter)
		self.glock.Unlock()
		return nil
	}

	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
	self.manager.slock.Log().Infof("Arbiter voter close")
	return nil
}

func (self *ArbiterVoter) StartVote() error {
	self.glock.Lock()
	if self.closed || self.voting {
		if self.wakeupSignal != nil {
			close(self.wakeupSignal)
			self.wakeupSignal = nil
		}
		self.glock.Unlock()
		return errors.New("already voting")
	}
	self.voting = true
	self.glock.Unlock()

	go func() {
		self.manager.slock.Log().Infof("Arbiter voter start election")
		defer func() {
			self.glock.Lock()
			self.voting = false
			if self.closed {
				close(self.closedWaiter)
			}
			self.glock.Unlock()
			self.manager.slock.Log().Infof("Arbiter voter election finish")
		}()

		for !self.closed {
			if self.manager.ownMember == nil || len(self.manager.members) == 0 {
				return
			}

			if self.manager.leaderMember != nil {
				if self.manager.leaderMember.status == ARBITER_MEMBER_STATUS_ONLINE {
					self.manager.slock.Log().Infof("Arbier voter election finish, current leader %s", self.manager.leaderMember.host)
					return
				}
			}

			onlineCount := 0
			for _, member := range self.manager.members {
				if member.status == ARBITER_MEMBER_STATUS_ONLINE {
					if member.host == self.proposalHost && self.manager.ownMember.host != self.proposalHost {
						self.manager.slock.Log().Infof("Arbier voter wait announcement, current leader %s", self.proposalHost)
						return
					}
					onlineCount++
				}
			}

			if onlineCount < len(self.manager.members)/2+1 {
				self.manager.slock.Log().Infof("Arbier voter online member not enough, total %d online %d", len(self.manager.members), onlineCount)
				_ = self.sleepWhenRetryVote()
				continue
			}

			err := self.DoVote()
			if err == nil {
				err = self.DoProposal()
				if err == nil {
					err = self.DoCommit()
					if err == nil {
						err = self.manager.voteSucced()
						if err == nil {
							return
						}
					}
				}
			}

			_ = self.sleepWhenRetryVote()
		}
		return
	}()
	return nil
}

func (self *ArbiterVoter) DoRequests(name string, handler func(*ArbiterMember) (interface{}, error)) []interface{} {
	members, finishCount := self.manager.members, 0
	responses, requestWaiter := make([]interface{}, 0), make(chan bool, 1)
	for _, member := range members {
		go func(member *ArbiterMember) {
			response, err := handler(member)
			if err == nil {
				responses = append(responses, response)
			} else {
				self.manager.slock.Log().Errorf("Arbier voter member %s request %s error %v", member.host, name, err)
			}

			self.glock.Lock()
			finishCount++
			if finishCount >= len(members) {
				close(requestWaiter)
			}
			self.glock.Unlock()
		}(member)
	}

	if len(members) > 0 {
		<-requestWaiter
	}
	return responses
}

func (self *ArbiterVoter) DoVote() error {
	self.voteHost, self.voteAofId = "", [16]byte{}
	responses := self.DoRequests("do vote", func(member *ArbiterMember) (interface{}, error) {
		return member.DoVote()
	})

	if len(responses) < len(self.manager.members)/2+1 {
		return errors.New("vote error")
	}

	var selectVoteResponse *protobuf.ArbiterVoteResponse = nil
	for _, response := range responses {
		voteResponse := response.(*protobuf.ArbiterVoteResponse)
		if voteResponse.Arbiter != 0 || voteResponse.Weight == 0 {
			continue
		}

		if selectVoteResponse == nil {
			selectVoteResponse = voteResponse
			continue
		}

		if selectVoteResponse.AofId == voteResponse.AofId {
			if selectVoteResponse.Weight < voteResponse.Weight {
				selectVoteResponse = voteResponse
			}
			if selectVoteResponse.Weight == voteResponse.Weight {
				if selectVoteResponse.Host < voteResponse.Host {
					selectVoteResponse = voteResponse
				}
			}
			continue
		}

		if self.manager.CompareAofId(self.manager.DecodeAofId(voteResponse.AofId), self.manager.DecodeAofId(selectVoteResponse.AofId)) > 0 {
			selectVoteResponse = voteResponse
		}
	}

	if selectVoteResponse == nil {
		return errors.New("not found")
	}

	self.voteHost = selectVoteResponse.Host
	self.voteAofId = self.manager.DecodeAofId(selectVoteResponse.AofId)
	self.manager.slock.Log().Infof("Arbier voter do vote succed,  host %s aof_id %x proposal_id %d", self.voteHost, self.voteAofId, self.proposalId)
	return nil
}

func (self *ArbiterVoter) DoProposal() error {
	self.proposalId++
	responses := self.DoRequests("do proposal", func(member *ArbiterMember) (interface{}, error) {
		return member.DoProposal(self.proposalId, self.voteHost, self.voteAofId)
	})

	if len(responses) < len(self.manager.members)/2+1 {
		return errors.New("member accept proposal count too small")
	}
	self.manager.slock.Log().Infof("Arbier voter do proposal succed, host %s aof_id %x proposal_id %d", self.voteHost, self.voteAofId, self.proposalId)
	return nil
}

func (self *ArbiterVoter) DoCommit() error {
	self.proposalHost = self.voteHost
	self.proposalFromHost = self.manager.ownMember.host
	self.commitId = self.proposalId

	responses := self.DoRequests("do commit", func(member *ArbiterMember) (interface{}, error) {
		return member.DoCommit(self.proposalId, self.proposalHost, self.voteAofId)
	})

	if len(responses) < len(self.manager.members)/2+1 {
		self.proposalHost = ""
		self.proposalFromHost = ""
		return errors.New("member accept proposal count too small")
	}

	self.manager.slock.Log().Infof("Arbier voter do commit succed, host %s aof_id %x commit_id %d", self.voteHost, self.voteAofId, self.commitId)
	return nil
}

func (self *ArbiterVoter) DoAnnouncement() error {
	self.manager.slock.Log().Infof("Arbiter replication do announcement start")
	for _, member := range self.manager.members {
		if member.role == ARBITER_ROLE_LEADER {
			_, err := member.DoAnnouncement()
			if err != nil {
				if member.host == self.proposalHost && !member.isSelf && self.voting {
					self.proposalHost = ""
					self.proposalFromHost = ""
					self.manager.slock.Log().Infof("Arbiter voter do announcement to leader %s error %v, will restart election", member.host, err)
					return err
				}
				self.manager.slock.Log().Infof("Arbiter voter do announcement to leader %s error %v", member.host, err)
				return err
			}
		}
	}

	for _, member := range self.manager.members {
		if member.role != ARBITER_ROLE_LEADER {
			_, err := member.DoAnnouncement()
			if err != nil {
				self.manager.slock.Log().Infof("Arbiter voter announcement to follower %s error %v", member.host, err)
			}
		}
	}
	self.manager.slock.Log().Infof("Arbiter replication do announcement finish")
	return nil
}

func (self *ArbiterVoter) sleepWhenRetryVote() error {
	self.glock.Lock()
	self.wakeupSignal = make(chan bool, 1)
	self.glock.Unlock()

	delay_time := int64(5000)
	n, err := rand.Int(rand.Reader, big.NewInt(5000))
	if err == nil {
		delay_time = 100 + n.Int64()
	}

	select {
	case <-self.wakeupSignal:
		return nil
	case <-time.After(time.Duration(delay_time) * time.Millisecond):
		self.glock.Lock()
		self.wakeupSignal = nil
		self.glock.Unlock()
		return nil
	}
}

func (self *ArbiterVoter) WakeupRetryVote() error {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
	return nil
}

type ArbiterManager struct {
	slock        *SLock
	glock        *sync.Mutex
	store        *ArbiterStore
	voter        *ArbiterVoter
	members      []*ArbiterMember
	ownMember    *ArbiterMember
	leaderMember *ArbiterMember
	name         string
	gid          string
	version      uint32
	vertime      uint64
	stoped       bool
	loaded       bool
}

func NewArbiterManager(slock *SLock, name string) *ArbiterManager {
	store := NewArbiterStore()
	voter := NewArbiterVoter()
	manager := &ArbiterManager{slock, &sync.Mutex{}, store, voter,
		make([]*ArbiterMember, 0), nil, nil, name, "", 1, 0,
		false, false}
	voter.manager = manager
	return manager
}

func (self *ArbiterManager) Load() error {
	defer self.glock.Unlock()
	self.glock.Lock()

	err := self.store.Load(self)
	if err != nil {
		self.slock.Log().Errorf("Arbiter load meta file error %v", err)
	}

	aofId, err := self.slock.aof.LoadMaxId()
	if err != nil {
		self.slock.Log().Errorf("Arbiter load aof file maxid error %v", err)
	} else {
		self.slock.Log().Infof("Arbiter load aof file maxid %x", aofId)
	}
	self.slock.replicationManager.currentRequestId = aofId
	self.voter.proposalId = self.voter.commitId
	return nil
}

func (self *ArbiterManager) Start() error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.ownMember == nil || len(self.members) == 0 {
		return nil
	}

	for _, member := range self.members {
		_ = member.Open()
		go member.Run()
	}

	self.slock.updateState(STATE_VOTE)
	err := self.StartVote()
	if err != nil {
		self.slock.Log().Errorf("Arbiter start election error %v", err)
	}
	self.slock.Log().Infof("Arbiter start")
	return nil
}

func (self *ArbiterManager) Close() error {
	if self.stoped {
		return nil
	}

	if self.ownMember == nil || len(self.members) == 0 {
		self.stoped = true
		self.slock.logger.Infof("Arbiter closed")
		return nil
	}

	self.glock.Lock()
	if self.ownMember.role == ARBITER_ROLE_LEADER && len(self.members) > 1 {
		self.ownMember.abstianed = true
		_ = self.QuitLeader()
	}
	self.stoped = true
	self.glock.Unlock()

	_ = self.voter.Close()
	<-self.voter.closedWaiter
	for _, member := range self.members {
		_ = member.Close()
		<-member.closedWaiter
	}
	self.slock.logger.Infof("Arbiter closed")
	return nil
}

func (self *ArbiterManager) Config(host string, weight uint32, arbiter uint32) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.ownMember != nil || len(self.members) != 0 {
		return errors.New("member info error")
	}

	_, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return errors.New("host invalid error")
	}

	member := NewArbiterMember(self, host, weight, arbiter)
	member.isSelf = true
	err = member.Open()
	if err != nil {
		return err
	}

	self.ownMember = member
	self.members = append(self.members, member)
	go member.Run()
	self.slock.updateState(STATE_VOTE)
	self.gid = self.EncodeAofId(protocol.GenRequestId())
	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	self.voter.proposalId = uint64(self.version)
	_ = self.store.Save(self)
	err = self.StartVote()
	if err != nil {
		self.slock.Log().Errorf("Arbiter config error %v", err)
	}
	self.slock.logger.Infof("Arbiter config host %s weight %d arbiter %d", host, weight, arbiter)
	return nil
}

func (self *ArbiterManager) AddMember(host string, weight uint32, arbiter uint32) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.ownMember == nil || len(self.members) == 0 {
		return errors.New("member info error")
	}

	_, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return errors.New("host invalid error")
	}

	for _, member := range self.members {
		if member.host == host {
			return errors.New("has save member error")
		}
	}

	member := NewArbiterMember(self, host, weight, arbiter)
	member.role = ARBITER_ROLE_FOLLOWER
	err = member.Open()
	if err != nil {
		return err
	}

	self.members = append(self.members, member)
	go member.Run()
	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	_ = self.store.Save(self)
	self.DoAnnouncement()
	_ = self.updateStatus()
	self.slock.logger.Infof("Arbiter add member host %s weight %d arbiter %d", host, weight, arbiter)
	return nil
}

func (self *ArbiterManager) RemoveMember(host string) error {
	self.glock.Lock()
	if self.ownMember == nil || len(self.members) == 0 {
		self.glock.Unlock()
		return errors.New("member info error")
	}

	if self.ownMember.host == host {
		self.glock.Unlock()
		return errors.New("can not remove self error")
	}

	var currentMember *ArbiterMember = nil
	members := make([]*ArbiterMember, 0)
	for _, member := range self.members {
		if member.host == host {
			currentMember = member
		} else {
			members = append(members, member)
		}
	}

	if currentMember == nil {
		self.glock.Unlock()
		return errors.New("not found member")
	}

	self.members = members
	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	_ = self.store.Save(self)
	self.DoAnnouncement()
	_ = self.updateStatus()
	self.glock.Unlock()

	_, _ = currentMember.DoAnnouncement()
	_ = currentMember.Close()
	<-currentMember.closedWaiter
	self.slock.logger.Infof("Arbiter remove member host %s", host)
	return nil
}

func (self *ArbiterManager) UpdateMember(host string, weight uint32, arbiter uint32) error {
	defer self.glock.Unlock()
	self.glock.Lock()

	if self.ownMember == nil || len(self.members) == 0 {
		return errors.New("member info error")
	}

	_, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return errors.New("host invalid error")
	}

	var currentMember *ArbiterMember = nil
	members := make([]*ArbiterMember, 0)
	for _, member := range self.members {
		if member.host == host {
			currentMember = member
		} else {
			members = append(members, member)
		}
	}

	if currentMember == nil {
		return errors.New("not found member")
	}
	currentMember.weight = weight
	currentMember.arbiter = arbiter
	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	_ = self.store.Save(self)
	if self.ownMember.role == ARBITER_ROLE_LEADER && self.ownMember.weight == 0 {
		_ = self.QuitLeader()
	} else {
		self.DoAnnouncement()
		_ = self.updateStatus()
	}
	self.slock.logger.Infof("Arbiter update member host %s weight %d arbiter %d", host, weight, arbiter)
	return nil
}

func (self *ArbiterManager) GetMembers() []*ArbiterMember {
	return self.members
}

func (self *ArbiterManager) QuitLeader() error {
	self.slock.Log().Infof("Arbiter quit leader start")
	err := self.slock.replicationManager.transparencyManager.ChangeLeader("")
	if err != nil {
		self.slock.Log().Errorf("Arbiter quit leader change transparency address error %v", err)
	}
	err = self.slock.replicationManager.SwitchToFollower("")
	if err != nil {
		self.slock.Log().Errorf("Arbiter equit leader change to follower error %v", err)
	}
	self.ownMember.role = ARBITER_ROLE_FOLLOWER
	self.leaderMember = nil
	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	_ = self.store.Save(self)
	self.glock.Unlock()
	_ = self.voter.DoAnnouncement()
	self.glock.Lock()
	self.slock.Log().Infof("Arbiter quit leader finish")
	return nil
}

func (self *ArbiterManager) QuitMember() error {
	self.slock.Log().Infof("Arbiter quit members start")
	_ = self.slock.replicationManager.SwitchToFollower("")

	self.glock.Lock()
	members := self.members
	self.members = make([]*ArbiterMember, 0)
	self.ownMember = nil
	self.leaderMember = nil
	self.voter.proposalId = 0
	self.voter.proposalHost = ""
	self.voter.proposalFromHost = ""
	self.voter.commitId = 0
	self.version = 1
	self.vertime = 0
	_ = self.store.Save(self)
	self.glock.Unlock()

	for _, member := range members {
		_ = member.Close()
		<-member.closedWaiter
	}
	self.slock.Log().Infof("Arbiter quit members finish")
	return nil
}

func (self *ArbiterManager) DoAnnouncement() {
	if self.stoped {
		return
	}

	go func() {
		err := self.voter.DoAnnouncement()
		if err != nil {
			self.slock.Log().Errorf("Arbiter announcement error %v", err)
		}
	}()
}

func (self *ArbiterManager) StartVote() error {
	if self.stoped {
		return nil
	}

	_ = self.voter.StartVote()
	return nil
}

func (self *ArbiterManager) voteSucced() error {
	if self.stoped {
		return nil
	}

	self.slock.Log().Infof("Arbiter election succed, current leader %s", self.voter.proposalHost)
	self.glock.Lock()
	proposalHost := self.voter.proposalHost
	for _, member := range self.members {
		if member.host == self.voter.proposalHost {
			member.role = ARBITER_ROLE_LEADER
			self.leaderMember = member
			if member.host == self.ownMember.host {
				self.voter.proposalHost = ""
				self.voter.proposalFromHost = ""
			}
		} else {
			if member.arbiter != 0 {
				member.role = ARBITER_ROLE_ARBITER
			} else {
				member.role = ARBITER_ROLE_FOLLOWER
			}
		}
	}

	self.version++
	self.vertime = uint64(time.Now().UnixNano()) / 1e6
	_ = self.store.Save(self)
	if proposalHost == self.ownMember.host {
		if self.slock.state == STATE_LEADER {
			self.DoAnnouncement()
		} else {
			_ = self.updateStatus()
		}
	} else {
		if self.slock.state == STATE_LEADER {
			err := self.slock.replicationManager.transparencyManager.ChangeLeader("")
			if err != nil {
				self.slock.Log().Errorf("Arbiter election succed change transparency address error %v", err)
			}
			err = self.slock.replicationManager.SwitchToFollower("")
			if err != nil {
				self.slock.Log().Errorf("Arbiter election succed change to follower error %v", err)
			}
		}
		self.glock.Unlock()
		err := self.voter.DoAnnouncement()
		if err != nil {
			self.slock.Log().Infof("Arbiter election succed change status announcement leader fail")
			return err
		}
		self.glock.Lock()
		_ = self.updateStatus()
	}

	self.slock.Log().Infof("Arbiter election succed change status finish")
	self.glock.Unlock()
	return nil
}

func (self *ArbiterManager) updateStatus() error {
	if self.stoped {
		return nil
	}

	for _, member := range self.members {
		if member.isSelf {
			self.ownMember = member
		}
		if member.role == ARBITER_ROLE_LEADER {
			self.leaderMember = member
		}
	}

	if self.ownMember == nil {
		return nil
	}

	if self.leaderMember == nil {
		err := self.slock.replicationManager.transparencyManager.ChangeLeader("")
		if err != nil {
			self.slock.Log().Errorf("Arbiter update status reset transparency address error %v", err)
		}
		err = self.slock.replicationManager.SwitchToFollower("")
		if err != nil {
			self.slock.Log().Errorf("Arbiter update status reset follower error %v", err)
		}
		_ = self.StartVote()
		return nil
	}

	if self.leaderMember.host == self.ownMember.host {
		if self.slock.state != STATE_LEADER {
			if self.ownMember.arbiter == 0 {
				if !self.loaded {
					err := self.slock.initLeader()
					if err != nil {
						self.slock.Log().Errorf("Arbiter update status init leader error %v", err)
						go func() {
							self.glock.Lock()
							_ = self.QuitLeader()
							self.glock.Unlock()
						}()
						return err
					}
					self.slock.startLeader()
					self.loaded = true
					self.DoAnnouncement()
					_ = self.voter.WakeupRetryVote()
					return nil
				}

				err := self.slock.replicationManager.SwitchToLeader()
				if err != nil {
					self.slock.Log().Errorf("Arbiter update status change leader error %v", err)
					go func() {
						self.glock.Lock()
						_ = self.QuitLeader()
						self.glock.Unlock()
					}()
					return err
				}
				err = self.slock.replicationManager.transparencyManager.ChangeLeader("")
				if err != nil {
					self.slock.Log().Errorf("Arbiter update status change transparency address error %v", err)
				}
				self.DoAnnouncement()
			} else {
				_ = self.QuitLeader()
			}
			_ = self.voter.WakeupRetryVote()
		}
		return nil
	}

	if self.ownMember.arbiter == 0 {
		if !self.loaded {
			err := self.slock.initFollower(self.leaderMember.host)
			if err != nil {
				self.slock.Log().Errorf("Arbiter update status init follower error %v", err)
			}
			self.slock.startFollower()
			err = self.slock.replicationManager.transparencyManager.ChangeLeader(self.leaderMember.host)
			if err != nil {
				self.slock.Log().Errorf("Arbiter update status change transparency address error %v", err)
			}
			self.loaded = true
			_ = self.voter.WakeupRetryVote()
			return nil
		}

		err := self.slock.replicationManager.transparencyManager.ChangeLeader(self.leaderMember.host)
		if err != nil {
			self.slock.Log().Errorf("Arbiter update status change transparency address error %v", err)
		}
		err = self.slock.replicationManager.SwitchToFollower(self.leaderMember.host)
		if err != nil {
			self.slock.Log().Errorf("Arbiter update status change follower error %v", err)
		}
		_ = self.voter.WakeupRetryVote()
		return nil
	}

	err := self.slock.replicationManager.SwitchToFollower("")
	if err != nil {
		self.slock.Log().Errorf("Arbiter update status reset follower error %v", err)
	}
	err = self.slock.replicationManager.transparencyManager.ChangeLeader(self.leaderMember.host)
	if err != nil {
		self.slock.Log().Errorf("Arbiter update status reset transparency address error %v", err)
	}
	_ = self.voter.WakeupRetryVote()
	return nil
}

func (self *ArbiterManager) memberStatusUpdated(member *ArbiterMember) error {
	if self.ownMember == nil || self.stoped {
		return nil
	}

	if member.host == self.voter.proposalHost && member.status != ARBITER_MEMBER_STATUS_ONLINE {
		self.voter.proposalHost = ""
		self.voter.proposalFromHost = ""
		err := self.StartVote()
		if err != nil {
			self.slock.Log().Errorf("Arbiter proposal host offline restart election error %v", err)
			return nil
		}
		self.slock.Log().Infof("Arbiter proposal host offline restart election")
		return nil
	}

	if member.host == self.voter.proposalFromHost && member.status != ARBITER_MEMBER_STATUS_ONLINE {
		self.voter.proposalHost = ""
		self.voter.proposalFromHost = ""
		err := self.StartVote()
		if err != nil {
			self.slock.Log().Errorf("Arbiter proposal vote host offline restart election error %v", err)
			return nil
		}
		self.slock.Log().Infof("Arbiter proposal host vote offline restart election")
		return nil
	}

	if self.voter.proposalHost == "" && self.leaderMember == nil {
		err := self.StartVote()
		if err != nil {
			self.slock.Log().Errorf("Arbiter leader reset restart election error %v", err)
			return nil
		}
		self.slock.Log().Infof("Arbiter leader reset restart election")
		return nil
	}

	if member.role == ARBITER_ROLE_LEADER {
		if member.status != ARBITER_MEMBER_STATUS_ONLINE {
			err := self.StartVote()
			if err != nil {
				self.slock.Log().Errorf("Arbiter leader offline restart election error %v", err)
				return nil
			}
			self.slock.Log().Infof("Arbiter leader offline restart election")
		}
		return nil
	}

	if self.ownMember.role == ARBITER_ROLE_LEADER {
		if member.status == ARBITER_MEMBER_STATUS_ONLINE {
			self.DoAnnouncement()
			return nil
		}

		onlineCount := 0
		for _, member := range self.members {
			if member.status == ARBITER_MEMBER_STATUS_ONLINE {
				onlineCount++
			}
		}

		if onlineCount < len(self.members)/2+1 {
			go func() {
				self.glock.Lock()
				_ = self.QuitLeader()
				self.glock.Unlock()
				err := self.StartVote()
				if err != nil {
					self.slock.Log().Errorf("Arbiter member online not enough restart election %v", err)
				}
				self.slock.Log().Infof("Arbiter member online not enough restart election")
			}()
		}
	}
	return nil
}

func (self *ArbiterManager) GetCurrentAofID() [16]byte {
	if self.ownMember == nil {
		return [16]byte{}
	}

	if self.ownMember.arbiter != 0 {
		aofId := [16]byte{}
		for _, member := range self.members {
			if member.role != ARBITER_ROLE_ARBITER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
				if self.CompareAofId(member.aofId, aofId) > 0 {
					aofId = member.aofId
				}
			}
		}
		self.ownMember.aofId = aofId
		return aofId
	}
	return self.slock.replicationManager.GetCurrentAofID()
}

func (self *ArbiterManager) EncodeAofId(aofId [16]byte) string {
	return fmt.Sprintf("%x", aofId)
}

func (self *ArbiterManager) DecodeAofId(aofId string) [16]byte {
	buf, err := hex.DecodeString(aofId)
	if err != nil || len(buf) != 16 {
		return [16]byte{}
	}
	return [16]byte{buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]}
}

func (self *ArbiterManager) CompareAofId(a [16]byte, b [16]byte) int {
	if a == b {
		return 0
	}

	aid := uint64(a[0])<<32 | uint64(a[1])<<40 | uint64(a[2])<<48 | uint64(a[3])<<56 | uint64(a[4]) | uint64(a[5])<<8 | uint64(a[6])<<16 | uint64(a[7])<<24
	acommandTime := uint64(a[8]) | uint64(a[9])<<8 | uint64(a[10])<<16 | uint64(a[11])<<24 | uint64(a[12])<<32 | uint64(a[13])<<40 | uint64(a[14])<<48 | uint64(a[15])<<56

	bid := uint64(b[0])<<32 | uint64(b[1])<<40 | uint64(b[2])<<48 | uint64(b[3])<<56 | uint64(b[4]) | uint64(b[5])<<8 | uint64(b[6])<<16 | uint64(b[7])<<24
	bcommandTime := uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 | uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56

	if aid > bid {
		return 1
	}
	if aid < bid {
		return -1
	}
	if acommandTime > bcommandTime {
		return 1
	}
	return -1
}

func (self *ArbiterManager) GetCallMethods() map[string]BinaryServerProtocolCallHandler {
	handlers := make(map[string]BinaryServerProtocolCallHandler, 2)
	handlers["REPL_CONNECT"] = self.commandHandleConnectCommand
	handlers["REPL_VOTE"] = self.commandHandleVoteCommand
	handlers["REPL_PROPOSAL"] = self.commandHandleProposalCommand
	handlers["REPL_COMMIT"] = self.commandHandleCommitCommand
	handlers["REPL_ANNOUNCEMENT"] = self.commandHandleAnnouncementCommand
	handlers["REPL_STATUS"] = self.commandHandleStatusCommand
	return handlers
}

func (self *ArbiterManager) commandHandleConnectCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}

	request := protobuf.ArbiterConnectRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if self.ownMember != nil && len(self.members) > 0 {
		var currentMember *ArbiterMember = nil
		for _, member := range self.members {
			if member.host == request.FromHost {
				currentMember = member
				break
			}
		}

		if currentMember == nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_NOT_MEMBER", nil), nil
		}
	}

	server := NewArbiterServer(serverProtocol)
	response, err := server.handleInit(self, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_CALL", nil), nil
	}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	serverProtocol.stream.streamType = STREAM_TYPE_ARBITER
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) commandHandleVoteCommand(_ *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}

	request := protobuf.ArbiterVoteRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if self.ownMember == nil || len(self.members) == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
	}

	if self.ownMember.abstianed {
		return protocol.NewCallResultCommand(command, 0, "ERR_ABSTIANED", nil), nil
	}

	response := protobuf.ArbiterVoteResponse{ErrMessage: "", Host: self.ownMember.host, Weight: self.ownMember.weight,
		Arbiter: self.ownMember.arbiter, AofId: self.EncodeAofId(self.GetCurrentAofID()), Role: uint32(self.ownMember.role)}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) commandHandleProposalCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}

	request := protobuf.ArbiterProposalRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if self.ownMember == nil || len(self.members) == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
	}

	if self.ownMember.role == ARBITER_ROLE_LEADER {
		self.DoAnnouncement()
		return protocol.NewCallResultCommand(command, 0, "ERR_ROLE", nil), nil
	}

	var voteMember *ArbiterMember = nil
	var voteFromMember *ArbiterMember = nil
	for _, member := range self.members {
		if member.role == ARBITER_ROLE_LEADER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
			self.DoAnnouncement()
			return protocol.NewCallResultCommand(command, 0, "ERR_STATUS", nil), nil
		}

		if member.host == request.Host {
			voteMember = member
		}
		if member.server != nil && member.server.protocol == serverProtocol {
			voteFromMember = member
		}

		if self.CompareAofId(member.aofId, self.DecodeAofId(request.AofId)) > 0 {
			return protocol.NewCallResultCommand(command, 0, "ERR_AOFID", nil), nil
		}
	}

	if voteMember == nil || voteFromMember == nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
	}

	if voteMember.status != ARBITER_MEMBER_STATUS_ONLINE {
		return protocol.NewCallResultCommand(command, 0, "ERR_OFFLINE", nil), nil
	}

	if self.voter.proposalId >= request.ProposalId || self.voter.proposalHost != "" {
		response := protobuf.ArbiterProposalResponse{ErrMessage: "", ProposalId: self.voter.proposalId}
		data, err := response.Marshal()
		if err != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		return protocol.NewCallResultCommand(command, 0, "ERR_PROPOSALID", data), nil
	}

	response := protobuf.ArbiterProposalResponse{ErrMessage: "", ProposalId: self.voter.proposalId}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}

	self.voter.proposalId = request.ProposalId
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) commandHandleCommitCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}

	request := protobuf.ArbiterCommitRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if self.ownMember == nil || len(self.members) == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
	}

	var voteMember *ArbiterMember = nil
	var voteFromMember *ArbiterMember = nil
	for _, member := range self.members {
		if member.host == request.Host {
			voteMember = member
		}
		if member.server != nil && member.server.protocol == serverProtocol {
			voteFromMember = member
		}
	}

	if voteMember == nil || voteFromMember == nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
	}

	if self.voter.commitId >= request.ProposalId {
		response := protobuf.ArbiterCommitResponse{ErrMessage: "", CommitId: self.voter.commitId}
		data, err := response.Marshal()
		if err != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		return protocol.NewCallResultCommand(command, 0, "ERR_COMMITID", data), nil
	}

	response := protobuf.ArbiterCommitResponse{ErrMessage: "", CommitId: self.voter.commitId}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	self.voter.proposalHost = request.Host
	self.voter.proposalFromHost = voteFromMember.host
	self.voter.commitId = request.ProposalId
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) commandHandleAnnouncementCommand(serverProtocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}
	defer self.slock.Log().Infof("Arbiter handle announcementcommand finish")
	self.slock.Log().Infof("Arbiter handle announcementcommand start")

	request := protobuf.ArbiterAnnouncementRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if request.Replset.Name != self.name {
		return protocol.NewCallResultCommand(command, 0, "ERR_NAME", nil), nil
	}

	if self.ownMember != nil && len(self.members) > 0 && request.Replset.Gid != self.gid {
		return protocol.NewCallResultCommand(command, 0, "ERR_GID", nil), nil
	}

	self.glock.Lock()
	if request.Replset.CommitId == self.voter.commitId && self.voter.proposalHost != "" {
		if self.version < request.Replset.Version {
			self.version = request.Replset.Version
		}
		if self.vertime < request.Replset.Vertime {
			self.vertime = request.Replset.Vertime
		}
	} else {
		if request.Replset.Version < self.version || (request.Replset.Version == self.version && request.Replset.Vertime < self.vertime) {
			self.slock.Log().Infof("Arbiter handle announcement version waring CommitId %d %d Version %d %d Vertime %d %d", request.Replset.CommitId,
				self.voter.commitId, request.Replset.Version, self.version, request.Replset.Vertime, self.vertime)
			self.glock.Unlock()
			return protocol.NewCallResultCommand(command, 0, "ERR_VERSION", nil), nil
		}
	}

	members, newMembers, unopenMembers := make(map[string]*ArbiterMember, 4), make([]*ArbiterMember, 0), make([]*ArbiterMember, 0)
	for _, member := range self.members {
		members[member.host] = member
	}

	var ownMember, leaderMember *ArbiterMember = nil, nil
	for _, rplm := range request.Replset.Members {
		if member, ok := members[rplm.Host]; ok {
			member.weight = rplm.Weight
			member.arbiter = rplm.Arbiter
			if rplm.Role != ARBITER_ROLE_UNKNOWN {
				member.role = uint8(rplm.Role)
			}
			delete(members, rplm.Host)
			if member.host == request.Replset.Owner {
				ownMember = member
				ownMember.isSelf = true
			}
			if member.role == ARBITER_ROLE_LEADER {
				leaderMember = member
			}
			newMembers = append(newMembers, member)
		} else {
			member = NewArbiterMember(self, rplm.Host, rplm.Weight, rplm.Arbiter)
			member.role = uint8(rplm.Role)
			if member.host == request.Replset.Owner {
				ownMember = member
				ownMember.isSelf = true
			}
			if member.role == ARBITER_ROLE_LEADER {
				leaderMember = member
			}
			newMembers = append(newMembers, member)
			unopenMembers = append(unopenMembers, member)
		}
	}

	if ownMember == nil {
		go func() {
			self.glock.Lock()
			if self.ownMember.role == ARBITER_ROLE_LEADER {
				_ = self.QuitLeader()
			}
			self.glock.Unlock()
			_ = self.QuitMember()
		}()
		self.glock.Unlock()
		return protocol.NewCallResultCommand(command, 0, "", nil), nil
	}

	leaderHost := ""
	if leaderMember != nil {
		if request.Replset.CommitId != self.voter.commitId || self.voter.proposalHost == "" {
			if ownMember.role == ARBITER_ROLE_LEADER && leaderMember.host != ownMember.host {
				self.glock.Unlock()
				return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
			}

			if self.leaderMember != nil {
				if self.leaderMember.status == ARBITER_MEMBER_STATUS_ONLINE && leaderMember.host != self.leaderMember.host {
					self.glock.Unlock()
					return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
				}
			}
		}

		leaderHost = leaderMember.host
		if request.Replset.CommitId >= self.voter.commitId && self.voter.proposalHost != "" {
			self.slock.Log().Infof("Arbiter handle announcement accept leader %s", self.voter.proposalHost)
			self.voter.proposalHost = ""
			self.voter.proposalFromHost = ""
		}
	}

	self.members = newMembers
	self.ownMember = ownMember
	self.leaderMember = leaderMember
	self.gid = request.Replset.Gid
	self.version = request.Replset.Version
	self.vertime = request.Replset.Vertime
	_ = self.store.Save(self)
	if self.ownMember.role == ARBITER_ROLE_LEADER {
		_ = self.updateStatus()
	}
	self.glock.Unlock()
	self.slock.Log().Infof("Arbiter handle announcement from %s leader %s member count %d version %d commitid %d",
		request.FromHost, leaderHost, len(newMembers), request.Replset.Version, request.Replset.CommitId)

	go func() {
		for _, member := range unopenMembers {
			_ = member.Open()
			go member.Run()
		}
		for _, member := range members {
			_ = member.Close()
		}

		if self.ownMember.role != ARBITER_ROLE_LEADER {
			self.glock.Lock()
			err := self.updateStatus()
			if err != nil {
				self.slock.Log().Errorf("Arbiter handle announcement update status error %v", err)
			}
			self.glock.Unlock()
		} else {
			if self.ownMember.weight == 0 {
				self.glock.Lock()
				_ = self.QuitLeader()
				self.glock.Unlock()
			}
		}
		self.slock.Log().Infof("Arbiter handle announcementcommand update status succed")
	}()

	for _, member := range self.members {
		if member.host == request.FromHost && member.server == nil {
			server := NewArbiterServer(serverProtocol)
			_ = server.Attach(self, request.FromHost)
			break
		}
	}

	response := protobuf.ArbiterAnnouncementResponse{ErrMessage: ""}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) commandHandleStatusCommand(_ *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.stoped {
		return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
	}

	request := protobuf.ArbiterStatusRequest{}
	err := request.Unmarshal(command.Data)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
	}

	if self.ownMember == nil || len(self.members) == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
	}

	aofId := self.EncodeAofId(self.GetCurrentAofID())
	response := protobuf.ArbiterStatusResponse{ErrMessage: "", AofId: aofId, Role: uint32(self.ownMember.role)}
	data, err := response.Marshal()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	return protocol.NewCallResultCommand(command, 0, "", data), nil
}
