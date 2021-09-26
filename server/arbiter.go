package server

import (
    "crypto/rand"
    "encoding/hex"
    "errors"
    "fmt"
    "github.com/snower/slock/client"
    "github.com/snower/slock/protocol"
    "github.com/snower/slock/server/protobuf"
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
    filename        string
}

func NewArbiterStore() *ArbiterStore {
    return &ArbiterStore{""}
}

func (self *ArbiterStore) Init(manager *ArbiterManager) error {
    data_dir, err := filepath.Abs(Config.DataDir)
    if err != nil {
        manager.slock.Log().Errorf("Arbiter config meta data dir error %v", err)
        return err
    }

    if _, err := os.Stat(data_dir); os.IsNotExist(err) {
        manager.slock.Log().Errorf("Arbiter config meta data dir error %v", err)
        return err
    }
    
    self.filename = filepath.Join(data_dir, "meta.pb")
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
    data, err = self.ReadHeader(data)
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
            member.isself = true
            manager.own_member = member
        }
        members = append(members, member)
    }
    if manager.own_member == nil {
        _ = file.Close()
        return errors.New("unknown own member info")
    }
    manager.members = members
    manager.gid = replset.Gid
    manager.version = replset.Version - 1
    manager.vertime = replset.Vertime
    manager.voter.commit_id = replset.CommitId
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

    file, err := os.OpenFile(self.filename, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0644)
    if err != nil {
        manager.slock.Log().Errorf("Arbiter open meta file error %v", err)
        return err
    }

    err = self.WriteHeader(file)
    if err != nil {
        manager.slock.Log().Errorf("Arbiter write meta file header error %v", err)
        _ = file.Close()
        return err
    }

    members := make([]*protobuf.ReplSetMember, 0)
    for _, member := range manager.members {
        rplm := &protobuf.ReplSetMember{Host:member.host, Weight:member.weight, Arbiter:member.arbiter, Role:uint32(member.role)}
        members = append(members, rplm)
    }

    owner := ""
    if manager.own_member != nil {
        owner = manager.own_member.host
    }
    replset := protobuf.ReplSet{Name:manager.name, Gid:manager.gid, Version:manager.version, Vertime:manager.vertime,
        Owner:owner, Members:members, CommitId:manager.voter.commit_id}
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

func (self *ArbiterStore) ReadHeader(buf []byte) ([]byte, error) {
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

    header_len := uint16(buf[9]) | uint16(buf[10])<<8
    if header_len != 0x0000 {
        return nil, errors.New("Meta File Header Len Error")
    }
    return buf[header_len + 11:], nil
}

func (self *ArbiterStore) WriteHeader(file *os.File) error {
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
    member          *ArbiterMember
    glock           *sync.Mutex
    stream          *client.Stream
    protocol        *client.BinaryClientProtocol
    rchannel       chan protocol.CommandDecode
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
}

func NewArbiterClient(member *ArbiterMember) *ArbiterClient {
    return &ArbiterClient{member, &sync.Mutex{}, nil, nil,
        make(chan protocol.CommandDecode, 8), false, make(chan bool, 1),nil}
}

func (self *ArbiterClient) Open(addr string) error {
    if self.protocol != nil {
        return errors.New("Client is Opened")
    }

    self.member.manager.slock.Log().Infof("Arbiter client connect %s", addr)
    conn, err := net.DialTimeout("tcp", addr, 2 * time.Second)
    if err != nil {
        self.member.manager.slock.Log().Warnf("Arbiter client connect %s error %v", addr, err)
        return err
    }
    stream := client.NewStream(conn)
    client_protocol := client.NewBinaryClientProtocol(stream)

    self.glock.Lock()
    self.stream = stream
    self.protocol = client_protocol
    err = self.HandleInit()
    if err != nil {
        _ = client_protocol.Close()
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
    self.stream = nil
    self.protocol = nil
    _ = self.WakeupRetryConnect()
    self.member.manager.slock.Log().Warnf("Arbiter client connect %s close", self.member.host)
    return nil
}

func (self *ArbiterClient) HandleInit() error {
    request := protobuf.ArbiterConnectRequest{FromHost:self.member.manager.own_member.host, ToHost:self.member.host}
    data, err := request.Marshal()
    if err != nil {
        return err
    }
    
    call_command := protocol.NewCallCommand("REPL_CONNECT", data)
    err = self.protocol.Write(call_command)
    if err != nil {
        return err
    }
    result_command, err := self.protocol.Read()
    if err != nil {
        return err
    }
    
    call_result_command, ok := result_command.(*protocol.CallResultCommand)
    if !ok {
        return errors.New("unkonwn command")
    }
    
    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        if call_result_command.ErrType == "ERR_NOT_MEMBER" {
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
            call_result_command.Result, call_result_command.ErrType)
        return errors.New("init error")
    }

    self.member.manager.slock.Log().Infof("Arbiter client connect %s init succed", self.member.host)
    return nil
}

func (self *ArbiterClient) Run() {
    _ = self.member.ClientOffline(self)
    for ; !self.closed; {
        if self.protocol == nil {
            err := self.Open(self.member.host)
            if err != nil {
                _ = self.SleepWhenRetryConnect()
                continue
            }
        }

        self.member.manager.slock.Log().Infof("Arbiter client connect %s connected", self.member.host)
        _ = self.member.ClientOnline(self)
        for ; !self.closed; {
            command, err := self.protocol.Read()
            if err != nil {
                self.rchannel <- nil
                if self.protocol != nil {
                    _ = self.protocol.Close()
                }
                self.protocol = nil
                self.stream = nil
                _ = self.member.ClientOffline(self)
                break
            }
            self.rchannel <- command
        }
    }

    close(self.rchannel)
    close(self.closed_waiter)
    self.protocol = nil
    self.stream = nil
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
        result := <- rchannel
        if result == nil {
            self.glock.Unlock()
            return nil, errors.New("read command error")
        }

        call_result_command, ok := result.(*protocol.CallResultCommand)
        if !ok {
            self.glock.Unlock()
            return nil, errors.New("unknown command")
        }

        if call_result_command.RequestId != command.RequestId {
            continue
        }
        self.glock.Unlock()
        return call_result_command, nil
    }
}

func (self *ArbiterClient) SleepWhenRetryConnect() error {
    self.member.glock.Lock()
    self.wakeup_signal = make(chan bool, 1)
    self.member.glock.Unlock()

    select {
    case <- self.wakeup_signal:
        return nil
    case <- time.After(5 * time.Second):
        self.member.glock.Lock()
        self.wakeup_signal = nil
        self.member.glock.Unlock()
        return nil
    }
}

func (self *ArbiterClient) WakeupRetryConnect() error {
    self.member.glock.Lock()
    if self.wakeup_signal != nil {
        close(self.wakeup_signal)
        self.wakeup_signal = nil
    }
    self.member.glock.Unlock()
    return nil
}

type ArbiterServer struct {
    member          *ArbiterMember
    stream          *Stream
    protocol        *BinaryServerProtocol
    closed          bool
    closed_waiter   chan bool
}

func NewArbiterServer(protocol *BinaryServerProtocol) *ArbiterServer {
    return &ArbiterServer{nil, protocol.stream, protocol, false, make(chan bool, 1)}
}

func (self *ArbiterServer) Close() error {
    self.closed = true
    if self.protocol != nil {
        _ = self.protocol.Close()
    }
    self.stream = nil
    self.protocol = nil
    if self.member != nil {
        self.member.manager.slock.Log().Infof("Arbiter server connect from %s close", self.member.host)
    }
    return nil
}

func (self *ArbiterServer) HandleInit(manager *ArbiterManager, request *protobuf.ArbiterConnectRequest) (*protobuf.ArbiterConnectResponse, error) {
    if manager.own_member != nil {
        if manager.own_member.host != request.ToHost {
            return nil, io.EOF
        }
    }

    err := self.Attach(manager, request.FromHost)
    if err != nil {
        return &protobuf.ArbiterConnectResponse{ErrMessage:"unknown member"}, nil
    }
    return &protobuf.ArbiterConnectResponse{ErrMessage:""}, nil
}

func (self *ArbiterServer) Attach(manager *ArbiterManager, from_host string) error {
    var current_member *ArbiterMember = nil
    for _, member := range manager.members {
        if member.host == from_host {
            current_member = member
            break
        }
    }

    if current_member == nil {
        return errors.New("unknown member")
    }

    if current_member.server != nil {
        if current_member.server.protocol == self.protocol {
            return nil
        }

        server := current_member.server
        if !server.closed {
            _ = server.Close()
        }
    }
    current_member.server = self
    go self.Run()
    self.member = current_member
    if current_member.client != nil {
        _ = current_member.client.WakeupRetryConnect()
    }
    _ = manager.voter.WakeupRetryVote()
    self.member.manager.slock.Log().Infof("Arbiter server accept client %s connected", current_member.host)
    return nil
}

func (self *ArbiterServer) Run() {
    if !self.stream.closed {
        <- self.stream.closed_waiter
    }
    self.protocol = nil
    self.stream = nil
    self.closed = true
    close(self.closed_waiter)
    if self.member != nil {
        self.member.manager.slock.Log().Infof("Arbiter server connect from %s closed", self.member.host)
        self.member.server = nil
    }
}

type ArbiterMember struct {
    manager         *ArbiterManager
    glock           *sync.Mutex
    client          *ArbiterClient
    server          *ArbiterServer
    host            string
    weight          uint32
    arbiter         uint32
    role            uint8
    status          uint8
    last_updated    int64
    last_delay      int64
    last_error      int
    aof_id          [16]byte
    isself          bool
    abstianed       bool
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
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

    if !self.isself {
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
        closed_waiter := self.client.closed_waiter
        _ = self.client.Close()
        <- closed_waiter
        self.client = nil
    }
    
    if self.server != nil {
        closed_waiter := self.server.closed_waiter
        _ = self.server.Close()
        <- closed_waiter
        self.server = nil
    }
    self.Wakeup()
    self.manager.slock.Log().Infof("Arbiter member %s close", self.host)
    return nil
}

func (self *ArbiterMember) UpdateStatus() error {
    if self.isself {
        self.aof_id = self.manager.GetCurrentAofID()
        self.last_updated = time.Now().UnixNano()
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
    
    call_command := protocol.NewCallCommand("REPL_STATUS", data)
    call_result_command, err := self.client.Request(call_command)
    if err != nil {
        self.last_error++
        if self.last_error >= 5 {
            _ = self.client.protocol.Close()
            self.last_error = 0
        }
        return err
    }
    
    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        self.last_error++
        if self.last_error >= 5 {
            _ = self.client.protocol.Close()
            self.last_error = 0
        }
        self.manager.slock.Log().Warnf("Arbiter member %s update status error %v", self.host, err)
        return errors.New(fmt.Sprintf("call error %d %s", call_result_command.Result, call_result_command.ErrType))
    }
    
    response := protobuf.ArbiterStatusResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return err
    }

    self.aof_id = self.manager.DecodeAofId(response.AofId)
    self.role = uint8(response.Role)
    self.last_updated = time.Now().UnixNano()
    self.last_delay = self.last_updated - now
    self.last_error = 0
    return nil
}

func (self *ArbiterMember) ClientOnline(client  *ArbiterClient) error {
    if self.client != client {
        return nil
    }

    self.status = ARBITER_MEMBER_STATUS_ONLINE
    _ = self.manager.MemberStatusUpdated(self)
    _ = self.manager.voter.WakeupRetryVote()
    return nil
}

func (self *ArbiterMember) ClientOffline(client  *ArbiterClient) error {
    if self.client != client {
        return nil
    }
    
    self.status = ARBITER_MEMBER_STATUS_OFFLINE
    _ = self.manager.MemberStatusUpdated(self)
    return nil
}

func (self *ArbiterMember) Run() {
    if !self.isself {
        go func() {
            if self.client == nil {
                return
            }
            self.client.Run()
        }()
    } else {
        self.status = ARBITER_MEMBER_STATUS_ONLINE
        self.last_updated = time.Now().UnixNano()
    }

    for ; !self.closed; {
        self.glock.Lock()
        self.wakeup_signal = make(chan bool, 1)
        self.glock.Unlock()

        select {
        case <-self.wakeup_signal:
            continue
        case <-time.After(5 * time.Second):
            self.glock.Lock()
            self.wakeup_signal = nil
            self.glock.Unlock()
            if self.client != nil {
                _ = self.UpdateStatus()
            }
        }
    }
    close(self.closed_waiter)
    self.manager.slock.Log().Infof("Arbiter member %s closed", self.host)
}

func (self *ArbiterMember) Wakeup() {
    self.glock.Lock()
    if self.wakeup_signal != nil {
        close(self.wakeup_signal)
        self.wakeup_signal = nil
    }
    self.glock.Unlock()
}

func (self *ArbiterMember) DoVote() (*protobuf.ArbiterVoteResponse, error) {
    if self.isself {
        if self.abstianed {
            return nil, errors.New("stop vote")
        }

        aof_id := self.manager.EncodeAofId(self.manager.GetCurrentAofID())
        return &protobuf.ArbiterVoteResponse{ErrMessage:"", Host:self.host, Weight:self.weight,
            Arbiter:self.arbiter, AofId:aof_id, Role:uint32(self.role)}, nil
    }
    if self.status != ARBITER_MEMBER_STATUS_ONLINE {
        return nil, errors.New("not online")
    }

    request := protobuf.ArbiterVoteRequest{}
    data, err := request.Marshal()
    if err != nil {
        return nil, err
    }

    call_command := protocol.NewCallCommand("REPL_VOTE", data)
    call_result_command, err := self.client.Request(call_command)
    if err != nil {
        return nil, err
    }

    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        if call_result_command.ErrType == "ERR_UNINIT" && self.manager.own_member != nil {
            self.manager.DoAnnouncement()
        }
        return nil, errors.New(fmt.Sprintf("except code %d:%s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterVoteResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

    self.aof_id = self.manager.DecodeAofId(response.AofId)
    self.role = uint8(response.Role)
    self.manager.slock.Log().Infof("Arbiter member %s do vote succed", self.host)
    return &response, nil
}

func (self *ArbiterMember) DoProposal(proposal_id uint64, host string, aof_id [16]byte) (*protobuf.ArbiterProposalResponse, error) {
    if self.isself {
        return &protobuf.ArbiterProposalResponse{ErrMessage:""}, nil
    }
    if self.status != ARBITER_MEMBER_STATUS_ONLINE {
        return nil, errors.New("not online")
    }

    request := protobuf.ArbiterProposalRequest{ProposalId:proposal_id, AofId:self.manager.EncodeAofId(aof_id), Host:host}
    data, err := request.Marshal()
    if err != nil {
        return nil, err
    }

    call_command := protocol.NewCallCommand("REPL_PROPOSAL", data)
    call_result_command, err := self.client.Request(call_command)
    if err != nil {
        return nil, err
    }

    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        if call_result_command.ErrType == "ERR_PROPOSALID" {
            response := protobuf.ArbiterProposalResponse{}
            err = response.Unmarshal(call_result_command.Data)
            if err == nil {
                self.manager.voter.proposal_id = response.ProposalId
            }
        }
        return nil, errors.New(fmt.Sprintf("except code %d:%s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterProposalResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

    self.manager.slock.Log().Infof("Arbiter member %s do vote succed", self.host)
    return &response, nil
}

func (self *ArbiterMember) DoCommit(proposal_id uint64, host string, aof_id [16]byte) (*protobuf.ArbiterCommitResponse, error) {
    if self.isself {
        return &protobuf.ArbiterCommitResponse{ErrMessage:""}, nil
    }
    if self.status != ARBITER_MEMBER_STATUS_ONLINE {
        return nil, errors.New("not online")
    }

    request := protobuf.ArbiterCommitRequest{ProposalId:proposal_id, AofId:self.manager.EncodeAofId(aof_id), Host:host}
    data, err := request.Marshal()
    if err != nil {
        return nil, err
    }

    call_command := protocol.NewCallCommand("REPL_COMMIT", data)
    call_result_command, err := self.client.Request(call_command)
    if err != nil {
        return nil, err
    }

    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        return nil, errors.New(fmt.Sprintf("except code %d:%s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterCommitResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

    self.manager.slock.Log().Infof("Arbiter member %s do commit succed", self.host)
    return &response, nil
}


func (self *ArbiterMember) DoAnnouncement() (*protobuf.ArbiterAnnouncementResponse, error) {
    if self.isself {
        return &protobuf.ArbiterAnnouncementResponse{ErrMessage:""}, nil
    }
    
    if self.status != ARBITER_MEMBER_STATUS_ONLINE {
        return nil, errors.New("not online")
    }

    members := make([]*protobuf.ReplSetMember, 0)
    for _, member := range self.manager.members {
        rplm := &protobuf.ReplSetMember{Host:member.host, Weight:member.weight, Arbiter:member.arbiter, Role:uint32(member.role)}
        members = append(members, rplm)
    }

    replset := protobuf.ReplSet{Name:self.manager.name, Gid:self.manager.gid, Version:self.manager.version, Vertime:self.manager.vertime,
        Owner:self.host, Members:members, CommitId:self.manager.voter.commit_id}
    request := protobuf.ArbiterAnnouncementRequest{FromHost:self.manager.own_member.host, ToHost:self.host, Replset:&replset}
    data, err := request.Marshal()
    if err != nil {
        return nil, err
    }

    call_command := protocol.NewCallCommand("REPL_ANNOUNCEMENT", data)
    call_result_command, err := self.client.Request(call_command)
    if err != nil {
        return nil, err
    }

    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
        self.manager.slock.Log().Errorf("Arbiter member %s do announcement error %d:%s", self.host, call_result_command.Result, call_result_command.ErrType)
        return nil, errors.New(fmt.Sprintf("except code %d:%s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterAnnouncementResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

    self.manager.slock.Log().Infof("Arbiter member %s do announcement succed", self.host)
    return &response, nil
}

type ArbiterVoter struct {
    manager         *ArbiterManager
    glock           *sync.Mutex
    commit_id       uint64
    proposal_id     uint64
    proposal_host   string
    vote_host       string
    vote_aof_id     [16]byte
    voting          bool
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
}

func NewArbiterVoter() *ArbiterVoter {
    return &ArbiterVoter{nil, &sync.Mutex{}, 0, 0, "", "", [16]byte{},
        false, false, make(chan bool, 1), nil}
}

func (self *ArbiterVoter) Close() error {
    self.glock.Lock()
    self.closed = true
    if !self.voting {
        close(self.closed_waiter)
        self.glock.Unlock()
        return nil
    }

    if self.wakeup_signal != nil {
        close(self.wakeup_signal)
        self.wakeup_signal = nil
    }
    self.glock.Unlock()
    self.manager.slock.Log().Infof("Arbiter voter close")
    return nil
}

func (self *ArbiterVoter) StartVote() error {
    self.glock.Lock()
    if self.closed || self.voting {
        if self.wakeup_signal != nil {
            close(self.wakeup_signal)
            self.wakeup_signal = nil
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
                close(self.closed_waiter)
            }
            self.glock.Unlock()
            self.manager.slock.Log().Infof("Arbiter voter election finish")
        }()

        for ; !self.closed; {
            if self.manager.own_member == nil || len(self.manager.members) == 0 {
                return
            }

            if self.manager.leader_member != nil {
                if self.manager.leader_member.status == ARBITER_MEMBER_STATUS_ONLINE {
                    self.manager.slock.Log().Infof("Arbier voter election finish, current leader %s", self.manager.leader_member.host)
                    return
                }
            }

            online_count := 0
            for _, member := range self.manager.members {
                if member.status == ARBITER_MEMBER_STATUS_ONLINE {
                    if member.host == self.proposal_host && self.manager.own_member.host != self.proposal_host {
                        self.manager.slock.Log().Infof("Arbier voter wait announcement, current leader %s", self.proposal_host)
                        return
                    }
                    online_count++
                }
            }

            if online_count < len(self.manager.members) / 2 + 1 {
                self.manager.slock.Log().Infof("Arbier voter online member not enough, total %d online %d", len(self.manager.members), online_count)
                _ = self.SleepWhenRetryVote()
                continue
            }

            err := self.DoVote()
            if err == nil {
                err = self.DoProposal()
                if err == nil {
                    err = self.DoCommit()
                    if err == nil {
                        _ = self.manager.VoteSucced()
                        return
                    }
                }
            }

            _ = self.SleepWhenRetryVote()
        }
        return
    }()
    return nil
}

func (self *ArbiterVoter) DoRequests(name string, handler func(*ArbiterMember) (interface{}, error)) []interface{} {
    members, finish_count := self.manager.members, 0
    responses, request_waiter := make([]interface{}, 0), make(chan bool, 1)
    for _, member := range members {
        go func(member *ArbiterMember) {
            response, err := handler(member)
            if err == nil {
                responses = append(responses, response)
            } else {
                self.manager.slock.Log().Errorf("Arbier voter member %s request %s error %v", member.host, name, err)
            }

            self.glock.Lock()
            finish_count++
            if finish_count >= len(members) {
                close(request_waiter)
            }
            self.glock.Unlock()
        }(member)
    }

    if len(members) > 0 {
        <- request_waiter
    }
    return responses
}

func (self *ArbiterVoter) DoVote() error {
    self.vote_host, self.vote_aof_id = "", [16]byte{}
    responses := self.DoRequests("do vote", func(member *ArbiterMember) (interface{}, error) {
        return member.DoVote()
    })

    if len(responses) < len(self.manager.members) / 2 + 1 {
        return errors.New("vote error")
    }

    var select_vote_response *protobuf.ArbiterVoteResponse = nil
    for _, response := range responses {
        vote_response := response.(*protobuf.ArbiterVoteResponse)
        if vote_response.Arbiter != 0 || vote_response.Weight == 0 {
            continue
        }

        if select_vote_response == nil {
            select_vote_response = vote_response
            continue
        }

        if select_vote_response.AofId == vote_response.AofId {
            if select_vote_response.Weight < vote_response.Weight {
                select_vote_response = vote_response
            }
            if select_vote_response.Weight == vote_response.Weight {
                if select_vote_response.Host < vote_response.Host {
                    select_vote_response = vote_response
                }
            }
            continue
        }

        if self.manager.CompareAofId(self.manager.DecodeAofId(vote_response.AofId), self.manager.DecodeAofId(select_vote_response.AofId)) > 0 {
            select_vote_response = vote_response
        }
    }

    if select_vote_response == nil {
        return errors.New("not found")
    }

    self.vote_host = select_vote_response.Host
    self.vote_aof_id = self.manager.DecodeAofId(select_vote_response.AofId)
    self.manager.slock.Log().Infof("Arbier voter do vote succed,  host %s aof_id %x proposal_id %d", self.vote_host, self.vote_aof_id, self.proposal_id)
    return nil
}

func (self *ArbiterVoter) DoProposal() error {
    self.proposal_id++
    responses := self.DoRequests("do proposal", func(member *ArbiterMember) (interface{}, error) {
        return member.DoProposal(self.proposal_id, self.vote_host, self.vote_aof_id)
    })
    
    if len(responses) < len(self.manager.members) / 2 + 1 {
        return errors.New("member accept proposal count too small")
    }
    self.manager.slock.Log().Infof("Arbier voter do proposal succed, host %s aof_id %x proposal_id %d", self.vote_host, self.vote_aof_id, self.proposal_id)
    return nil
}

func (self *ArbiterVoter) DoCommit() error {
    self.proposal_host = self.vote_host
    self.commit_id = self.proposal_id

    responses := self.DoRequests("do commit", func(member *ArbiterMember) (interface{}, error) {
        return member.DoCommit(self.proposal_id, self.proposal_host, self.vote_aof_id)
    })

    if len(responses) < len(self.manager.members) / 2 + 1 {
        self.proposal_host = ""
        return errors.New("member accept proposal count too small")
    }

    self.manager.slock.Log().Infof("Arbier voter do commit succed, host %s aof_id %x commit_id %d", self.vote_host, self.vote_aof_id, self.commit_id)
    return nil
}

func (self *ArbiterVoter) DoAnnouncement() error {
    self.manager.slock.Log().Infof("Arbiter replication do announcement start")
    for _, member := range self.manager.members {
        if member.role == ARBITER_ROLE_LEADER {
            _, err := member.DoAnnouncement()
            if err != nil {
                if member.host == self.proposal_host && !member.isself && self.voting {
                    self.proposal_host = ""
                    _ = self.manager.StartVote()
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

func (self *ArbiterVoter) SleepWhenRetryVote() error {
    self.glock.Lock()
    self.wakeup_signal = make(chan bool, 1)
    self.glock.Unlock()

    delay_time := int64(5000)
    n, err := rand.Int(rand.Reader, big.NewInt(5000))
    if err == nil {
        delay_time = 100 + n.Int64()
    }

    select {
    case <- self.wakeup_signal:
        return nil
    case <- time.After(time.Duration(delay_time) * time.Millisecond):
        self.glock.Lock()
        self.wakeup_signal = nil
        self.glock.Unlock()
        return nil
    }
}

func (self *ArbiterVoter) WakeupRetryVote() error {
    self.glock.Lock()
    if self.wakeup_signal != nil {
        close(self.wakeup_signal)
        self.wakeup_signal = nil
    }
    self.glock.Unlock()
    return nil
}

type ArbiterManager struct {
    slock           *SLock
    glock           *sync.Mutex
    store           *ArbiterStore
    voter           *ArbiterVoter
    members         []*ArbiterMember
    own_member      *ArbiterMember
    leader_member   *ArbiterMember
    name            string
    gid             string
    version         uint32
    vertime         uint64
    stoped          bool
    loaded          bool
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

    aof_id, err := self.slock.aof.LoadMaxId()
    if err != nil {
        self.slock.Log().Errorf("Arbiter load aof file maxid error %v", err)
    } else {
        self.slock.Log().Infof("Arbiter load aof file maxid %x", aof_id)
    }
    self.slock.replication_manager.current_request_id = aof_id
    self.voter.proposal_id = self.voter.commit_id
    return nil
}

func (self *ArbiterManager) Start() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.own_member == nil || len(self.members) == 0 {
        return nil
    }

    for _, member := range self.members {
        _ = member.Open()
        go member.Run()
    }

    self.slock.UpdateState(STATE_VOTE)
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

    if self.own_member == nil || len(self.members) == 0 {
        self.stoped = true
        self.slock.logger.Infof("Arbiter closed")
        return nil
    }

    self.glock.Lock()
    if self.own_member.role == ARBITER_ROLE_LEADER && len(self.members) > 1 {
        self.own_member.abstianed = true
        _ = self.QuitLeader()
    } else {
        _ = self.store.Save(self)
    }
    self.stoped = true
    self.glock.Unlock()

    _ = self.voter.Close()
    <- self.voter.closed_waiter
    for _, member := range self.members {
        _ = member.Close()
        <- member.closed_waiter
    }
    self.slock.logger.Infof("Arbiter closed")
    return nil
}

func (self *ArbiterManager) Config(host string, weight uint32, arbiter uint32) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.own_member != nil || len(self.members) != 0 {
        return errors.New("member info error")
    }

    member := NewArbiterMember(self, host, weight, arbiter)
    member.isself = true
    err := member.Open()
    if err != nil {
        return err
    }
    
    self.own_member = member
    self.members = append(self.members, member)
    go member.Run()
    self.slock.UpdateState(STATE_VOTE)
    self.gid = self.EncodeAofId(protocol.GenRequestId())
    self.version++
    self.vertime = uint64(time.Now().UnixNano()) / 1e6
    self.voter.proposal_id = uint64(self.version)
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

    if self.own_member == nil || len(self.members) == 0 {
        return errors.New("member info error")
    }

    for _, member := range self.members {
        if member.host == host {
            return errors.New("has save member error")
        }
    }

    member := NewArbiterMember(self, host, weight, arbiter)
    member.role = ARBITER_ROLE_FOLLOWER
    err := member.Open()
    if err != nil {
        return err
    }

    self.members = append(self.members, member)
    go member.Run()
    self.version++
    self.vertime = uint64(time.Now().UnixNano()) / 1e6
    _ = self.store.Save(self)
    self.DoAnnouncement()
    _ = self.UpdateStatus()
    self.slock.logger.Infof("Arbiter add member host %s weight %d arbiter %d", host, weight, arbiter)
    return nil
}

func (self *ArbiterManager) RemoveMember(host string) error {
    self.glock.Lock()
    if self.own_member == nil || len(self.members) == 0 {
        self.glock.Unlock()
        return errors.New("member info error")
    }

    if self.own_member.host == host {
        self.glock.Unlock()
        return errors.New("can not remove self error")
    }

    var current_member *ArbiterMember = nil
    members := make([]*ArbiterMember, 0)
    for _, member := range self.members {
        if member.host == host {
            current_member = member
        } else {
            members = append(members, member)
        }
    }

    if current_member == nil {
        self.glock.Unlock()
        return errors.New("not found member")
    }

    self.members = members
    self.version++
    self.vertime = uint64(time.Now().UnixNano()) / 1e6
    _ = self.store.Save(self)
    self.DoAnnouncement()
    _ = self.UpdateStatus()
    self.glock.Unlock()

    _, _ = current_member.DoAnnouncement()
    _ = current_member.Close()
    <- current_member.closed_waiter
    self.slock.logger.Infof("Arbiter remove member host %s", host)
    return nil
}

func (self *ArbiterManager) UpdateMember(host string, weight uint32, arbiter uint32) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.own_member == nil || len(self.members) == 0 {
        return errors.New("member info error")
    }

    var current_member *ArbiterMember = nil
    members := make([]*ArbiterMember, 0)
    for _, member := range self.members {
        if member.host == host {
            current_member = member
        } else {
            members = append(members, member)
        }
    }

    if current_member == nil {
        return errors.New("not found member")
    }
    current_member.weight = weight
    current_member.arbiter = arbiter
    self.version++
    self.vertime = uint64(time.Now().UnixNano()) / 1e6
    _ = self.store.Save(self)
    if self.own_member.role == ARBITER_ROLE_LEADER && self.own_member.weight == 0 {
        _ = self.QuitLeader()
    } else {
        self.DoAnnouncement()
        _ = self.UpdateStatus()
    }
    self.slock.logger.Infof("Arbiter update member host %s weight %d arbiter %d", host, weight, arbiter)
    return nil
}

func (self *ArbiterManager) GetMembers() []*ArbiterMember {
    return self.members
}

func (self *ArbiterManager) QuitLeader() error {
    self.slock.Log().Infof("Arbiter quit leader start")
    _ = self.slock.replication_manager.SwitchToFollower("")
    self.own_member.role = ARBITER_ROLE_FOLLOWER
    self.leader_member = nil
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
    _ = self.slock.replication_manager.SwitchToFollower("")

    self.glock.Lock()
    members := self.members
    self.members = make([]*ArbiterMember, 0)
    self.own_member = nil
    self.leader_member = nil
    self.voter.proposal_id = 0
    self.voter.proposal_host = ""
    self.voter.commit_id = 0
    self.version = 1
    self.vertime = 0
    _ = self.store.Save(self)
    self.glock.Unlock()

    for _, member := range members {
        _ = member.Close()
        <- member.closed_waiter
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

func (self *ArbiterManager) VoteSucced() error {
    if self.stoped {
        return nil
    }

    self.slock.Log().Infof("Arbiter election succed, current leader %s", self.voter.proposal_host)
    self.glock.Lock()
    proposal_host := self.voter.proposal_host
    for _, member := range self.members {
        if member.host == self.voter.proposal_host {
            member.role = ARBITER_ROLE_LEADER
            self.leader_member = member
            if member.host == self.own_member.host {
                self.voter.proposal_host = ""
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
    if proposal_host == self.own_member.host {
        if self.slock.state == STATE_LEADER {
            self.DoAnnouncement()
        } else {
            _ = self.UpdateStatus()
        }
    } else {
        if self.slock.state == STATE_LEADER {
            err := self.slock.replication_manager.SwitchToFollower("")
            if err != nil {
                self.slock.Log().Errorf("Arbiter election succed change to follower error %v", err)
            }
            err = self.slock.replication_manager.transparency_manager.ChangeLeader("")
            if err != nil {
                self.slock.Log().Errorf("Arbiter election succed change transparency address error %v", err)
            }
        }
        self.glock.Unlock()
        _ = self.voter.DoAnnouncement()
        self.glock.Lock()
        _ = self.UpdateStatus()
    }

    self.slock.Log().Infof("Arbiter election succed change status finish")
    self.glock.Unlock()
    return nil
}


func (self *ArbiterManager) UpdateStatus() error {
    if self.stoped {
        return nil
    }

    for _, member := range self.members {
        if member.isself {
            self.own_member = member
        }
        if member.role == ARBITER_ROLE_LEADER {
            self.leader_member = member
        }
    }

    if self.own_member == nil {
        return nil
    }

    if self.leader_member == nil {
        err := self.slock.replication_manager.SwitchToFollower("")
        if err != nil {
            self.slock.Log().Errorf("Arbiter update status reset follower error %v", err)
        }
        err = self.slock.replication_manager.transparency_manager.ChangeLeader("")
        if err != nil {
            self.slock.Log().Errorf("Arbiter update status reset transparency address error %v", err)
        }
        _ = self.StartVote()
        return nil
    }

    if self.leader_member.host == self.own_member.host {
        if self.slock.state != STATE_LEADER {
            if self.own_member.arbiter == 0 {
                if !self.loaded {
                    err := self.slock.InitLeader()
                    if err != nil {
                        self.slock.Log().Errorf("Arbiter update status init leader error %v", err)
                    }
                    self.slock.StartLeader()
                    self.loaded = true
                    self.DoAnnouncement()
                    return nil
                }
                err := self.slock.replication_manager.SwitchToLeader()
                if err != nil {
                    self.slock.Log().Errorf("Arbiter update status change leader error %v", err)
                }
                self.DoAnnouncement()
            } else {
                _ = self.QuitLeader()
            }
        }
        return nil
    }

    if self.own_member.arbiter == 0 {
        if !self.loaded {
            err := self.slock.InitFollower(self.leader_member.host)
            if err != nil {
                self.slock.Log().Errorf("Arbiter update status init follower error %v", err)
            }
            self.slock.StartFollower()
            err = self.slock.replication_manager.transparency_manager.ChangeLeader(self.leader_member.host)
            if err != nil {
                self.slock.Log().Errorf("Arbiter update status change transparency address error %v", err)
            }
            self.loaded = true
            return nil
        }

        err := self.slock.replication_manager.SwitchToFollower(self.leader_member.host)
        if err != nil {
            self.slock.Log().Errorf("Arbiter update status change follower error %v", err)
        }
        err = self.slock.replication_manager.transparency_manager.ChangeLeader(self.leader_member.host)
        if err != nil {
            self.slock.Log().Errorf("Arbiter update status change transparency address error %v", err)
        }
        return nil
    }

    err := self.slock.replication_manager.SwitchToFollower("")
    if err != nil {
        self.slock.Log().Errorf("Arbiter update status reset follower error %v", err)
    }
    err = self.slock.replication_manager.transparency_manager.ChangeLeader(self.leader_member.host)
    if err != nil {
        self.slock.Log().Errorf("Arbiter update status reset transparency address error %v", err)
    }
    return nil
}

func (self *ArbiterManager) MemberStatusUpdated(member *ArbiterMember) error {
    if self.own_member == nil || self.stoped {
        return nil
    }

    if member.host == self.voter.proposal_host && member.status != ARBITER_MEMBER_STATUS_ONLINE {
        self.voter.proposal_host = ""
        err := self.StartVote()
        if err != nil {
            self.slock.Log().Errorf("Arbiter proposal host offline restart election error %v", err)
            return nil
        }
        self.slock.Log().Infof("Arbiter proposal host offline restart election")
        return nil
    }

    if self.voter.proposal_host == "" && self.leader_member == nil {
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

    if self.own_member.role == ARBITER_ROLE_LEADER {
        if member.status == ARBITER_MEMBER_STATUS_ONLINE {
            self.DoAnnouncement()
            return nil
        }

        online_count := 0
        for _, member := range self.members {
            if member.status == ARBITER_MEMBER_STATUS_ONLINE {
                online_count++
            }
        }

        if online_count < len(self.members) / 2 + 1 {
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
    if self.own_member == nil {
        return [16]byte{}
    }

    if self.own_member.arbiter != 0 {
        aof_id := [16]byte{}
        for _, member := range self.members {
            if member.role != ARBITER_ROLE_ARBITER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
                if self.CompareAofId(member.aof_id, aof_id) > 0 {
                    aof_id = member.aof_id
                }
            }
        }
        self.own_member.aof_id = aof_id
        return aof_id
    }
    return self.slock.replication_manager.GetCurrentAofID()
}

func (self *ArbiterManager) EncodeAofId(aof_id [16]byte) string {
    return fmt.Sprintf("%x", aof_id)
}

func (self *ArbiterManager) DecodeAofId(aof_id string) [16]byte {
    buf, err := hex.DecodeString(aof_id)
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
    acommand_time := uint64(a[8]) | uint64(a[9])<<8 | uint64(a[10])<<16 | uint64(a[11])<<24 | uint64(a[12])<<32 | uint64(a[13])<<40 | uint64(a[14])<<48 | uint64(a[15])<<56

    bid := uint64(b[0])<<32 | uint64(b[1])<<40 | uint64(b[2])<<48 | uint64(b[3])<<56 | uint64(b[4]) | uint64(b[5])<<8 | uint64(b[6])<<16 | uint64(b[7])<<24
    bcommand_time := uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 | uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56

    if aid > bid {
        return 1
    }
    if aid < bid {
        return -1
    }
    if acommand_time > bcommand_time {
        return 1
    }
    return -1
}

func (self *ArbiterManager) GetCallMethods() map[string]BinaryServerProtocolCallHandler{
    handlers := make(map[string]BinaryServerProtocolCallHandler, 2)
    handlers["REPL_CONNECT"] = self.CommandHandleConnectCommand
    handlers["REPL_VOTE"] = self.CommandHandleVoteCommand
    handlers["REPL_PROPOSAL"] = self.CommandHandleProposalCommand
    handlers["REPL_COMMIT"] = self.CommandHandleCommitCommand
    handlers["REPL_ANNOUNCEMENT"] = self.CommandHandleAnnouncementCommand
    handlers["REPL_STATUS"] = self.CommandHandleStatusCommand
    return handlers
}

func (self *ArbiterManager) CommandHandleConnectCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    if self.stoped {
        return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
    }

    request := protobuf.ArbiterConnectRequest{}
    err := request.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member != nil && len(self.members) > 0 {
        var current_member *ArbiterMember = nil
        for _, member := range self.members {
            if member.host == request.FromHost {
                current_member = member
                break
            }
        }

        if current_member == nil {
            return protocol.NewCallResultCommand(command, 0, "ERR_NOT_MEMBER", nil), nil
        }
    }

    server := NewArbiterServer(server_protocol)
    response, err := server.HandleInit(self, &request)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_CALL", nil), nil
    }
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    server_protocol.stream.stream_type = STREAM_TYPE_ARBITER
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleVoteCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    if self.stoped {
        return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
    }

    request := protobuf.ArbiterVoteRequest{}
    err := request.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
    }

    if self.own_member.abstianed {
        return protocol.NewCallResultCommand(command, 0, "ERR_ABSTIANED", nil), nil
    }

    response := protobuf.ArbiterVoteResponse{ErrMessage:"", Host:self.own_member.host, Weight:self.own_member.weight,
        Arbiter:self.own_member.arbiter, AofId:self.EncodeAofId(self.GetCurrentAofID()), Role:uint32(self.own_member.role)}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleProposalCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    if self.stoped {
        return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
    }

    request := protobuf.ArbiterProposalRequest{}
    err := request.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
    }

    if self.own_member.role == ARBITER_ROLE_LEADER {
        self.DoAnnouncement()
        return protocol.NewCallResultCommand(command, 0, "ERR_ROLE", nil), nil
    }

    var vote_member *ArbiterMember = nil
    for _, member := range self.members {
        if member.role == ARBITER_ROLE_LEADER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
            self.DoAnnouncement()
            return protocol.NewCallResultCommand(command, 0, "ERR_STATUS", nil), nil
        }

        if member.host == request.Host {
            vote_member = member
        }

        if self.CompareAofId(member.aof_id, self.DecodeAofId(request.AofId)) > 0 {
            return protocol.NewCallResultCommand(command, 0, "ERR_AOFID", nil), nil
        }
    }

    if vote_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
    }

    if vote_member.status != ARBITER_MEMBER_STATUS_ONLINE {
        return protocol.NewCallResultCommand(command, 0, "ERR_OFFLINE", nil), nil
    }

    if self.voter.proposal_id >= request.ProposalId || self.voter.proposal_host != "" {
        response := protobuf.ArbiterProposalResponse{ErrMessage:"", ProposalId:self.voter.proposal_id}
        data, err := response.Marshal()
        if err != nil {
            return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
        }
        return protocol.NewCallResultCommand(command, 0, "ERR_PROPOSALID", data), nil
    }

    response := protobuf.ArbiterProposalResponse{ErrMessage:"", ProposalId:self.voter.proposal_id}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }

    self.voter.proposal_id = request.ProposalId
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleCommitCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    if self.stoped {
        return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
    }

    request := protobuf.ArbiterCommitRequest{}
    err := request.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
    }

    var vote_member *ArbiterMember = nil
    for _, member := range self.members {
        if member.host == request.Host {
            vote_member = member
        }
    }

    if vote_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
    }

    if self.voter.commit_id >= request.ProposalId {
        response := protobuf.ArbiterCommitResponse{ErrMessage:"", CommitId:self.voter.commit_id}
        data, err := response.Marshal()
        if err != nil {
            return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
        }
        return protocol.NewCallResultCommand(command, 0, "ERR_COMMITID", data), nil
    }

    response := protobuf.ArbiterCommitResponse{ErrMessage:"", CommitId:self.voter.commit_id}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    self.voter.proposal_host = request.Host
    self.voter.commit_id = request.ProposalId
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleAnnouncementCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
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

    if self.own_member != nil && len(self.members) > 0 && request.Replset.Gid != self.gid {
        return protocol.NewCallResultCommand(command, 0, "ERR_GID", nil), nil
    }

    self.glock.Lock()
    if request.Replset.CommitId == self.voter.commit_id && self.voter.proposal_host != "" {
        if self.version < request.Replset.Version {
            self.version = request.Replset.Version
        }
        if self.vertime < request.Replset.Vertime {
            self.vertime = request.Replset.Vertime
        }
    } else {
        if request.Replset.Version < self.version || (request.Replset.Version == self.version && request.Replset.Vertime < self.vertime) {
            self.slock.Log().Infof("Arbiter handle announcement version waring CommitId %d %d Version %d %d Vertime %d %d", request.Replset.CommitId,
                self.voter.commit_id, request.Replset.Version, self.version, request.Replset.Vertime, self.vertime)
            self.glock.Unlock()
            return protocol.NewCallResultCommand(command, 0, "ERR_VERSION", nil), nil
        }
    }

    members, new_members, unopen_members := make(map[string]*ArbiterMember, 4), make([]*ArbiterMember, 0), make([]*ArbiterMember, 0)
    for _, member := range self.members {
        members[member.host] = member
    }

    var own_member, leader_member *ArbiterMember = nil, nil
    for _, rplm := range request.Replset.Members {
        if member, ok := members[rplm.Host]; ok {
            member.weight = rplm.Weight
            member.arbiter = rplm.Arbiter
            if rplm.Role != ARBITER_ROLE_UNKNOWN {
                member.role = uint8(rplm.Role)
            }
            delete(members, rplm.Host)
            if member.host == request.Replset.Owner {
                own_member = member
                own_member.isself = true
            }
            if member.role == ARBITER_ROLE_LEADER {
                leader_member = member
            }
            new_members = append(new_members, member)
        } else {
            member = NewArbiterMember(self, rplm.Host, rplm.Weight, rplm.Arbiter)
            member.role = uint8(rplm.Role)
            if member.host == request.Replset.Owner {
                own_member = member
                own_member.isself = true
            }
            if member.role == ARBITER_ROLE_LEADER {
                leader_member = member
            }
            new_members = append(new_members, member)
            unopen_members = append(unopen_members, member)
        }
    }

    if own_member == nil {
        go func() {
            self.glock.Lock()
            if self.own_member.role == ARBITER_ROLE_LEADER {
                _ = self.QuitLeader()
            }
            self.glock.Unlock()
            _ = self.QuitMember()
        }()
        self.glock.Unlock()
        return protocol.NewCallResultCommand(command, 0, "", nil), nil
    }

    leader_host := ""
    if leader_member != nil {
        if request.Replset.CommitId != self.voter.commit_id || self.voter.proposal_host == "" {
            if own_member.role == ARBITER_ROLE_LEADER && leader_member.host != own_member.host {
                self.glock.Unlock()
                return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
            }

            if self.leader_member != nil {
                if self.leader_member.status == ARBITER_MEMBER_STATUS_ONLINE && leader_member.host != self.leader_member.host {
                    self.glock.Unlock()
                    return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
                }
            }
        }

        leader_host = leader_member.host
        if request.Replset.CommitId >= self.voter.commit_id && self.voter.proposal_host != "" {
            self.slock.Log().Infof("Arbiter handle announcement accept leader %s", self.voter.proposal_host)
            self.voter.proposal_host = ""
        }
    }

    self.members = new_members
    self.own_member = own_member
    self.leader_member = leader_member
    self.gid = request.Replset.Gid
    self.version = request.Replset.Version
    self.vertime = request.Replset.Vertime
    _ = self.store.Save(self)
    if self.own_member.role == ARBITER_ROLE_LEADER {
        _ = self.UpdateStatus()
    }
    self.glock.Unlock()
    self.slock.Log().Infof("Arbiter handle announcement from %s leader %s member count %d version %d commitid %d",
        request.FromHost, leader_host, len(new_members), request.Replset.Version, request.Replset.CommitId)

    go func() {
        for _, member := range unopen_members {
            _ = member.Open()
            go member.Run()
        }
        for _, member := range members {
            _ = member.Close()
        }

        if self.own_member.role != ARBITER_ROLE_LEADER {
            self.glock.Lock()
            err := self.UpdateStatus()
            if err != nil {
                self.slock.Log().Errorf("Arbiter handle announcement update status error %v", err)
            }
            self.glock.Unlock()
        }
        self.slock.Log().Infof("Arbiter handle announcementcommand update status succed")
    }()

    for _, member := range self.members {
        if member.host == request.FromHost && member.server == nil {
            server := NewArbiterServer(server_protocol)
            _ = server.Attach(self, request.FromHost)
            break
        }
    }

    response := protobuf.ArbiterAnnouncementResponse{ErrMessage:""}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleStatusCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    if self.stoped {
        return protocol.NewCallResultCommand(command, 0, "ERR_STOPED", nil), nil
    }

    request := protobuf.ArbiterStatusRequest{}
    err := request.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
    }

    aof_id := self.EncodeAofId(self.GetCurrentAofID())
    response := protobuf.ArbiterStatusResponse{ErrMessage:"", AofId:aof_id, Role:uint32(self.own_member.role)}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}