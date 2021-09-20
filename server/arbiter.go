package server

import (
    "encoding/hex"
    "errors"
    "fmt"
    "github.com/snower/slock/client"
    "github.com/snower/slock/protocol"
    "github.com/snower/slock/server/protobuf"
    "io"
    "io/ioutil"
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
        return err
    }

    if _, err := os.Stat(data_dir); os.IsNotExist(err) {
        return err
    }
    
    self.filename = filepath.Join(data_dir, "meta.pb")
    manager.slock.Log().Infof("Arbiter Meta File %s", self.filename)
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
        return err
    }
    data, err := ioutil.ReadAll(file)
    if err != nil {
        file.Close()
        return err
    }
    data, err = self.ReadHeader(data)
    if err != nil {
        file.Close()
        return err
    }

    replset := protobuf.ReplSet{}
    err = replset.Unmarshal(data)
    if err != nil {
        file.Close()
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
        file.Close()
        return errors.New("unknown own member info")
    }
    manager.members = members
    file.Close()
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
        return err
    }

    err = self.WriteHeader(file)
    if err != nil {
        file.Close()
        return err
    }

    members := make([]*protobuf.ReplSetMember, 0)
    for _, member := range manager.members {
        rplm := &protobuf.ReplSetMember{Host:member.host, Weight:member.weight, Arbiter:member.arbiter, Role:uint32(member.role)}
        members = append(members, rplm)
    }

    replset := protobuf.ReplSet{Name:manager.name, Version:manager.version, Owner:manager.own_member.host, Members:members}
    data, err := replset.Marshal()
    if err != nil {
        file.Close()
        return err
    }

    n, werr := file.Write(data)
    if werr != nil {
        file.Close()
        return werr
    }
    if n != len(data) {
        file.Close()
        return errors.New("write data error")
    }
    file.Close()
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
    rcommand        chan protocol.CommandDecode
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
}

func NewArbiterClient(member *ArbiterMember) *ArbiterClient {
    return &ArbiterClient{member, &sync.Mutex{}, nil, nil,
        nil, false, make(chan bool, 1),nil}
}

func (self *ArbiterClient) Open(addr string) error {
    if self.protocol != nil {
        return errors.New("Client is Opened")
    }

    self.member.manager.slock.Log().Infof("Arbiter Client Connect %s", addr)
    conn, err := net.DialTimeout("tcp", addr, 2 * time.Second)
    if err != nil {
        return err
    }
    stream := client.NewStream(conn)
    client_protocol := client.NewBinaryClientProtocol(stream)
    self.stream = stream
    self.protocol = client_protocol

    err = self.HandleInit()
    if err != nil {
        if self.protocol != nil {
            self.protocol.Close()
        }
        self.protocol = nil
        self.stream = nil
        return err
    }
    return nil
}

func (self *ArbiterClient) Close() error {
    self.closed = true
    if self.protocol != nil {
        self.protocol.Close()
    }
    self.stream = nil
    self.protocol = nil
    if self.wakeup_signal != nil {
        self.wakeup_signal <- true
    }
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
        return errors.New("init error")
    }
    return nil
}

func (self *ArbiterClient) Run() {
    self.member.ClientOffline(self)
    for ; !self.closed; {
        if self.protocol == nil {
            err := self.Open(self.member.host)
            if err != nil {
                self.SleepWhenRetryConnect()
                continue
            }
        }

        self.member.manager.slock.Log().Infof("Arbiter Client Connected %s", self.member.host)
        self.member.ClientOnline(self)
        for ; !self.closed; {
            command, err := self.protocol.Read()
            if self.rcommand != nil {
                self.rcommand <- command
            }

            if err != nil {
                if self.protocol != nil {
                    self.protocol.Close()
                }
                self.protocol = nil
                self.stream = nil
                self.member.ClientOffline(self)
                break
            }
        }
    }

    if self.rcommand != nil {
        self.rcommand <- nil
    }
    self.closed_waiter <- true
    self.protocol = nil
    self.stream = nil
    self.member.manager.slock.Log().Infof("Arbiter Client Close %s", self.member.host)
}

func (self *ArbiterClient) Request(command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.closed {
        return nil, errors.New("client closed")
    }

    if self.protocol == nil {
        return nil, errors.New("client unconnecd")
    }

    err := self.protocol.Write(command)
    if err != nil {
        return nil, err
    }

    self.rcommand = make(chan protocol.CommandDecode, 1)
    result := <- self.rcommand
    self.rcommand = nil
    if result == nil {
        return nil, errors.New("read command error")
    }

    call_result_command, ok := result.(*protocol.CallResultCommand)
    if ok {
        return call_result_command, nil
    }
    return nil, errors.New("unknown command")
}

func (self *ArbiterClient) SleepWhenRetryConnect() error {
    self.wakeup_signal = make(chan bool, 1)
    select {
    case <- self.wakeup_signal:
        self.wakeup_signal = nil
        return nil
    case <- time.After(5 * time.Second):
        self.wakeup_signal = nil
        return nil
    }
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
        self.protocol.Close()
    }
    self.stream = nil
    self.protocol = nil
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
            server.Close()
            <- server.closed_waiter
        }
    }
    current_member.server = self
    self.member = current_member
    go self.Run()
    if current_member.client != nil {
        if current_member.client.wakeup_signal != nil {
            current_member.client.wakeup_signal <- true
        }
    }

    if manager.voter.wakeup_signal != nil {
        manager.voter.wakeup_signal <- true
    }
    self.member.manager.slock.Log().Infof("Arbiter Server Accept Client Connected %s", current_member.host)
    return nil
}

func (self *ArbiterServer) Run() {
    if !self.stream.closed {
        self.stream.closed_waiter = make(chan bool, 1)
        <- self.stream.closed_waiter
    }
    self.protocol = nil
    self.stream = nil
    self.closed = true
    if self.closed_waiter != nil {
        self.closed_waiter <- true
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
    aof_id          [16]byte
    isself          bool
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
}

func NewArbiterMember(manager *ArbiterManager, host string, weight uint32, arbiter uint32) *ArbiterMember {
    return &ArbiterMember{manager, &sync.Mutex{}, nil, nil, host, weight, arbiter, ARBITER_ROLE_UNKNOWN,
        ARBITER_MEMBER_STATUS_UNOPEN, 0, 0, [16]byte{}, false, false, make(chan bool, 1), nil}
}

func (self *ArbiterMember) Open() error {
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
    if self.closed {
        return nil
    }

    self.closed = true
    if self.client != nil {
        self.client.Close()
        if !self.client.closed {
            <- self.client.closed_waiter
        }
        self.client = nil
    }
    
    if self.server != nil {
        self.server.Close()
        if !self.server.closed {
            <- self.server.closed_waiter
        }
        self.server = nil
    }

    if self.wakeup_signal != nil {
        self.wakeup_signal <- true
    }
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
        return err
    }
    
    if call_result_command.Result != 0 || call_result_command.ErrType != "" {
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
    return nil
}

func (self *ArbiterMember) ClientOnline(client  *ArbiterClient) error {
    if self.client != client {
        return nil
    }

    self.status = ARBITER_MEMBER_STATUS_ONLINE
    self.manager.MemberStatusUpdated(self)
    return nil
}

func (self *ArbiterMember) ClientOffline(client  *ArbiterClient) error {
    if self.client != client {
        return nil
    }
    
    self.status = ARBITER_MEMBER_STATUS_OFFLINE
    self.manager.MemberStatusUpdated(self)
    return nil
}

func (self *ArbiterMember) Run() {
    if !self.isself {
        go self.client.Run()
    } else {
        self.status = ARBITER_MEMBER_STATUS_ONLINE
        self.last_updated = time.Now().UnixNano()
    }

    for ; !self.closed; {
        self.wakeup_signal = make(chan bool, 1)
        select {
        case <-self.wakeup_signal:
            self.wakeup_signal = nil
        case <-time.After(5 * time.Second):
            self.wakeup_signal = nil
            if self.client != nil {
                self.UpdateStatus()
            }
        }
    }

    self.closed_waiter <- true
}

func (self *ArbiterMember) DoVote() (*protobuf.ArbiterVoteResponse, error) {
    if self.isself || self.status != ARBITER_MEMBER_STATUS_ONLINE {
        member, err := self.manager.GetVoteHost()
        if err != nil {
            return nil, err
        }
        aof_id := self.manager.EncodeAofId(self.manager.GetCurrentAofID())
        return &protobuf.ArbiterVoteResponse{ErrMessage:"", VoteHost:member.host,
            VoteAofId:self.manager.EncodeAofId(member.aof_id), AofId:aof_id, Role:uint32(member.role)}, nil
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

        if call_result_command.ContentLen > 0 {
            response := protobuf.ArbiterVoteResponse{}
            err = response.Unmarshal(call_result_command.Data)
            if err == nil {
                self.aof_id = self.manager.DecodeAofId(response.AofId)
                self.role = uint8(response.Role)
            }
        }
        self.manager.slock.Log().Errorf("Arbiter Vote Error %s %d %s", self.host, call_result_command.Result, call_result_command.ErrType)
        return nil, errors.New(fmt.Sprintf("call error %d %s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterVoteResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

    self.aof_id = self.manager.DecodeAofId(response.AofId)
    self.role = uint8(response.Role)
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
        self.manager.slock.Log().Errorf("Arbiter Proposal Error %s %d %s", self.host, call_result_command.Result, call_result_command.ErrType)
        return nil, errors.New(fmt.Sprintf("call error %d %s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterProposalResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }

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
        self.manager.slock.Log().Errorf("Arbiter Commit Error %s %d %s", self.host, call_result_command.Result, call_result_command.ErrType)
        return nil, errors.New(fmt.Sprintf("call error %d %s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterCommitResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }
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

    replset := protobuf.ReplSet{Name:self.manager.name, Version:self.manager.version, Owner:self.host, Members:members}
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
        self.manager.slock.Log().Errorf("Arbiter Announcement Error %s %d %s", self.host, call_result_command.Result, call_result_command.ErrType)
        return nil, errors.New(fmt.Sprintf("call error %d %s", call_result_command.Result, call_result_command.ErrType))
    }

    response := protobuf.ArbiterAnnouncementResponse{}
    err = response.Unmarshal(call_result_command.Data)
    if err != nil {
        return nil, err
    }
    return &response, nil
}

type ArbiterVoter struct {
    manager         *ArbiterManager
    glock           *sync.Mutex
    proposal_id     uint64
    vote_host       string
    vote_aof_id     [16]byte
    request_waiter  chan bool
    state           uint8
    closed          bool
    closed_waiter   chan bool
    wakeup_signal	chan bool
}

func NewArbiterVoter() *ArbiterVoter {
    return &ArbiterVoter{nil, &sync.Mutex{}, 0, "", [16]byte{},
        make(chan bool, 1), 0, false, nil, nil}
}

func (self *ArbiterVoter) Close() error {
    self.glock.Lock()
    self.closed = true
    if self.state != 0 {
        self.closed_waiter = make(chan bool, 1)
        self.glock.Unlock()
        if self.wakeup_signal != nil {
            self.wakeup_signal <- true
        }
        <- self.closed_waiter
        self.glock.Lock()
        self.closed_waiter = nil
    }
    self.glock.Unlock()

    if self.wakeup_signal != nil {
        self.wakeup_signal <- true
    }
    return nil
}

func (self *ArbiterVoter) StartVote() error {
    self.glock.Lock()
    if self.closed || self.state != 0 {
        self.glock.Unlock()
        return errors.New("state error")
    }
    self.state = 1
    self.glock.Unlock()
    
    go func() {
        defer func() {
            self.glock.Lock()
            self.state = 0
            if self.closed_waiter != nil {
                self.closed_waiter <- true
            }
            self.glock.Unlock()
        }()

        for ; !self.closed; {
            if self.manager.leader_member != nil {
                if self.manager.leader_member.status == ARBITER_MEMBER_STATUS_ONLINE {
                    self.manager.slock.Log().Infof("Arbier Voter Vote Finish, Current Leader %s", self.manager.leader_member.host)
                    return
                }
            }

            err := self.DoVote()
            if err == nil {
                err = self.DoProposal()
                if err == nil {
                    err = self.DoCommit()
                    if err == nil {
                        self.manager.VoteSucced()
                        return
                    }
                }
            }

            self.SleepWhenRetryConnect()
        }
        return
    }()
    return nil
}

func (self *ArbiterVoter) DoRequests(name string, handler func(*ArbiterMember) (interface{}, error)) []interface{} {
    count, finish_count := 0, 0
    responses := make([]interface{}, 0)
    self.glock.Lock()
    for _, member := range self.manager.members {
        go func(member *ArbiterMember) {
            response, err := handler(member)
            self.glock.Lock()
            if err == nil {
                responses = append(responses, response)
            } else {
                self.manager.slock.Log().Errorf("Arbier Voter Request Error %s %v %v", member.host, name, err)
            }
            finish_count++
            if finish_count >= count {
                self.request_waiter <- true
            }
            self.glock.Unlock()
        }(member)
        count++
    }
    self.glock.Unlock()
    <- self.request_waiter
    return responses
}

func (self *ArbiterVoter) DoVote() error {
    self.vote_host, self.vote_aof_id = "", [16]byte{}
    responses := self.DoRequests("DoVote", func(member *ArbiterMember) (interface{}, error) {
        return member.DoVote()
    })
    
    require_vote_count := len(self.manager.members) / 2 + 1
    if len(responses) < require_vote_count {
        return errors.New("member vote count too small")
    }
    
    votes, max_vote := make(map[string]int, 4), 0
    for i := 0; i < len(responses); i++ {
        response := responses[i].(*protobuf.ArbiterVoteResponse)
        if vi, ok := votes[response.VoteHost]; ok {
            votes[response.VoteHost]++
            if max_vote < vi + 1 {
                max_vote = vi + 1
            }
        } else {
            votes[response.VoteHost] = 1
            if max_vote < 1 {
                max_vote = 1
            }
        }
        if self.manager.CompareAofId(self.manager.DecodeAofId(response.VoteAofId), self.vote_aof_id) > 0 {
            self.vote_aof_id = self.manager.DecodeAofId(response.VoteAofId)
        }
    }
    
    if max_vote < require_vote_count {
        return errors.New("member vote count too small")
    }
    
    for host, vc := range votes {
        if vc >= max_vote {
            self.vote_host = host
        }
    }
    self.manager.slock.Log().Infof("Arbier Voter Vote Succed %s %x %d", self.vote_host, self.vote_aof_id, self.proposal_id)
    return nil
}

func (self *ArbiterVoter) DoProposal() error {
    responses := self.DoRequests("DoProposal", func(member *ArbiterMember) (interface{}, error) {
        return member.DoProposal(self.proposal_id + 1, self.vote_host, self.vote_aof_id)
    })
    
    if len(responses) < len(self.manager.members) / 2 + 1 {
        return errors.New("member accept proposal count too small")
    }

    self.manager.slock.Log().Infof("Arbier Voter Proposal Succed %s %x %d", self.vote_host, self.vote_aof_id, self.proposal_id)
    return nil
}

func (self *ArbiterVoter) DoCommit() error {
    responses := self.DoRequests("DoCommit", func(member *ArbiterMember) (interface{}, error) {
        return member.DoCommit(self.proposal_id + 1, self.vote_host, self.vote_aof_id)
    })

    if len(responses) < len(self.manager.members) / 2 + 1 {
        return errors.New("member accept proposal count too small")
    }
    
    self.proposal_id++
    self.manager.slock.Log().Infof("Arbier Voter Commit Succed %s %x %d", self.vote_host, self.vote_aof_id, self.proposal_id)
    return nil
}

func (self *ArbiterVoter) DoAnnouncement() error {
    responses := self.DoRequests("DoAnnouncement", func(member *ArbiterMember) (interface{}, error) {
        return member.DoAnnouncement()
    })

    if len(responses) < len(self.manager.members) / 2 + 1 {
        return errors.New("member accept proposal count too small")
    }
    return nil
}

func (self *ArbiterVoter) SleepWhenRetryConnect() error {
    self.wakeup_signal = make(chan bool, 1)
    select {
    case <- self.wakeup_signal:
        self.wakeup_signal = nil
        return nil
    case <- time.After(5 * time.Second):
        self.wakeup_signal = nil
        return nil
    }
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
    version         uint32
    stoped          bool
    loaded          bool
}

func NewArbiterManager(slock *SLock, name string) *ArbiterManager {
    store := NewArbiterStore()
    voter := NewArbiterVoter()
    manager := &ArbiterManager{slock, &sync.Mutex{}, store, voter,
        make([]*ArbiterMember, 0), nil, nil, name, 1, false, false}
    voter.manager = manager
    return manager
}

func (self *ArbiterManager) Load() error {
    defer self.glock.Unlock()
    self.glock.Lock()
    
    err := self.store.Load(self)
    if err != nil {
        self.slock.Log().Errorf("Arbiter Load Meta File error: %v", err)
    }

    aof_id, err := self.slock.aof.LoadMaxId()
    if err != nil {
        self.slock.Log().Errorf("Arbiter Load Aof File MaxID error: %v", err)
    } else {
        self.slock.Log().Infof("Arbiter Load Aof File MaxID %x", aof_id)
    }
    self.slock.replication_manager.current_request_id = aof_id
    self.voter.proposal_id = uint64(self.version)
    return nil
}

func (self *ArbiterManager) Start() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.own_member == nil || len(self.members) == 0 {
        return nil
    }

    for _, member := range self.members {
        err := member.Open()
        if err != nil {
            return err
        }
    }

    for _, member := range self.members {
        go member.Run()
    }

    self.slock.UpdateState(STATE_VOTE)
    err := self.StartVote()
    if err != nil {
        self.slock.Log().Errorf("Arbiter Vote error: %v", err)
    }
    return nil
}

func (self *ArbiterManager) Close() error {
    self.glock.Lock()
    if self.stoped {
        self.glock.Unlock()
        return nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        self.stoped = true
        self.glock.Unlock()
        self.slock.logger.Infof("Arbiter Closed")
        return nil
    }

    if self.own_member.role == ARBITER_ROLE_LEADER && len(self.members) > 1 {
        err := self.QuitLeader()
        if err != nil {
            self.glock.Unlock()
            return err
        }
    } else {
        self.store.Save(self)
    }
    self.stoped = true
    self.glock.Unlock()

    self.voter.Close()
    for _, member := range self.members {
        member.Close()
        <- member.closed_waiter
    }
    self.slock.logger.Infof("Arbiter Closed")
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
    self.slock.UpdateState(STATE_VOTE)
    go member.Run()
    self.version++
    self.voter.proposal_id = uint64(self.version)
    self.store.Save(self)
    err = self.StartVote()
    if err != nil {
        self.slock.Log().Errorf("Arbiter Vote error: %v", err)
    }
    self.slock.logger.Infof("Arbiter Config %s %d %d", host, weight, arbiter)
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
    self.store.Save(self)
    self.DoAnnouncement()
    self.UpdateStatus()
    self.slock.logger.Infof("Arbiter Add Member %s %d %d", host, weight, arbiter)
    return nil
}

func (self *ArbiterManager) RemoveMember(host string) error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.own_member == nil || len(self.members) == 0 {
        return errors.New("member info error")
    }

    if self.own_member.host == host {
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
        return errors.New("not found member")
    }
    self.members = members
    current_member.Close()
    <- current_member.closed_waiter
    self.version++
    self.store.Save(self)
    self.DoAnnouncement()
    self.UpdateStatus()
    self.slock.logger.Infof("Arbiter Remove Member %s", host)
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
    self.store.Save(self)
    self.DoAnnouncement()
    self.UpdateStatus()
    self.slock.logger.Infof("Arbiter Update Member %s %d %d", host, weight, arbiter)
    return nil
}

func (self *ArbiterManager) GetMembers() []*ArbiterMember {
    return self.members
}

func (self *ArbiterManager) QuitLeader() error {
    self.slock.UpdateState(STATE_SYNC)
    self.own_member.role = ARBITER_ROLE_FOLLOWER
    self.leader_member = nil
    self.version++
    self.store.Save(self)
    self.voter.DoAnnouncement()

    if self.own_member.arbiter == 0 {
        err := self.slock.replication_manager.SwitchToFollower("")
        if err != nil {
            return err
        }
        return nil
    }
    self.slock.UpdateState(STATE_FOLLOWER)
    return nil
}

func (self *ArbiterManager) GetVoteHost() (*ArbiterMember, error) {
    if self.own_member == nil || len(self.members) == 0 {
        return nil, errors.New("not members")
    }

    var vote_member *ArbiterMember = nil
    for _, member := range self.members {
        if !member.isself {
            if member.status != ARBITER_MEMBER_STATUS_ONLINE || member.arbiter != 0 {
                continue
            }
        } else {
            member.aof_id = self.GetCurrentAofID()
        }

        if vote_member == nil {
            vote_member = member
            continue
        }

        if vote_member.aof_id == member.aof_id {
            if vote_member.weight < member.weight {
                vote_member = member
            }
            if vote_member.weight == member.weight {
                if vote_member.host < member.host {
                    vote_member = member
                }
            }
            continue
        }

        if self.CompareAofId(member.aof_id, vote_member.aof_id) > 0 {
            vote_member = member
        }
    }

    if vote_member == nil {
        return nil, errors.New("not found")
    }
    return vote_member, nil
}

func (self *ArbiterManager) DoAnnouncement() {
    if self.stoped {
        return
    }

    go func() {
        self.slock.Log().Infof("Arbiter Replication Announcement")
        err := self.voter.DoAnnouncement()
        if err != nil {
            self.slock.Log().Errorf("Arbiter Announcement Error %v", err)
        }
    }()
}

func (self *ArbiterManager) StartVote() error {
    if self.stoped {
        return nil
    }

    self.slock.UpdateState(STATE_VOTE)
    self.slock.Log().Infof("Arbiter Start Vote")
    return self.voter.StartVote()
}

func (self *ArbiterManager) VoteSucced() error {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.stoped {
        return nil
    }

    for _, member := range self.members {
        if member.host == self.voter.vote_host {
            member.role = ARBITER_ROLE_LEADER
            self.leader_member = member
        } else {
            member.role = ARBITER_ROLE_FOLLOWER
        }
    }

    self.version++
    self.store.Save(self)
    if self.own_member.role == ARBITER_ROLE_LEADER {
        self.UpdateStatus()
        self.voter.DoAnnouncement()
    } else {
        if self.slock.state == STATE_LEADER {
            self.slock.UpdateState(STATE_SYNC)
        }
        self.voter.DoAnnouncement()
        self.UpdateStatus()
    }

    self.slock.Log().Infof("Arbiter Vote Succed")
    if self.leader_member != nil {
        self.slock.Log().Infof("Arbiter Current Leader: %s", self.leader_member.host)
    }
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

        if member.status == ARBITER_MEMBER_STATUS_UNOPEN {
            err := member.Open()
            if err == nil {
                go member.Run()
            }
        }
    }

    if self.own_member == nil {
        return nil
    }

    if self.leader_member == nil {
        if self.own_member.arbiter == 0 {
            self.slock.replication_manager.SwitchToFollower("")
        } else {
            self.slock.UpdateState(STATE_FOLLOWER)
        }
        self.StartVote()
        return nil
    }

    if self.leader_member.host == self.own_member.host {
        if self.slock.state != STATE_LEADER {
            if self.own_member.arbiter == 0 {
                if !self.loaded {
                    self.slock.InitLeader()
                    self.slock.StartLeader()
                    self.loaded = true
                    return nil
                }
                self.slock.replication_manager.SwitchToLeader()
            } else {
                self.slock.UpdateState(STATE_LEADER)
            }
        }
        return nil
    }

    if self.own_member.arbiter == 0 {
        if !self.loaded {
            self.slock.InitFollower(self.leader_member.host)
            self.slock.StartFollower()
            self.loaded = true
            return nil
        }
        self.slock.replication_manager.SwitchToFollower(self.leader_member.host)
        return nil
    }
    self.slock.UpdateState(STATE_FOLLOWER)
    return nil
}

func (self *ArbiterManager) MemberStatusUpdated(member *ArbiterMember) error {
    if self.own_member == nil || self.stoped {
        return nil
    }

    if member.role == ARBITER_ROLE_LEADER {
        if member.status != ARBITER_MEMBER_STATUS_ONLINE {
            err := self.StartVote()
            if err != nil {
                self.slock.Log().Errorf("Arbiter Vote error: %v", err)
            }
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
            self.glock.Lock()
            err := self.QuitLeader()
            self.glock.Unlock()
            if err != nil {
                self.slock.Log().Errorf("Arbiter Quit Leader error: %v", err)
            }
            err = self.StartVote()
            if err != nil {
                self.slock.Log().Errorf("Arbiter Vote error: %v", err)
            }
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
    resquest := protobuf.ArbiterConnectRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    server := NewArbiterServer(server_protocol)
    response, err := server.HandleInit(self, &resquest)
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
    resquest := protobuf.ArbiterVoteRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil || len(self.members) == 0 {
        return protocol.NewCallResultCommand(command, 0, "ERR_UNINIT", nil), nil
    }

    go func() {
        for _, member := range self.members {
            if member.status == ARBITER_MEMBER_STATUS_ONLINE && !member.isself {
                member.UpdateStatus()
            }
        }
    }()

    err_type := ""
    if self.own_member.role == ARBITER_ROLE_LEADER {
        self.DoAnnouncement()
        err_type = "ERR_ROLE"
    } else {
        for _, member := range self.members {
            if member.role == ARBITER_ROLE_LEADER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
                err_type = "ERR_STATUS"
            }
        }
    }

    member, err := self.GetVoteHost()
    if err != nil {
        response := protobuf.ArbiterVoteResponse{ErrMessage:"", VoteHost:"", VoteAofId:"",
            AofId:self.EncodeAofId(self.GetCurrentAofID()), Role:uint32(self.own_member.role)}
        data, err := response.Marshal()
        if err != nil {
            return protocol.NewCallResultCommand(command, 0, "ERR_VOTE", nil), nil
        }
        return protocol.NewCallResultCommand(command, 0, "ERR_VOTE", data), nil
    }

    response := protobuf.ArbiterVoteResponse{ErrMessage:"", VoteHost:member.host, VoteAofId:self.EncodeAofId(member.aof_id),
        AofId:self.EncodeAofId(self.GetCurrentAofID()), Role:uint32(self.own_member.role)}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, err_type, data), nil
}

func (self *ArbiterManager) CommandHandleProposalCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    resquest := protobuf.ArbiterProposalRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    var vote_member *ArbiterMember = nil
    for _, member := range self.members {
        if member.role == ARBITER_ROLE_LEADER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
            return protocol.NewCallResultCommand(command, 0, "ERR_STATUS", nil), nil
        }

        if member.host == resquest.Host {
            vote_member = member
        }

        if self.CompareAofId(member.aof_id, self.DecodeAofId(resquest.AofId)) > 0 {
            return protocol.NewCallResultCommand(command, 0, "ERR_AOFID", nil), nil
        }
    }

    if vote_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
    }

    if self.voter.proposal_id >= resquest.ProposalId {
        return protocol.NewCallResultCommand(command, 0, "ERR_PROPOSALID", nil), nil
    }
    response := protobuf.ArbiterProposalResponse{ErrMessage:""}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleCommitCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    resquest := protobuf.ArbiterCommitRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    var vote_member *ArbiterMember = nil
    for _, member := range self.members {
        if member.role == ARBITER_ROLE_LEADER && member.status == ARBITER_MEMBER_STATUS_ONLINE {
            return protocol.NewCallResultCommand(command, 0, "ERR_STATUS", nil), nil
        }

        if member.host == resquest.Host {
            vote_member = member
        }

        if self.CompareAofId(member.aof_id, self.DecodeAofId(resquest.AofId)) > 0 {
            return protocol.NewCallResultCommand(command, 0, "ERR_AOFID", nil), nil
        }
    }

    if vote_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_HOST", nil), nil
    }

    if self.voter.proposal_id >= resquest.ProposalId {
        return protocol.NewCallResultCommand(command, 0, "ERR_PROPOSALID", nil), nil
    }
    response := protobuf.ArbiterCommitResponse{ErrMessage:""}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    self.voter.proposal_id = resquest.ProposalId
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}

func (self *ArbiterManager) CommandHandleAnnouncementCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
    resquest := protobuf.ArbiterAnnouncementRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if resquest.Replset.Name != self.name {
        return protocol.NewCallResultCommand(command, 0, "ERR_NAME", nil), nil
    }

    if resquest.Replset.Version < self.version {
        return protocol.NewCallResultCommand(command, 0, "ERR_VERSION", nil), nil
    }

    members, new_members := make(map[string]*ArbiterMember, 4), make([]*ArbiterMember, 0)
    for _, member := range self.members {
        members[member.host] = member
    }

    var own_member, leader_member *ArbiterMember = nil, nil
    for _, rplm := range resquest.Replset.Members {
        if member, ok := members[rplm.Host]; ok {
            member.weight = rplm.Weight
            member.arbiter = rplm.Arbiter
            member.role = uint8(rplm.Role)
            delete(members, rplm.Host)
            if member.host == resquest.Replset.Owner {
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
            self.members = append(self.members, member)
            if member.host == resquest.Replset.Owner {
                own_member = member
                own_member.isself = true
            }
            if member.role == ARBITER_ROLE_LEADER {
                leader_member = member
            }
            new_members = append(new_members, member)
        }
    }

    if own_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_OWN", nil), nil
    }

    if leader_member != nil {
        if own_member.role == ARBITER_ROLE_LEADER && leader_member.host != own_member.host {
            return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
        }

        if self.leader_member != nil {
            if self.leader_member.status == ARBITER_MEMBER_STATUS_ONLINE && leader_member.host != self.leader_member.host {
                return protocol.NewCallResultCommand(command, 0, "ERR_LEADER", nil), nil
            }
        }
    }

    self.glock.Lock()
    self.members = new_members
    self.own_member = own_member
    self.leader_member = leader_member

    for _, member := range members {
        member.Close()
    }
    self.version = resquest.Replset.Version
    self.store.Save(self)
    self.glock.Unlock()

    go func() {
        self.glock.Lock()
        err := self.UpdateStatus()
        if err != nil {
            self.slock.Log().Errorf("Arbiter Update Status Error %v", err)
        }
        self.glock.Unlock()
    }()

    for _, member := range self.members {
        if member.host == resquest.FromHost && member.server == nil {
            server := NewArbiterServer(server_protocol)
            server.Attach(self, resquest.FromHost)
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
    resquest := protobuf.ArbiterStatusRequest{}
    err := resquest.Unmarshal(command.Data)
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
    }

    if self.own_member == nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_CONFIG", nil), nil
    }

    aof_id := self.EncodeAofId(self.GetCurrentAofID())
    response := protobuf.ArbiterStatusResponse{ErrMessage:"", AofId:aof_id, Role:uint32(self.own_member.role)}
    data, err := response.Marshal()
    if err != nil {
        return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
    }
    return protocol.NewCallResultCommand(command, 0, "", data), nil
}