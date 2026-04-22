package server

import (
	"net"
	"sync"
	"testing"

	"github.com/snower/slock/protocol"
)

type testSubscribeServerProtocol struct {
	stream *Stream
	proxy  *ProxyServerProtocol
}

func newTestSubscribeServerProtocol() *testSubscribeServerProtocol {
	serverProtocol := &testSubscribeServerProtocol{stream: &Stream{closedWaiter: make(chan struct{})}}
	serverProtocol.proxy = &ProxyServerProtocol{serverProtocol: serverProtocol}
	return serverProtocol
}

func (self *testSubscribeServerProtocol) Init(_ [16]byte) error { return nil }
func (self *testSubscribeServerProtocol) Lock()                 {}
func (self *testSubscribeServerProtocol) Unlock()               {}
func (self *testSubscribeServerProtocol) Read() (protocol.CommandDecode, error) {
	return nil, nil
}
func (self *testSubscribeServerProtocol) Write(protocol.CommandEncode) error { return nil }
func (self *testSubscribeServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return nil, nil
}
func (self *testSubscribeServerProtocol) WriteCommand(protocol.CommandEncode) error { return nil }
func (self *testSubscribeServerProtocol) Process() error                            { return nil }
func (self *testSubscribeServerProtocol) ProcessParse([]byte) error                 { return nil }
func (self *testSubscribeServerProtocol) ProcessBuild(protocol.ICommand) error      { return nil }
func (self *testSubscribeServerProtocol) ProcessCommad(protocol.ICommand) error     { return nil }
func (self *testSubscribeServerProtocol) ProcessLockCommand(*protocol.LockCommand) error {
	return nil
}
func (self *testSubscribeServerProtocol) ProcessLockResultCommand(*protocol.LockCommand, uint8, uint16, uint8, []byte) error {
	return nil
}
func (self *testSubscribeServerProtocol) ProcessLockResultCommandLocked(*protocol.LockCommand, uint8, uint16, uint8, []byte) error {
	return nil
}
func (self *testSubscribeServerProtocol) Close() error                   { return nil }
func (self *testSubscribeServerProtocol) GetStream() *Stream             { return self.stream }
func (self *testSubscribeServerProtocol) GetProxy() *ProxyServerProtocol { return self.proxy }
func (self *testSubscribeServerProtocol) AddProxy(proxy *ProxyServerProtocol) error {
	self.proxy = proxy
	return nil
}
func (self *testSubscribeServerProtocol) RemoteAddr() net.Addr                  { return &net.TCPAddr{} }
func (self *testSubscribeServerProtocol) GetLockCommand() *protocol.LockCommand { return nil }
func (self *testSubscribeServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
	return nil
}
func (self *testSubscribeServerProtocol) FreeLockCommand(*protocol.LockCommand) error {
	return nil
}
func (self *testSubscribeServerProtocol) FreeLockCommandLocked(*protocol.LockCommand) error {
	return nil
}
func (self *testSubscribeServerProtocol) FreeCollect() error { return nil }

func TestSubscriberUpdateRemoveMaskKeepsOtherMasks(t *testing.T) {
	serverProtocol := newTestSubscribeServerProtocol()
	subscriber := &Subscriber{
		glock:                      &sync.Mutex{},
		lockKeyMasks:               make([][2]uint64, 0),
		serverProtocolClosedWaiter: make(chan struct{}),
		pullWaiter:                 make(chan struct{}, 1),
	}
	subscriber.serverProtocol = serverProtocol

	mask1 := [16]byte{1}
	mask2 := [16]byte{2}
	mask3 := [16]byte{3}

	if err := subscriber.Update(serverProtocol, 0, mask1, 30, 1024); err != nil {
		t.Fatalf("add first mask failed: %v", err)
	}
	if err := subscriber.Update(serverProtocol, 0, mask2, 30, 1024); err != nil {
		t.Fatalf("add second mask failed: %v", err)
	}
	if err := subscriber.Update(serverProtocol, 0, mask3, 30, 1024); err != nil {
		t.Fatalf("add third mask failed: %v", err)
	}
	if err := subscriber.Update(serverProtocol, 1, mask2, 30, 1024); err != nil {
		t.Fatalf("remove middle mask failed: %v", err)
	}

	if len(subscriber.lockKeyMasks) != 2 {
		t.Fatalf("expected 2 masks to remain, got %d", len(subscriber.lockKeyMasks))
	}
	if subscriber.lockKeyMasks[0] != [2]uint64{1, 0} {
		t.Fatalf("expected first mask to remain, got %#v", subscriber.lockKeyMasks[0])
	}
	if subscriber.lockKeyMasks[1] != [2]uint64{3, 0} {
		t.Fatalf("expected third mask to remain, got %#v", subscriber.lockKeyMasks[1])
	}
}
