package server

import (
	"sync"
	"testing"
	"time"

	"github.com/snower/slock/protocol"
)

func TestSLockAddServerProtocolReturnsSession(t *testing.T) {
	serverConfig := &ServerConfig{DBConcurrent: 1}
	slock := NewSLock(serverConfig, nil)
	serverProtocol := NewDefaultServerProtocol(slock)

	session := slock.addServerProtocol(serverProtocol)
	if session == nil {
		t.Fatal("expected session to be returned")
	}
	if session.serverProtocol != serverProtocol {
		t.Fatal("expected session to keep protocol reference")
	}
	if slock.protocolSessions[session.sessionId] != session {
		t.Fatal("expected session to be stored in protocolSessions")
	}
}

func TestSLockFreeCollectorCollectShrinksFreeQueue(t *testing.T) {
	serverConfig := &ServerConfig{DBConcurrent: 1}
	slock := NewSLock(serverConfig, nil)
	slock.freeLockCommandLock = &sync.Mutex{}
	slock.freeLockCommandQueue = NewLockCommandQueue(4, 16, 32)
	slock.freeLockCommandCount = 20
	for i := 0; i < 20; i++ {
		_ = slock.freeLockCommandQueue.Push(&protocol.LockCommand{})
	}

	collector := &SLockFreeCollector{
		lastCollectTime:          time.Now().Unix() - 1,
		lastTotalCommandCount:    0,
		lastAvgCommandCount:      10,
		lastFreeLockCommandCount: 19,
	}
	if err := collector.Collect(slock, 1); err != nil {
		t.Fatalf("collect failed: %v", err)
	}

	if slock.freeLockCommandQueue.Len() != 20 {
		t.Fatalf("expected free queue len 19 after one object reclaimed, got %d", slock.freeLockCommandQueue.Len())
	}
	if collector.lastFreeLockCommandCount != 20 {
		t.Fatalf("expected collector to record previous free count 20, got %d", collector.lastFreeLockCommandCount)
	}
}
