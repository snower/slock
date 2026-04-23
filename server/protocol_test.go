package server

import (
	"sync"
	"testing"
	"time"

	"github.com/snower/slock/protocol"
)

func TestServerProtocolFreeCollectorCollectMovesCommandsToSLock(t *testing.T) {
	serverConfig := &ServerConfig{DBConcurrent: 1}
	slock := NewSLock(serverConfig, nil)
	slock.freeLockCommandLock = &sync.Mutex{}
	slock.freeLockCommandQueue = NewLockCommandQueue(4, 16, 32)

	lockedFreeCommands := NewLockCommandQueue(4, 16, 32)
	for i := 0; i < 20; i++ {
		_ = lockedFreeCommands.Push(&protocol.LockCommand{})
	}

	collector := &ServerProtocolFreeCollector{
		lastCollectTime:          time.Now().Unix() - 1,
		lastTotalCommandCount:    0,
		lastAvgCommandCount:      10,
		lastFreeLockCommandCount: 19,
	}
	glock := &sync.Mutex{}
	if err := collector.Collect(slock, glock, lockedFreeCommands, 1); err != nil {
		t.Fatalf("collect failed: %v", err)
	}

	if lockedFreeCommands.Len() != 17 {
		t.Fatalf("expected protocol free queue len 19 after one object moved, got %d", lockedFreeCommands.Len())
	}
	if slock.freeLockCommandQueue.Len() != 3 {
		t.Fatalf("expected one command to be moved into slock free queue, got %d", slock.freeLockCommandQueue.Len())
	}
	if slock.freeLockCommandCount != 3 {
		t.Fatalf("expected slock free command count to increment to 1, got %d", slock.freeLockCommandCount)
	}
}
