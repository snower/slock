package server

import (
	"testing"
	"time"
)

func TestStopAndDrainTimerDrainsPendingTick(t *testing.T) {
	timer := time.NewTimer(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	stopAndDrainTimer(timer)

	select {
	case <-timer.C:
		t.Fatal("expected pending timer tick to be drained")
	default:
	}
}

func TestResetTimerClearsPendingTick(t *testing.T) {
	timer := time.NewTimer(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	resetTimer(timer, 40*time.Millisecond)
	defer stopAndDrainTimer(timer)

	select {
	case <-timer.C:
		t.Fatal("received stale timer tick after reset")
	case <-time.After(15 * time.Millisecond):
	}

	select {
	case <-timer.C:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected timer to fire after reset")
	}
}
