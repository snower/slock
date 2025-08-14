package server

import (
	"math/rand"
	"testing"

	"github.com/snower/slock/protocol"
)

func BenchmarkLockManagerQueue(b *testing.B) {
	lockManager := &LockManager{}
	queue := NewLockManagerQueue(4, 16, 4)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			n := rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Push(lockManager)
			}
			n = rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Pop()
			}
		}
	}
}

func TestLockManagerQueuePushPop(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Pop() != head {
		t.Error("LockManagerQueue Push Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockManagerQueue Push Pop Test Nil Fail")
		return
	}
}

func TestLockManagerQueuePushPopRight(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.PopRight() != tail {
		t.Error("LockManagerQueue Push PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockManagerQueue Push PopRight Test Nil Fail")
		return
	}
}

func TestLockManagerQueuePushLeftPop(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.Pop() != head {
		t.Error("LockManagerQueue PushLeft Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockManagerQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockManagerQueuePushLeftPopRight(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.PopRight() != head {
		t.Error("LockManagerQueue PushLeft PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockManagerQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockManagerQueueHead(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Head() != head {
		t.Error("LockManagerQueue Head Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Head() != nil {
		t.Error("LockManagerQueue Head Test Nil Fail")
		return
	}
}

func TestLockManagerQueueTail(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Tail() != tail {
		t.Error("LockManagerQueue Tail Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Tail() != nil {
		t.Error("LockManagerQueue Tail Test Nil Fail")
		return
	}
}

func TestLockManagerQueueLen(t *testing.T) {
	head := &LockManager{}
	tail := &LockManager{}

	q := NewLockManagerQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Len() != 2 {
		t.Error("LockManagerQueue Len Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	for i := 0; i < 10; i++ {
		_ = q.Push(head)
	}
	if q.Len() != 10 {
		t.Error("LockManagerQueue Len Test Many Fail")
		return
	}

	for i := 0; i < 10; i++ {
		q.Pop()
	}
	if q.Len() != 0 {
		t.Error("LockManagerQueue Len Test Zero Fail")
		return
	}
}

func TestLockManagerQueueReset(t *testing.T) {
	l := &LockManager{}
	q := NewLockManagerQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockManagerQueue Reset queue_size Fail")
		return
	}
	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockManagerQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}
}

func TestLockManagerQueueRellac(t *testing.T) {
	l := &LockManager{}
	q := NewLockManagerQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	rellacNodeIndex := q.nodeIndex
	_ = q.Rellac()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockManagerQueue Rellac queue_size Fail")
		return
	}
	if q.nodeIndex != rellacNodeIndex {
		t.Error("LockManagerQueue Rellac nodeIndex Fail")
		return
	}
	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockManagerQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Rellac()
	if q.nodeIndex == rellacNodeIndex {
		t.Errorf("LockManagerQueue Rellac nodeIndex Fail %d %d", q.nodeIndex, rellacNodeIndex)
		return
	}
}

func TestLockManagerQueueResize(t *testing.T) {
	l := &LockManager{}
	q := NewLockManagerQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 60 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}
	ht := q.tailNodeIndex - q.headNodeIndex

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != ht {
		t.Error("LockManagerQueue Node Index Fail")
		return
	}

	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockManagerQueue Resize Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != 0 {
		t.Error("LockManagerQueue Empty Node Index Fail")
		return
	}

	nodeIndex = 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockManagerQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockManagerQueue Resize queue_size Fail")
		return
	}
}

func TestLockManagerQueueRestructuring(t *testing.T) {
	l := &LockManager{}
	q := NewLockManagerQueue(2, 6, 4)
	qlen := 0
	rlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
				rlen++
			}
		} else {
			if q.tailQueueIndex > 0 && q.tailQueue[q.tailQueueIndex-1] != nil {
				q.tailQueue[q.tailQueueIndex-1] = nil
				rlen--
			}
		}
	}

	lastL := &LockManager{}
	if q.Push(lastL) == nil {
		qlen++
		rlen++
	}

	if q.Len() != int32(qlen) {
		t.Error("LockManagerQueue Len Fail")
		return
	}

	_ = q.Restructuring()
	if q.Len() != int32(rlen) {
		t.Error("LockManagerQueue Restructuring Len Fail")
		return
	}

	if q.PopRight() != lastL {
		t.Error("LockManagerQueue Restructuring Value Fail")
		return
	}
	rlen--

	nodeSize := 0
	nodeIndex := 0
	for i, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
		nodeIndex = i
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Errorf("LockManagerQueue Restructuring queue_size Fail %d %d", q.queueSize, q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)))
		return
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockManagerQueue Empty Node_Index Fail %d %d", nodeSize, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		rlen--
	}

	if rlen != 0 {
		t.Error("LockManagerQueue Restructuring Pop Empty Fail")
		return
	}
}

func TestLockManagerQueueIter(t *testing.T) {
	q := NewLockManagerQueue(2, 6, 4)
	for i := 0; i < 100; i++ {
		_ = q.Push(&LockManager{})
	}

	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockManagerQueue Pop After Iter Empty Fail")
			return
		}
		for _, lockManager := range nodeQueues {
			if lockManager == nil {
				t.Error("LockManagerQueue Push After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockManagerQueue Pop After Iter Empty Fail")
			return
		}
		for _, lockManager := range nodeQueues {
			if lockManager == nil {
				t.Error("LockManagerQueue Pop After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) != 0 {
			t.Error("LockManagerQueue Pop After Iter Not Empty Fail")
			return
		}
	}
}

func BenchmarkLockQueue(b *testing.B) {
	lock := &Lock{}
	queue := NewLockQueue(4, 16, 4)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			n := rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Push(lock)
			}
			n = rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Pop()
			}
		}
	}
}

func TestLockQueuePushPop(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Pop() != head {
		t.Error("LockQueue Push Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockQueue Push Pop Test Nil Fail")
		return
	}
}

func TestLockQueuePushPopRight(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.PopRight() != tail {
		t.Error("LockQueue Push PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockQueue Push PopRight Test Nil Fail")
		return
	}
}

func TestLockQueuePushLeftPop(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.Pop() != head {
		t.Error("LockQueue PushLeft Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockQueuePushLeftPopRight(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.PopRight() != head {
		t.Error("LockQueue PushLeft PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockQueueHead(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Head() != head {
		t.Error("LockQueue Head Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Head() != nil {
		t.Error("LockQueue Head Test Nil Fail")
		return
	}
}

func TestLockQueueTail(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Tail() != tail {
		t.Error("LockQueue Tail Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Tail() != nil {
		t.Error("LockQueue Tail Test Nil Fail")
		return
	}
}

func TestLockQueueLen(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Len() != 2 {
		t.Error("LockQueue Len Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	for i := 0; i < 10; i++ {
		_ = q.Push(head)
	}
	if q.Len() != 10 {
		t.Error("LockQueue Len Test Many Fail")
		return
	}

	for i := 0; i < 10; i++ {
		q.Pop()
	}
	if q.Len() != 0 {
		t.Error("LockQueue Len Test Zero Fail")
		return
	}
}

func TestLockQueueReset(t *testing.T) {
	l := &Lock{}
	q := NewLockQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockQueue Reset queue_size Fail")
		return
	}
	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}
}

func TestLockQueueRellac(t *testing.T) {
	l := &Lock{}
	q := NewLockQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	rellacNodeIndex := q.nodeIndex
	_ = q.Rellac()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockQueue Rellac queue_size Fail")
		return
	}
	if q.nodeIndex != rellacNodeIndex {
		t.Error("LockQueue Rellac nodeIndex Fail")
		return
	}
	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Rellac()
	if q.nodeIndex == rellacNodeIndex {
		t.Errorf("LockQueue Rellac nodeIndex Fail %d %d", q.nodeIndex, rellacNodeIndex)
		return
	}
}

func TestLockQueueResize(t *testing.T) {
	l := &Lock{}
	q := NewLockQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 60 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}
	ht := q.tailNodeIndex - q.headNodeIndex

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != ht {
		t.Error("LockQueue Node Index Fail")
		return
	}

	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockQueue Resize Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != 0 {
		t.Error("LockQueue Empty Node Index Fail")
		return
	}

	nodeIndex = 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockQueue Resize queue_size Fail")
		return
	}
}

func TestLockQueueRestructuring(t *testing.T) {
	l := &Lock{}
	q := NewLockQueue(2, 6, 4)
	qlen := 0
	rlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
				rlen++
			}
		} else {
			if q.tailQueueIndex > 0 && q.tailQueue[q.tailQueueIndex-1] != nil {
				q.tailQueue[q.tailQueueIndex-1] = nil
				rlen--
			}
		}
	}

	lastL := &Lock{}
	if q.Push(lastL) == nil {
		qlen++
		rlen++
	}

	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	_ = q.Restructuring()
	if q.Len() != int32(rlen) {
		t.Error("LockQueue Restructuring Len Fail")
		return
	}

	if q.PopRight() != lastL {
		t.Error("LockQueue Restructuring Value Fail")
		return
	}
	rlen--

	nodeSize := 0
	nodeIndex := 0
	for i, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
		nodeIndex = i
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Errorf("LockQueue Restructuring queue_size Fail %d %d", q.queueSize, q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)))
		return
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", nodeSize, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		rlen--
	}

	if rlen != 0 {
		t.Error("LockQueue Restructuring Pop Empty Fail")
		return
	}
}

func TestLockQueueIter(t *testing.T) {
	q := NewLockQueue(2, 6, 4)
	for i := 0; i < 100; i++ {
		_ = q.Push(&Lock{})
	}

	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockQueue Pop After Iter Empty Fail")
			return
		}
		for _, lock := range nodeQueues {
			if lock == nil {
				t.Error("LockQueue Push After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockQueue Pop After Iter Empty Fail")
			return
		}
		for _, lock := range nodeQueues {
			if lock == nil {
				t.Error("LockQueue Pop After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) != 0 {
			t.Error("LockQueue Pop After Iter Not Empty Fail")
			return
		}
	}
}

func BenchmarkLockCommandQueue(b *testing.B) {
	lockCommand := &protocol.LockCommand{}
	queue := NewLockCommandQueue(4, 16, 4)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			n := rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Push(lockCommand)
			}
			n = rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Pop()
			}
		}
	}
}

func TestLockCommandQueuePushPop(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Pop() != head {
		t.Error("LockCommandQueue Push Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockCommandQueue Push Pop Test Nil Fail")
		return
	}
}

func TestLockCommandQueuePushPopRight(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.PopRight() != tail {
		t.Error("LockCommandQueue Push PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockCommandQueue Push PopRight Test Nil Fail")
		return
	}
}

func TestLockCommandQueuePushLeftPop(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.Pop() != head {
		t.Error("LockCommandQueue PushLeft Pop Test Fail")
		return
	}

	q.Pop()
	if q.Pop() != nil {
		t.Error("LockCommandQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockCommandQueuePushLeftPopRight(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.PushLeft(tail)

	if q.PopRight() != head {
		t.Error("LockCommandQueue PushLeft PopRight Test Fail")
		return
	}

	q.PopRight()
	if q.PopRight() != nil {
		t.Error("LockCommandQueue PushLeft Pop Test Nil Fail")
		return
	}
}

func TestLockCommandQueueHead(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Head() != head {
		t.Error("LockCommandQueue Head Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Head() != nil {
		t.Error("LockCommandQueue Head Test Nil Fail")
		return
	}
}

func TestLockCommandQueueTail(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Tail() != tail {
		t.Error("LockCommandQueue Tail Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	if q.Tail() != nil {
		t.Error("LockCommandQueue Tail Test Nil Fail")
		return
	}
}

func TestLockCommandQueueLen(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	_ = q.Push(head)
	_ = q.Push(tail)

	if q.Len() != 2 {
		t.Error("LockCommandQueue Len Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	for i := 0; i < 10; i++ {
		_ = q.Push(head)
	}
	if q.Len() != 10 {
		t.Error("LockCommandQueue Len Test Many Fail")
		return
	}

	for i := 0; i < 10; i++ {
		q.Pop()
	}
	if q.Len() != 0 {
		t.Error("LockCommandQueue Len Test Zero Fail")
		return
	}
}

func TestLockCommandQueueReset(t *testing.T) {
	l := &protocol.LockCommand{}
	q := NewLockCommandQueue(2, 3, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockCommandQueue Reset queue_size Fail")
		return
	}

	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}
}

func TestLockCommandQueueRellac(t *testing.T) {
	l := &Lock{}
	q := NewLockQueue(2, 6, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	rellacNodeIndex := q.nodeIndex
	_ = q.Rellac()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockCommandQueue Rellac queue_size Fail")
		return
	}
	if q.nodeIndex != rellacNodeIndex {
		t.Error("LockCommandQueue Rellac nodeIndex Fail")
		return
	}
	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Rellac()
	if q.nodeIndex == rellacNodeIndex {
		t.Errorf("LockCommandQueue Rellac nodeIndex Fail %d %d", q.nodeIndex, rellacNodeIndex)
		return
	}
}

func TestLockCommandQueueResize(t *testing.T) {
	l := &protocol.LockCommand{}
	q := NewLockCommandQueue(2, 3, 4)
	qlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 60 {
			if q.Push(l) == nil {
				qlen++
			}
		} else {
			if q.Pop() != nil {
				qlen--
			}
		}
	}

	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}
	ht := q.tailNodeIndex - q.headNodeIndex

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != ht {
		t.Error("LockCommandQueue Node Index Fail")
		return
	}

	nodeIndex := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockCommandQueue Resize Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	_ = q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	if q.headNodeIndex != q.baseNodeSize || q.tailNodeIndex-q.headNodeIndex != 0 {
		t.Error("LockCommandQueue Empty Node Index Fail")
		return
	}

	nodeIndex = 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		nodeIndex = i
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", nodeIndex, q.nodeIndex)
		return
	}

	_ = q.Reset()
	nodeSize := 0
	for _, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Error("LockCommandQueue Resize queue_size Fail")
		return
	}
}

func TestLockCommandQueueRestructuring(t *testing.T) {
	l := &protocol.LockCommand{}
	q := NewLockCommandQueue(2, 6, 4)
	qlen := 0
	rlen := 0

	for i := 0; i < 1000; i++ {
		if rand.Intn(100) < 70 {
			if q.Push(l) == nil {
				qlen++
				rlen++
			}
		} else {
			if q.tailQueueIndex > 0 && q.tailQueue[q.tailQueueIndex-1] != nil {
				q.tailQueue[q.tailQueueIndex-1] = nil
				rlen--
			}
		}
	}

	lastL := &protocol.LockCommand{}
	if q.Push(lastL) == nil {
		qlen++
		rlen++
	}

	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	_ = q.Restructuring()
	if q.Len() != int32(rlen) {
		t.Error("LockCommandQueue Restructuring Len Fail")
		return
	}

	if q.PopRight() != lastL {
		t.Error("LockCommandQueue Restructuring Value Fail")
		return
	}
	rlen--

	nodeSize := 0
	nodeIndex := 0
	for i, nodeQueue := range q.queues {
		if nodeQueue == nil {
			break
		}
		nodeSize++
		nodeIndex = i
	}
	if q.queueSize != q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)) {
		t.Errorf("LockCommandQueue Restructuring queue_size Fail %d %d", q.queueSize, q.baseQueueSize*int32(uint32(1)<<uint32(nodeSize-1)))
		return
	}
	if nodeIndex != int(q.nodeIndex) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", nodeSize, q.nodeIndex)
		return
	}

	for q.Pop() != nil {
		rlen--
	}

	if rlen != 0 {
		t.Error("LockCommandQueue Restructuring Pop Empty Fail")
		return
	}
}

func TestLockCommandQueueIter(t *testing.T) {
	q := NewLockCommandQueue(2, 6, 4)
	for i := 0; i < 100; i++ {
		_ = q.Push(&protocol.LockCommand{})
	}

	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockQueue Pop After Iter Empty Fail")
			return
		}
		for _, lock := range nodeQueues {
			if lock == nil {
				t.Error("LockQueue Push After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) == 0 {
			t.Error("LockQueue Pop After Iter Empty Fail")
			return
		}
		for _, lock := range nodeQueues {
			if lock == nil {
				t.Error("LockQueue Pop After Iter Fail")
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		_ = q.Pop()
	}
	for i := range q.IterNodes() {
		nodeQueues := q.IterNodeQueues(int32(i))
		if len(nodeQueues) != 0 {
			t.Error("LockQueue Pop After Iter Not Empty Fail")
			return
		}
	}
}
