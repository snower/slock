package server

import (
	"github.com/snower/slock/protocol"
	"math/rand"
	"testing"
)

func TestLockQueuePushPop(t *testing.T) {
	head := &Lock{}
	tail := &Lock{}

	q := NewLockQueue(1, 3, 4)
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.PushLeft(tail)

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
	q.Push(head)
	q.PushLeft(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

	if q.Len() != 2 {
		t.Error("LockQueue Len Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	for i := 0; i < 10; i++ {
		q.Push(head)
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

	q.Reset()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Error("LockQueue Reset queue_size Fail")
		return
	}
	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
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

	q.Rellac()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Error("LockQueue Reset queue_size Fail")
		return
	}
	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
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
	ht := q.tail_node_index - q.head_node_index

	q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	if q.head_node_index != q.base_node_size || q.tail_node_index-q.head_node_index != ht {
		t.Error("LockQueue Node Index Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	if q.head_node_index != q.base_node_size || q.tail_node_index-q.head_node_index != 0 {
		t.Error("LockQueue Empty Node Index Fail")
		return
	}

	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
		return
	}

	q.Reset()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
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
			if q.tail_queue_index > 0 && q.tail_queue[q.tail_queue_index-1] != nil {
				q.tail_queue[q.tail_queue_index-1] = nil
				rlen--
			}
		}
	}

	last_l := &Lock{}
	if q.Push(last_l) == nil {
		qlen++
		rlen++
	}

	if q.Len() != int32(qlen) {
		t.Error("LockQueue Len Fail")
		return
	}

	q.Restructuring()
	if q.Len() != int32(rlen) {
		t.Error("LockQueue Restructuring Len Fail")
		return
	}

	if q.PopRight() != last_l {
		t.Error("LockQueue Restructuring Value Fail")
		return
	}
	rlen--

	node_size := 0
	node_index := 0
	for i, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
		node_index = i
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Errorf("LockQueue Restructuring queue_size Fail %d %d", q.queue_size, q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)))
		return
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockQueue Empty Node_Index Fail %d %d", node_size, q.node_index)
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

func TestLockCommandQueuePushPop(t *testing.T) {
	head := &protocol.LockCommand{}
	tail := &protocol.LockCommand{}

	q := NewLockCommandQueue(1, 3, 4)
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.PushLeft(tail)

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
	q.Push(head)
	q.PushLeft(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

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
	q.Push(head)
	q.Push(tail)

	if q.Len() != 2 {
		t.Error("LockCommandQueue Len Test Fail")
		return
	}

	q.Pop()
	q.Pop()
	for i := 0; i < 10; i++ {
		q.Push(head)
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

	q.Reset()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Error("LockCommandQueue Reset queue_size Fail")
		return
	}

	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
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

	q.Rellac()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Error("LockCommandQueue Reset queue_size Fail")
		return
	}

	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
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
	ht := q.tail_node_index - q.head_node_index

	q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	if q.head_node_index != q.base_node_size || q.tail_node_index-q.head_node_index != ht {
		t.Error("LockCommandQueue Node Index Fail")
		return
	}

	for q.Pop() != nil {
		qlen--
	}

	q.Resize()
	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	if q.head_node_index != q.base_node_size || q.tail_node_index-q.head_node_index != 0 {
		t.Error("LockCommandQueue Empty Node Index Fail")
		return
	}

	node_index := 0
	for i, node := range q.queues {
		if node == nil {
			break
		}
		node_index = i
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", node_index, q.node_index)
		return
	}

	q.Reset()
	node_size := 0
	for _, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Error("LockCommandQueue Resize queue_size Fail")
		return
	}
}

func TestLockCommandQueueRestructuring(t *testing.T) {
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
			if q.tail_queue_index > 0 && q.tail_queue[q.tail_queue_index-1] != nil {
				q.tail_queue[q.tail_queue_index-1] = nil
				rlen--
			}
		}
	}

	last_l := &Lock{}
	if q.Push(last_l) == nil {
		qlen++
		rlen++
	}

	if q.Len() != int32(qlen) {
		t.Error("LockCommandQueue Len Fail")
		return
	}

	q.Restructuring()
	if q.Len() != int32(rlen) {
		t.Error("LockCommandQueue Restructuring Len Fail")
		return
	}

	if q.PopRight() != last_l {
		t.Error("LockCommandQueue Restructuring Value Fail")
		return
	}
	rlen--

	node_size := 0
	node_index := 0
	for i, node_queue := range q.queues {
		if node_queue == nil {
			break
		}
		node_size++
		node_index = i
	}
	if q.queue_size != q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)) {
		t.Errorf("LockCommandQueue Restructuring queue_size Fail %d %d", q.queue_size, q.base_queue_size*int32(uint32(1)<<uint32(node_size-1)))
		return
	}
	if node_index != int(q.node_index) {
		t.Errorf("LockCommandQueue Empty Node_Index Fail %d %d", node_size, q.node_index)
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
