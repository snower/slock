package slock

import (
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
    for i:= 0; i < 10; i++{
        q.Push(head)
    }
    if q.Len() != 10 {
        t.Error("LockQueue Len Test Many Fail")
        return
    }

    for i:= 0; i < 10; i++{
        q.Pop()
    }
    if q.Len() != 0 {
        t.Error("LockQueue Len Test Zero Fail")
        return
    }
}

func TestLockCommandQueuePushPop(t *testing.T) {
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

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
    head := &LockCommand{}
    tail := &LockCommand{}

    q := NewLockCommandQueue(1, 3, 4)
    q.Push(head)
    q.Push(tail)

    if q.Len() != 2 {
        t.Error("LockCommandQueue Len Test Fail")
        return
    }

    q.Pop()
    q.Pop()
    for i:= 0; i < 10; i++{
        q.Push(head)
    }
    if q.Len() != 10 {
        t.Error("LockCommandQueue Len Test Many Fail")
        return
    }

    for i:= 0; i < 10; i++{
        q.Pop()
    }
    if q.Len() != 0 {
        t.Error("LockCommandQueue Len Test Zero Fail")
        return
    }
}