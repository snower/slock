package slock

import (
    "errors"
)

type LockQueue struct {
    head_queue_index int32
    head_queue_size int32
    head_queue []*Lock


    tail_queue_index int32
    tail_queue_size int32
    tail_queue []*Lock

    head_node_index int32
    tail_node_index int32

    queues [][]*Lock
    node_queue_sizes []int32
    base_node_size int32
    node_size int32
    shrink_node_size int32
    base_queue_size int32
    queue_size int32
}

func NewLockQueue(base_node_size int32, node_size int32, queue_size int32) *LockQueue {
    queues := make([][]*Lock, node_size)
    node_queue_sizes := make([]int32, node_size)

    queues[0] = make([]*Lock, queue_size)
    node_queue_sizes[0] = queue_size

    return &LockQueue{0, queue_size, queues[0], 0,
        queue_size, queues[0], 0, 0,
    queues, node_queue_sizes,base_node_size, node_size,
    0, queue_size, queue_size}
}

func (self *LockQueue) Push(lock *Lock) error {
    self.tail_queue[self.tail_queue_index] = lock
    self.tail_queue_index++

    if self.tail_queue_index >= self.tail_queue_size{
        self.tail_queue_index = 0
        self.tail_node_index++

        if self.tail_node_index >= self.node_size {
            self.queue_size = self.queue_size * 2
            self.queues = append(self.queues, make([]*Lock, self.queue_size))
            self.node_queue_sizes = append(self.node_queue_sizes, self.queue_size)
            self.node_size++
        } else if self.queues[self.tail_node_index] == nil {
            self.queue_size = self.queue_size * 2
            self.queues[self.tail_node_index] = make([]*Lock, self.queue_size)
            self.node_queue_sizes[self.tail_node_index] = self.queue_size
        }

        self.tail_queue = self.queues[self.tail_node_index]
        self.tail_queue_size = self.node_queue_sizes[self.tail_node_index]
    }

    return nil
}

func (self *LockQueue) PushLeft(lock *Lock) error{
    if self.head_node_index <= 0 && self.head_queue_index <= 0 {
        return errors.New("full")
    }

    self.head_queue_index--
    if self.head_queue_index < 0 {
        self.head_node_index--
        if self.head_node_index < 0 {
            self.head_node_index = 0
            self.head_queue_index = 0
            return nil
        }

        self.head_queue_index = self.node_queue_sizes[self.head_node_index] - 1
        self.head_queue = self.queues[self.head_node_index]
        self.head_queue_size = self.node_queue_sizes[self.head_node_index]
    }

    self.head_queue[self.head_queue_index] = lock

    return nil
}

func (self *LockQueue) Pop() *Lock{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    lock := self.head_queue[self.head_queue_index]
    self.head_queue[self.head_queue_index] = nil
    self.head_queue_index++

    if self.head_queue_index >= self.head_queue_size {
        self.head_queue_index = 0
        self.head_node_index++
        self.head_queue = self.queues[self.head_node_index]
        self.head_queue_size = self.node_queue_sizes[self.head_node_index]
    }
    return lock
}

func (self *LockQueue) PopRight() *Lock{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    self.tail_queue_index--
    if self.tail_queue_index < 0 {
        self.tail_node_index--
        if self.tail_node_index < 0 {
            self.tail_queue_index = 0
            self.tail_node_index = 0
            return nil;
        }

        self.tail_queue_index = self.node_queue_sizes[self.tail_node_index] - 1
        self.tail_queue = self.queues[self.tail_node_index]
        self.tail_queue_size = self.node_queue_sizes[self.tail_node_index]
    }
    
    lock := self.tail_queue[self.tail_queue_index]
    self.tail_queue[self.tail_queue_index] = nil
    return lock
}

func (self *LockQueue) Head() *Lock{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    return self.head_queue[self.head_queue_index]
}

func (self *LockQueue) Tail() *Lock{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    return self.tail_queue[self.tail_queue_index]
}

func (self *LockQueue) Shrink(size int32) int32{
    if size == 0 {
        size = self.node_queue_sizes[self.head_node_index]
    }

    shrink_size := int32(0)
    for size >= self.node_queue_sizes[self.head_node_index] {
        if self.shrink_node_size >= self.node_size {
            break
        }

        size -= self.node_queue_sizes[self.head_node_index]
        shrink_size += self.node_queue_sizes[self.head_node_index]
        self.queues[self.shrink_node_size] = nil
        self.node_queue_sizes[self.head_node_index] = 0
        self.shrink_node_size++
    }
    return shrink_size
}

func (self *LockQueue) Reset() error{
    for ; self.tail_node_index > self.base_node_size; {
        if self.queue_size > self.base_queue_size {
            self.queue_size = self.queue_size / 2
        }
        self.queues[self.tail_node_index] = nil
        self.node_queue_sizes[self.tail_node_index] = 0
        self.tail_node_index--
    }

    self.head_node_index=0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]
    return nil
}

func (self *LockQueue) Len() int32{
    queue_len := self.node_queue_sizes[self.head_node_index] - self.head_queue_index
    for i := self.head_node_index + 1; i < self.tail_node_index; i++{
        queue_len += self.node_queue_sizes[self.head_node_index]
    }
    queue_len += self.tail_queue_index
    return queue_len
}