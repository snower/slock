package server

import (
    "errors"
    "github.com/snower/slock/protocol"
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
    node_index int32
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
    queues, node_queue_sizes,base_node_size, 0, node_size,
    0, queue_size, queue_size}
}

func (self *LockQueue) Push(lock *Lock) error {
    self.tail_queue[self.tail_queue_index] = lock
    self.tail_queue_index++

    if self.tail_queue_index >= self.tail_queue_size{
        self.tail_node_index++
        self.tail_queue_index = 0

        if self.tail_node_index >= self.node_size {
            self.queue_size = self.queue_size * 2
            if self.queue_size > QUEUE_MAX_MALLOC_SIZE {
                self.queue_size = QUEUE_MAX_MALLOC_SIZE
            }

            self.queues = append(self.queues, make([]*Lock, self.queue_size))
            self.node_queue_sizes = append(self.node_queue_sizes, self.queue_size)
            self.node_index++
            self.node_size++
        } else if self.queues[self.tail_node_index] == nil {
            self.queue_size = self.queue_size * 2
            if self.queue_size > QUEUE_MAX_MALLOC_SIZE {
                self.queue_size = QUEUE_MAX_MALLOC_SIZE
            }

            self.queues[self.tail_node_index] = make([]*Lock, self.queue_size)
            self.node_queue_sizes[self.tail_node_index] = self.queue_size
            self.node_index++
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
        self.head_node_index++
        self.head_queue_index = 0
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

    return self.tail_queue[self.tail_queue_index - 1]
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
    for ; self.node_index >= self.base_node_size; {
        self.queues[self.node_index] = nil
        self.node_queue_sizes[self.node_index] = 0
        self.node_index--
    }

    self.queue_size = self.node_queue_sizes[self.node_index]
    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]
    return nil
}

func (self *LockQueue) Rellac() error{
    if self.node_index > self.tail_node_index {
        base_node_size := self.tail_node_index + (self.node_index - self.tail_node_index) / 2
        if base_node_size < self.base_node_size {
            base_node_size = self.base_node_size
        }

        for ; self.node_index >= base_node_size; {
            self.queues[self.node_index] = nil
            self.node_queue_sizes[self.node_index] = 0
            self.node_index--
        }
        self.queue_size = self.node_queue_sizes[self.node_index]
    }

    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]
    return nil
}

func (self *LockQueue) Resize() error {
    if self.head_node_index <= self.base_node_size {
        return nil
    }

    move_index := self.head_node_index - self.base_node_size
    for i := self.head_node_index; i <= self.tail_node_index; i++ {
        self.queues[i - move_index] = self.queues[i]
        self.node_queue_sizes[i - move_index] = self.node_queue_sizes[i]
        self.queues[i] = nil
        self.node_queue_sizes[i] = 0
        self.node_index--
    }
    self.head_node_index -= move_index
    self.tail_node_index -= move_index
    return nil
}

func (self *LockQueue) Restructuring() error {
    tail_node_index, tail_queue_index := self.tail_node_index, self.tail_queue_index
    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]

    for j := int32(0); j < tail_node_index; j++ {
        for k := int32(0); k < self.node_queue_sizes[j]; k++ {
            lock := self.queues[j][k]
            if lock != nil {
                self.queues[j][k] = nil
                self.Push(lock)
            }
        }
    }

    for k := int32(0); k < tail_queue_index; k++ {
        lock := self.queues[tail_node_index][k]
        if lock != nil {
            self.queues[tail_node_index][k] = nil
            self.Push(lock)
        }
    }

    for tail_node_index > self.tail_node_index + 1 {
        self.queues[tail_node_index] = nil
        self.node_queue_sizes[tail_node_index] = 0
        self.node_index--
        tail_node_index--
    }
    self.queue_size = self.node_queue_sizes[self.node_index]
    return nil
}

func (self *LockQueue) Len() int32{
    if self.tail_node_index <= self.head_node_index {
        return self.tail_queue_index - self.head_queue_index
    }

    queue_len := self.node_queue_sizes[self.head_node_index] - self.head_queue_index
    for i := self.head_node_index + 1; i < self.tail_node_index; i++{
        queue_len += self.node_queue_sizes[i]
    }
    queue_len += self.tail_queue_index
    return queue_len
}

func (self *LockQueue) IterNodes() [][]*Lock {
    return self.queues[self.head_node_index: self.tail_node_index+1]
}

func (self *LockQueue) IterNodeQueues(node_index int32) []*Lock{
    if node_index == self.head_node_index {
        if node_index == self.tail_node_index {
            return self.queues[node_index][self.head_queue_index:self.tail_queue_index]
        }
        return self.queues[node_index][self.head_queue_index:self.node_queue_sizes[node_index]]
    }

    if node_index == self.tail_node_index {
        return self.queues[node_index][:self.tail_queue_index]
    }
    return self.queues[node_index][:self.node_queue_sizes[node_index]]
}

type LockCommandQueue struct {
    head_queue_index int32
    head_queue_size int32
    head_queue []*protocol.LockCommand


    tail_queue_index int32
    tail_queue_size int32
    tail_queue []*protocol.LockCommand

    head_node_index int32
    tail_node_index int32

    queues [][]*protocol.LockCommand
    node_queue_sizes []int32
    base_node_size int32
    node_index int32
    node_size int32
    shrink_node_size int32
    base_queue_size int32
    queue_size int32
}

func NewLockCommandQueue(base_node_size int32, node_size int32, queue_size int32) *LockCommandQueue {
    queues := make([][]*protocol.LockCommand, node_size)
    node_queue_sizes := make([]int32, node_size)

    queues[0] = make([]*protocol.LockCommand, queue_size)
    node_queue_sizes[0] = queue_size

    return &LockCommandQueue{0, queue_size, queues[0], 0,
        queue_size, queues[0], 0, 0,
        queues, node_queue_sizes,base_node_size, 0, node_size,
        0, queue_size, queue_size}
}

func (self *LockCommandQueue) Push(lock *protocol.LockCommand) error {
    self.tail_queue[self.tail_queue_index] = lock
    self.tail_queue_index++

    if self.tail_queue_index >= self.tail_queue_size{
        self.tail_node_index++
        self.tail_queue_index = 0

        if self.tail_node_index >= self.node_size {
            self.queue_size = self.queue_size * 2
            if self.queue_size > QUEUE_MAX_MALLOC_SIZE {
                self.queue_size = QUEUE_MAX_MALLOC_SIZE
            }
            self.queues = append(self.queues, make([]*protocol.LockCommand, self.queue_size))
            self.node_queue_sizes = append(self.node_queue_sizes, self.queue_size)
            self.node_index++
            self.node_size++
        } else if self.queues[self.tail_node_index] == nil {
            self.queue_size = self.queue_size * 2
            if self.queue_size > QUEUE_MAX_MALLOC_SIZE {
                self.queue_size = QUEUE_MAX_MALLOC_SIZE
            }
            self.queues[self.tail_node_index] = make([]*protocol.LockCommand, self.queue_size)
            self.node_queue_sizes[self.tail_node_index] = self.queue_size
            self.node_index++
        }

        self.tail_queue = self.queues[self.tail_node_index]
        self.tail_queue_size = self.node_queue_sizes[self.tail_node_index]
    }

    return nil
}

func (self *LockCommandQueue) PushLeft(lock *protocol.LockCommand) error{
    if self.head_node_index <= 0 && self.head_queue_index <= 0 {
        return errors.New("full")
    }

    self.head_queue_index--
    if self.head_queue_index < 0 {
        self.head_node_index--
        self.head_queue_index = self.node_queue_sizes[self.head_node_index] - 1
        self.head_queue = self.queues[self.head_node_index]
        self.head_queue_size = self.node_queue_sizes[self.head_node_index]
    }

    self.head_queue[self.head_queue_index] = lock

    return nil
}

func (self *LockCommandQueue) Pop() *protocol.LockCommand{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    lock := self.head_queue[self.head_queue_index]
    self.head_queue[self.head_queue_index] = nil
    self.head_queue_index++

    if self.head_queue_index >= self.head_queue_size {
        self.head_node_index++
        self.head_queue_index = 0
        self.head_queue = self.queues[self.head_node_index]
        self.head_queue_size = self.node_queue_sizes[self.head_node_index]
    }
    return lock
}

func (self *LockCommandQueue) PopRight() *protocol.LockCommand{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    self.tail_queue_index--
    if self.tail_queue_index < 0 {
        self.tail_node_index--
        self.tail_queue_index = self.node_queue_sizes[self.tail_node_index] - 1
        self.tail_queue = self.queues[self.tail_node_index]
        self.tail_queue_size = self.node_queue_sizes[self.tail_node_index]
    }

    lock := self.tail_queue[self.tail_queue_index]
    self.tail_queue[self.tail_queue_index] = nil
    return lock
}

func (self *LockCommandQueue) Head() *protocol.LockCommand{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    return self.head_queue[self.head_queue_index]
}

func (self *LockCommandQueue) Tail() *protocol.LockCommand{
    if self.tail_queue_index <= self.head_queue_index && self.tail_node_index <= self.head_node_index {
        return nil
    }

    return self.tail_queue[self.tail_queue_index - 1]
}

func (self *LockCommandQueue) Shrink(size int32) int32{
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

func (self *LockCommandQueue) Reset() error{
    for ; self.node_index >= self.base_node_size; {
        self.queues[self.node_index] = nil
        self.node_queue_sizes[self.node_index] = 0
        self.node_index--
    }

    self.queue_size = self.node_queue_sizes[self.node_index]
    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]
    return nil
}

func (self *LockCommandQueue) Rellac() error{
    if self.node_index > self.tail_node_index {
        base_node_size := self.tail_node_index + (self.node_index - self.tail_node_index) / 2
        if base_node_size < self.base_node_size {
            base_node_size = self.base_node_size
        }

        for ; self.node_index >= base_node_size; {
            self.queues[self.node_index] = nil
            self.node_queue_sizes[self.node_index] = 0
            self.node_index--
        }
        self.queue_size = self.node_queue_sizes[self.node_index]
    }

    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]
    return nil
}

func (self *LockCommandQueue) Resize() error {
    if self.head_node_index <= self.base_node_size {
        return nil
    }

    move_index := self.head_node_index - self.base_node_size
    for i := self.head_node_index; i <= self.tail_node_index; i++ {
        self.queues[i - move_index] = self.queues[i]
        self.node_queue_sizes[i - move_index] = self.node_queue_sizes[i]
        self.queues[i] = nil
        self.node_queue_sizes[i] = 0
        self.node_index--
    }
    self.head_node_index -= move_index
    self.tail_node_index -= move_index
    return nil
}

func (self *LockCommandQueue) Restructuring() error {
    tail_node_index, tail_queue_index := self.tail_node_index, self.tail_queue_index
    self.head_node_index = 0
    self.head_queue_index = 0
    self.head_queue = self.queues[0]
    self.tail_queue = self.queues[0]
    self.tail_node_index = 0
    self.tail_queue_index = 0
    self.head_queue_size = self.node_queue_sizes[0]
    self.tail_queue_size = self.node_queue_sizes[0]

    for j := int32(0); j < tail_node_index; j++ {
        for k := int32(0); k < self.node_queue_sizes[j]; k++ {
            lock := self.queues[j][k]
            if lock != nil {
                self.queues[j][k] = nil
                self.Push(lock)
            }
        }
    }

    for k := int32(0); k < tail_queue_index; k++ {
        lock := self.queues[tail_node_index][k]
        if lock != nil {
            self.queues[tail_node_index][k] = nil
            self.Push(lock)
        }
    }

    for tail_node_index > self.tail_node_index + 1 {
        self.queues[tail_node_index] = nil
        self.node_queue_sizes[tail_node_index] = 0
        self.node_index--
        tail_node_index--
    }
    self.queue_size = self.node_queue_sizes[self.node_index]
    return nil
}

func (self *LockCommandQueue) Len() int32{
    if self.tail_node_index <= self.head_node_index {
        return self.tail_queue_index - self.head_queue_index
    }

    queue_len := self.node_queue_sizes[self.head_node_index] - self.head_queue_index
    for i := self.head_node_index + 1; i < self.tail_node_index; i++{
        queue_len += self.node_queue_sizes[i]
    }
    queue_len += self.tail_queue_index
    return queue_len
}

func (self *LockCommandQueue) IterNodes() [][]*protocol.LockCommand {
    return self.queues[self.head_node_index: self.tail_node_index+1]
}

func (self *LockCommandQueue) IterNodeQueues(node_index int32) []*protocol.LockCommand {
    if node_index == self.head_node_index {
        if node_index == self.tail_node_index {
            return self.queues[node_index][self.head_queue_index:self.tail_queue_index]
        }
        return self.queues[node_index][self.head_queue_index:self.node_queue_sizes[node_index]]
    }

    if node_index == self.tail_node_index {
        return self.queues[node_index][:self.tail_queue_index]
    }
    return self.queues[node_index][:self.node_queue_sizes[node_index]]
}