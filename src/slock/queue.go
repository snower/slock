package slock

type LockQueue struct {
    queues [][]*Lock
    node_queue_sizes []int
    base_node_size int
    node_size int
    shrink_node_size int
    base_queue_size int
    queue_size int
    head_node_index int
    tail_node_index int
    head_queue_index int
    tail_queue_index int
}

func NewLockQueue(node_size int, queue_size int) *LockQueue {
    queues := make([][]*Lock, node_size)
    node_queue_sizes := make([]int, node_size)

    queues[0] = make([]*Lock, queue_size)
    node_queue_sizes[0] = queue_size

    return &LockQueue{queues, node_queue_sizes,node_size, node_size, 0, queue_size, queue_size, 0, 0, 0, 0}
}

func (self *LockQueue) Push(lock *Lock) {
    self.queues[self.tail_node_index][self.tail_queue_index] = lock
    self.tail_queue_index++
    if self.tail_queue_index >= self.node_queue_sizes[self.tail_node_index]{
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
    }
}

func (self *LockQueue) Pop() *Lock{
    if self.tail_node_index <= self.head_node_index && self.tail_queue_index <= self.head_queue_index {
        return nil
    }

    lock := self.queues[self.head_node_index][self.head_queue_index]
    self.queues[self.head_node_index][self.head_queue_index] = nil
    self.head_queue_index++

    if self.head_queue_index >= self.node_queue_sizes[self.head_node_index] {
        self.head_queue_index = 0
        self.head_node_index++
    }
    return lock
}

func (self *LockQueue) Head() *Lock{
    if self.tail_node_index <= self.head_node_index && self.tail_queue_index <= self.head_queue_index {
        return nil
    }

    return self.queues[self.head_node_index][self.head_queue_index]
}

func (self *LockQueue) Tail() *Lock{
    if self.tail_node_index <= self.head_node_index && self.tail_queue_index <= self.head_queue_index {
        return nil
    }

    return self.queues[self.tail_node_index][self.tail_queue_index]
}

func (self *LockQueue) Shrink(size int) int{
    if size == 0 {
        size = self.node_queue_sizes[self.head_node_index]
    }

    shrink_size := 0
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
    self.tail_node_index = 0
    self.tail_queue_index = 0
    return nil
}

func (self *LockQueue) Len() int{
    len := self.node_queue_sizes[self.head_node_index] - self.head_queue_index
    for i := self.head_node_index + 1; i < self.tail_node_index; i++{
        len += self.node_queue_sizes[self.head_node_index]
    }
    len += self.tail_queue_index
    return len
}