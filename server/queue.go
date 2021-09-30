package server

import (
	"errors"
	"github.com/snower/slock/protocol"
)

type LockQueue struct {
	headQueueIndex int32
	headQueueSize  int32
	headQueue      []*Lock

	tailQueueIndex int32
	tailQueueSize  int32
	tailQueue      []*Lock

	headNodeIndex int32
	tailNodeIndex int32

	queues         [][]*Lock
	nodeQueueSizes []int32
	baseNodeSize   int32
	nodeIndex      int32
	nodeSize       int32
	shrinkNodeSize int32
	baseQueueSize  int32
	queueSize      int32
}

func NewLockQueue(baseNodeSize int32, nodeSize int32, queueSize int32) *LockQueue {
	queues := make([][]*Lock, nodeSize)
	nodeQueueSizes := make([]int32, nodeSize)

	queues[0] = make([]*Lock, queueSize)
	nodeQueueSizes[0] = queueSize

	return &LockQueue{0, queueSize, queues[0], 0,
		queueSize, queues[0], 0, 0,
		queues, nodeQueueSizes, baseNodeSize, 0, nodeSize,
		0, queueSize, queueSize}
}

func (self *LockQueue) Push(lock *Lock) error {
	self.tailQueue[self.tailQueueIndex] = lock
	self.tailQueueIndex++

	if self.tailQueueIndex >= self.tailQueueSize {
		self.tailNodeIndex++
		self.tailQueueIndex = 0

		if self.tailNodeIndex >= self.nodeSize {
			self.queueSize = self.queueSize * 2
			if self.queueSize > QUEUE_MAX_MALLOC_SIZE {
				self.queueSize = QUEUE_MAX_MALLOC_SIZE
			}

			self.queues = append(self.queues, make([]*Lock, self.queueSize))
			self.nodeQueueSizes = append(self.nodeQueueSizes, self.queueSize)
			self.nodeIndex++
			self.nodeSize++
		} else if self.queues[self.tailNodeIndex] == nil {
			self.queueSize = self.queueSize * 2
			if self.queueSize > QUEUE_MAX_MALLOC_SIZE {
				self.queueSize = QUEUE_MAX_MALLOC_SIZE
			}

			self.queues[self.tailNodeIndex] = make([]*Lock, self.queueSize)
			self.nodeQueueSizes[self.tailNodeIndex] = self.queueSize
			self.nodeIndex++
		}

		self.tailQueue = self.queues[self.tailNodeIndex]
		self.tailQueueSize = self.nodeQueueSizes[self.tailNodeIndex]
	}

	return nil
}

func (self *LockQueue) PushLeft(lock *Lock) error {
	if self.headNodeIndex <= 0 && self.headQueueIndex <= 0 {
		return errors.New("full")
	}

	self.headQueueIndex--
	if self.headQueueIndex < 0 {
		self.headNodeIndex--
		self.headQueueIndex = self.nodeQueueSizes[self.headNodeIndex] - 1
		self.headQueue = self.queues[self.headNodeIndex]
		self.headQueueSize = self.nodeQueueSizes[self.headNodeIndex]
	}

	self.headQueue[self.headQueueIndex] = lock

	return nil
}

func (self *LockQueue) Pop() *Lock {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	lock := self.headQueue[self.headQueueIndex]
	self.headQueue[self.headQueueIndex] = nil
	self.headQueueIndex++

	if self.headQueueIndex >= self.headQueueSize {
		self.headNodeIndex++
		self.headQueueIndex = 0
		self.headQueue = self.queues[self.headNodeIndex]
		self.headQueueSize = self.nodeQueueSizes[self.headNodeIndex]
	}
	return lock
}

func (self *LockQueue) PopRight() *Lock {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	self.tailQueueIndex--
	if self.tailQueueIndex < 0 {
		self.tailNodeIndex--
		self.tailQueueIndex = self.nodeQueueSizes[self.tailNodeIndex] - 1
		self.tailQueue = self.queues[self.tailNodeIndex]
		self.tailQueueSize = self.nodeQueueSizes[self.tailNodeIndex]
	}

	lock := self.tailQueue[self.tailQueueIndex]
	self.tailQueue[self.tailQueueIndex] = nil
	return lock
}

func (self *LockQueue) Head() *Lock {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	return self.headQueue[self.headQueueIndex]
}

func (self *LockQueue) Tail() *Lock {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	return self.tailQueue[self.tailQueueIndex-1]
}

func (self *LockQueue) Shrink(size int32) int32 {
	if size == 0 {
		size = self.nodeQueueSizes[self.headNodeIndex]
	}

	shrinkSize := int32(0)
	for size >= self.nodeQueueSizes[self.headNodeIndex] {
		if self.shrinkNodeSize >= self.nodeSize {
			break
		}

		size -= self.nodeQueueSizes[self.headNodeIndex]
		shrinkSize += self.nodeQueueSizes[self.headNodeIndex]
		self.queues[self.shrinkNodeSize] = nil
		self.nodeQueueSizes[self.headNodeIndex] = 0
		self.shrinkNodeSize++
	}
	return shrinkSize
}

func (self *LockQueue) Reset() error {
	for self.nodeIndex >= self.baseNodeSize {
		self.queues[self.nodeIndex] = nil
		self.nodeQueueSizes[self.nodeIndex] = 0
		self.nodeIndex--
	}

	self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]
	return nil
}

func (self *LockQueue) Rellac() error {
	if self.nodeIndex > self.tailNodeIndex {
		baseNodeSize := self.tailNodeIndex + (self.nodeIndex-self.tailNodeIndex)/2
		if baseNodeSize < self.baseNodeSize {
			baseNodeSize = self.baseNodeSize
		}

		for self.nodeIndex >= baseNodeSize {
			self.queues[self.nodeIndex] = nil
			self.nodeQueueSizes[self.nodeIndex] = 0
			self.nodeIndex--
		}
		self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	}

	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]
	return nil
}

func (self *LockQueue) Resize() error {
	if self.headNodeIndex <= self.baseNodeSize {
		return nil
	}

	for i := self.baseNodeSize; i < self.headNodeIndex; i++ {
		self.queues[i] = nil
		self.nodeQueueSizes[i] = 0
	}

	self.nodeIndex = self.baseNodeSize - 1
	moveIndex := self.headNodeIndex - self.baseNodeSize
	for i := self.headNodeIndex; i <= self.tailNodeIndex; i++ {
		self.queues[i-moveIndex] = self.queues[i]
		self.nodeQueueSizes[i-moveIndex] = self.nodeQueueSizes[i]
		self.queues[i] = nil
		self.nodeQueueSizes[i] = 0
		self.nodeIndex++
	}
	self.queueSize = self.baseQueueSize * int32(uint32(1)<<uint32(self.tailNodeIndex))
	self.headNodeIndex -= moveIndex
	self.tailNodeIndex -= moveIndex
	return nil
}

func (self *LockQueue) Restructuring() error {
	tailNodeIndex, tailQueueIndex := self.tailNodeIndex, self.tailQueueIndex
	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]

	for j := int32(0); j < tailNodeIndex; j++ {
		for k := int32(0); k < self.nodeQueueSizes[j]; k++ {
			lock := self.queues[j][k]
			if lock != nil {
				self.queues[j][k] = nil
				_ = self.Push(lock)
			}
		}
	}

	for k := int32(0); k < tailQueueIndex; k++ {
		lock := self.queues[tailNodeIndex][k]
		if lock != nil {
			self.queues[tailNodeIndex][k] = nil
			_ = self.Push(lock)
		}
	}

	for tailNodeIndex > self.tailNodeIndex+1 {
		self.queues[tailNodeIndex] = nil
		self.nodeQueueSizes[tailNodeIndex] = 0
		self.nodeIndex--
		tailNodeIndex--
	}
	self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	return nil
}

func (self *LockQueue) Len() int32 {
	if self.tailNodeIndex <= self.headNodeIndex {
		return self.tailQueueIndex - self.headQueueIndex
	}

	queueLen := self.nodeQueueSizes[self.headNodeIndex] - self.headQueueIndex
	for i := self.headNodeIndex + 1; i < self.tailNodeIndex; i++ {
		queueLen += self.nodeQueueSizes[i]
	}
	queueLen += self.tailQueueIndex
	return queueLen
}

func (self *LockQueue) IterNodes() [][]*Lock {
	return self.queues[self.headNodeIndex : self.tailNodeIndex+1]
}

func (self *LockQueue) IterNodeQueues(nodeIndex int32) []*Lock {
	if nodeIndex == self.headNodeIndex {
		if nodeIndex == self.tailNodeIndex {
			return self.queues[nodeIndex][self.headQueueIndex:self.tailQueueIndex]
		}
		return self.queues[nodeIndex][self.headQueueIndex:self.nodeQueueSizes[nodeIndex]]
	}

	if nodeIndex == self.tailNodeIndex {
		return self.queues[nodeIndex][:self.tailQueueIndex]
	}
	return self.queues[nodeIndex][:self.nodeQueueSizes[nodeIndex]]
}

type LockCommandQueue struct {
	headQueueIndex int32
	headQueueSize  int32
	headQueue      []*protocol.LockCommand

	tailQueueIndex int32
	tailQueueSize  int32
	tailQueue      []*protocol.LockCommand

	headNodeIndex int32
	tailNodeIndex int32

	queues         [][]*protocol.LockCommand
	nodeQueueSizes []int32
	baseNodeSize   int32
	nodeIndex      int32
	nodeSize       int32
	shrinkNodeSize int32
	baseQueueSize  int32
	queueSize      int32
}

func NewLockCommandQueue(baseNodeSize int32, nodeSize int32, queueSize int32) *LockCommandQueue {
	queues := make([][]*protocol.LockCommand, nodeSize)
	nodeQueueSizes := make([]int32, nodeSize)

	queues[0] = make([]*protocol.LockCommand, queueSize)
	nodeQueueSizes[0] = queueSize

	return &LockCommandQueue{0, queueSize, queues[0], 0,
		queueSize, queues[0], 0, 0,
		queues, nodeQueueSizes, baseNodeSize, 0, nodeSize,
		0, queueSize, queueSize}
}

func (self *LockCommandQueue) Push(lock *protocol.LockCommand) error {
	self.tailQueue[self.tailQueueIndex] = lock
	self.tailQueueIndex++

	if self.tailQueueIndex >= self.tailQueueSize {
		self.tailNodeIndex++
		self.tailQueueIndex = 0

		if self.tailNodeIndex >= self.nodeSize {
			self.queueSize = self.queueSize * 2
			if self.queueSize > QUEUE_MAX_MALLOC_SIZE {
				self.queueSize = QUEUE_MAX_MALLOC_SIZE
			}
			self.queues = append(self.queues, make([]*protocol.LockCommand, self.queueSize))
			self.nodeQueueSizes = append(self.nodeQueueSizes, self.queueSize)
			self.nodeIndex++
			self.nodeSize++
		} else if self.queues[self.tailNodeIndex] == nil {
			self.queueSize = self.queueSize * 2
			if self.queueSize > QUEUE_MAX_MALLOC_SIZE {
				self.queueSize = QUEUE_MAX_MALLOC_SIZE
			}
			self.queues[self.tailNodeIndex] = make([]*protocol.LockCommand, self.queueSize)
			self.nodeQueueSizes[self.tailNodeIndex] = self.queueSize
			self.nodeIndex++
		}

		self.tailQueue = self.queues[self.tailNodeIndex]
		self.tailQueueSize = self.nodeQueueSizes[self.tailNodeIndex]
	}

	return nil
}

func (self *LockCommandQueue) PushLeft(lock *protocol.LockCommand) error {
	if self.headNodeIndex <= 0 && self.headQueueIndex <= 0 {
		return errors.New("full")
	}

	self.headQueueIndex--
	if self.headQueueIndex < 0 {
		self.headNodeIndex--
		self.headQueueIndex = self.nodeQueueSizes[self.headNodeIndex] - 1
		self.headQueue = self.queues[self.headNodeIndex]
		self.headQueueSize = self.nodeQueueSizes[self.headNodeIndex]
	}

	self.headQueue[self.headQueueIndex] = lock

	return nil
}

func (self *LockCommandQueue) Pop() *protocol.LockCommand {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	lock := self.headQueue[self.headQueueIndex]
	self.headQueue[self.headQueueIndex] = nil
	self.headQueueIndex++

	if self.headQueueIndex >= self.headQueueSize {
		self.headNodeIndex++
		self.headQueueIndex = 0
		self.headQueue = self.queues[self.headNodeIndex]
		self.headQueueSize = self.nodeQueueSizes[self.headNodeIndex]
	}
	return lock
}

func (self *LockCommandQueue) PopRight() *protocol.LockCommand {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	self.tailQueueIndex--
	if self.tailQueueIndex < 0 {
		self.tailNodeIndex--
		self.tailQueueIndex = self.nodeQueueSizes[self.tailNodeIndex] - 1
		self.tailQueue = self.queues[self.tailNodeIndex]
		self.tailQueueSize = self.nodeQueueSizes[self.tailNodeIndex]
	}

	lock := self.tailQueue[self.tailQueueIndex]
	self.tailQueue[self.tailQueueIndex] = nil
	return lock
}

func (self *LockCommandQueue) Head() *protocol.LockCommand {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	return self.headQueue[self.headQueueIndex]
}

func (self *LockCommandQueue) Tail() *protocol.LockCommand {
	if self.tailQueueIndex <= self.headQueueIndex && self.tailNodeIndex <= self.headNodeIndex {
		return nil
	}

	return self.tailQueue[self.tailQueueIndex-1]
}

func (self *LockCommandQueue) Shrink(size int32) int32 {
	if size == 0 {
		size = self.nodeQueueSizes[self.headNodeIndex]
	}

	shrinkSize := int32(0)
	for size >= self.nodeQueueSizes[self.headNodeIndex] {
		if self.shrinkNodeSize >= self.nodeSize {
			break
		}

		size -= self.nodeQueueSizes[self.headNodeIndex]
		shrinkSize += self.nodeQueueSizes[self.headNodeIndex]
		self.queues[self.shrinkNodeSize] = nil
		self.nodeQueueSizes[self.headNodeIndex] = 0
		self.shrinkNodeSize++
	}
	return shrinkSize
}

func (self *LockCommandQueue) Reset() error {
	for self.nodeIndex >= self.baseNodeSize {
		self.queues[self.nodeIndex] = nil
		self.nodeQueueSizes[self.nodeIndex] = 0
		self.nodeIndex--
	}

	self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]
	return nil
}

func (self *LockCommandQueue) Rellac() error {
	if self.nodeIndex > self.tailNodeIndex {
		baseNodeSize := self.tailNodeIndex + (self.nodeIndex-self.tailNodeIndex)/2
		if baseNodeSize < self.baseNodeSize {
			baseNodeSize = self.baseNodeSize
		}

		for self.nodeIndex >= baseNodeSize {
			self.queues[self.nodeIndex] = nil
			self.nodeQueueSizes[self.nodeIndex] = 0
			self.nodeIndex--
		}
		self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	}

	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]
	return nil
}

func (self *LockCommandQueue) Resize() error {
	if self.headNodeIndex <= self.baseNodeSize {
		return nil
	}

	for i := self.baseNodeSize; i < self.headNodeIndex; i++ {
		self.queues[i] = nil
		self.nodeQueueSizes[i] = 0
	}

	self.nodeIndex = self.baseNodeSize - 1
	moveIndex := self.headNodeIndex - self.baseNodeSize
	for i := self.headNodeIndex; i <= self.tailNodeIndex; i++ {
		self.queues[i-moveIndex] = self.queues[i]
		self.nodeQueueSizes[i-moveIndex] = self.nodeQueueSizes[i]
		self.queues[i] = nil
		self.nodeQueueSizes[i] = 0
		self.nodeIndex++
	}
	self.queueSize = self.baseQueueSize * int32(uint32(1)<<uint32(self.tailNodeIndex))
	self.headNodeIndex -= moveIndex
	self.tailNodeIndex -= moveIndex
	return nil
}

func (self *LockCommandQueue) Restructuring() error {
	tailNodeIndex, tailQueueIndex := self.tailNodeIndex, self.tailQueueIndex
	self.headNodeIndex = 0
	self.headQueueIndex = 0
	self.headQueue = self.queues[0]
	self.tailQueue = self.queues[0]
	self.tailNodeIndex = 0
	self.tailQueueIndex = 0
	self.headQueueSize = self.nodeQueueSizes[0]
	self.tailQueueSize = self.nodeQueueSizes[0]

	for j := int32(0); j < tailNodeIndex; j++ {
		for k := int32(0); k < self.nodeQueueSizes[j]; k++ {
			lock := self.queues[j][k]
			if lock != nil {
				self.queues[j][k] = nil
				_ = self.Push(lock)
			}
		}
	}

	for k := int32(0); k < tailQueueIndex; k++ {
		lock := self.queues[tailNodeIndex][k]
		if lock != nil {
			self.queues[tailNodeIndex][k] = nil
			_ = self.Push(lock)
		}
	}

	for tailNodeIndex > self.tailNodeIndex+1 {
		self.queues[tailNodeIndex] = nil
		self.nodeQueueSizes[tailNodeIndex] = 0
		self.nodeIndex--
		tailNodeIndex--
	}
	self.queueSize = self.nodeQueueSizes[self.nodeIndex]
	return nil
}

func (self *LockCommandQueue) Len() int32 {
	if self.tailNodeIndex <= self.headNodeIndex {
		return self.tailQueueIndex - self.headQueueIndex
	}

	queueLen := self.nodeQueueSizes[self.headNodeIndex] - self.headQueueIndex
	for i := self.headNodeIndex + 1; i < self.tailNodeIndex; i++ {
		queueLen += self.nodeQueueSizes[i]
	}
	queueLen += self.tailQueueIndex
	return queueLen
}

func (self *LockCommandQueue) IterNodes() [][]*protocol.LockCommand {
	return self.queues[self.headNodeIndex : self.tailNodeIndex+1]
}

func (self *LockCommandQueue) IterNodeQueues(nodeIndex int32) []*protocol.LockCommand {
	if nodeIndex == self.headNodeIndex {
		if nodeIndex == self.tailNodeIndex {
			return self.queues[nodeIndex][self.headQueueIndex:self.tailQueueIndex]
		}
		return self.queues[nodeIndex][self.headQueueIndex:self.nodeQueueSizes[nodeIndex]]
	}

	if nodeIndex == self.tailNodeIndex {
		return self.queues[nodeIndex][:self.tailQueueIndex]
	}
	return self.queues[nodeIndex][:self.nodeQueueSizes[nodeIndex]]
}
