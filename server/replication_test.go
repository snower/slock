package server

import (
	"io"
	"testing"
	"time"
)

func TestReplicationBufferQueue_Push(t *testing.T) {
	queue := NewReplicationBufferQueue(nil,1024 * 1024)

	buf := make([]byte, 64)
	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56

	err := queue.Push(buf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Push Error Fail %v", err)
	}

	if queue.current_index != 1 {
		t.Errorf("ReplicationBufferQueue Push Current_index Error Fail %v", queue.current_index)
	}

	if queue.buf[0] != buf[0] || queue.buf[10] != buf[10] || queue.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Push Buf Error Fail %v", queue.buf)
	}
}

func TestReplicationBufferQueue_Pop(t *testing.T) {
	queue := NewReplicationBufferQueue(nil, 1024 * 1024)

	obuf := make([]byte, 64)
	buf := make([]byte, 64)
	queue.Push(buf)
	err := queue.Pop(0, obuf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if obuf[0] != buf[0] || obuf[10] != buf[10] || obuf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", obuf)
	}

	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56
	queue.Push(buf)
	err = queue.Pop(1, obuf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if obuf[0] != buf[0] || obuf[10] != buf[10] || obuf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", obuf)
	}
}

func TestReplicationBufferQueue_Head(t *testing.T) {
	queue := NewReplicationBufferQueue(nil, 1024 * 1024)

	obuf := make([]byte, 64)
	buf := make([]byte, 64)
	queue.Push(buf)
	queue.Push(buf)
	index, err := queue.Head(obuf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if index != 1 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", index)
	}

	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56

	queue.Push(buf)
	index, err = queue.Head(obuf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if index != 2 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", index)
	}

	if obuf[0] != buf[0] || obuf[10] != buf[10] || obuf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", obuf)
	}
}

func TestReplicationBufferQueue_Search(t *testing.T) {
	queue := NewReplicationBufferQueue(nil, 1024 * 1024)

	obuf := make([]byte, 64)
	buf := make([]byte, 64)
	queue.Push(buf)
	queue.Push(buf)
	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56
	queue.Push(buf)

	aof_lock := &AofLock{buf: make([]byte, 64)}
	aof_lock.AofIndex = 43
	aof_lock.AofId = 343294329
	aof_lock.Encode()
	queue.Push(aof_lock.buf)

	index, err := queue.Search(aof_lock.GetRequestId(), obuf)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if index != 3 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", index)
	}

	aof_lock.buf = obuf
	aof_lock.Decode()
	if aof_lock.AofIndex != 43 || aof_lock.AofId != 343294329 {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", aof_lock)
	}
}

func TestReplicationBufferQueue_Run(t *testing.T) {
	queue := NewReplicationBufferQueue(nil, 1024 * 1024)

	go func() {
		index := uint32(0)
		buf := make([]byte, 64)
		for i := 0; i < 100000; i++ {
			buf[0], buf[1], buf[2], buf[3] = uint8(index), uint8(index >> 8), uint8(index >> 16), uint8(index >> 24)
			index ++
			queue.Push(buf)
			if index % 1000 == 0 {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	rindex := uint64(0)
	for {
		buf := make([]byte, 64)
		for {
			err := queue.Pop(rindex, buf)
			if err == io.EOF {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			if err != nil {
				t.Errorf("ReplicationBufferQueue Run Pop Error Fail %v", err)
			}
			break
		}
		bindex := uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24
		if rindex != bindex {
			t.Errorf("ReplicationBufferQueue Run Data Error Fail %d %d", rindex, bindex)
		}

		rindex++
		if rindex >= 100000 {
			break
		}
	}
}