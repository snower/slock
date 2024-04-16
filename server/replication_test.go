package server

import (
	"io"
	"testing"
	"time"
)

func TestReplicationBufferQueue_Push(t *testing.T) {
	queue := NewReplicationBufferQueue(nil, 1024*1024, 4*1024*1024)

	buf := make([]byte, 64)
	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56

	err := queue.Push(buf, nil)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Push Error Fail %v", err)
	}

	if queue.seq != 1 {
		t.Errorf("ReplicationBufferQueue Push Current_index Error Fail %v", queue.seq)
	}

	if queue.headItem.buf[0] != buf[0] || queue.headItem.buf[10] != buf[10] || queue.headItem.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Push Buf Error Fail %v", queue.headItem.buf)
	}
}

func TestReplicationBufferQueue_Pop(t *testing.T) {
	cursor := &ReplicationBufferQueueCursor{nil, [16]byte{}, make([]byte, 64), nil, 0, true}
	queue := NewReplicationBufferQueue(nil, 1024*1024, 4*1024*1024)

	buf := make([]byte, 64)
	_ = queue.Push(buf, nil)
	err := queue.Pop(cursor)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if cursor.buf[0] != buf[0] || cursor.buf[10] != buf[10] || cursor.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", cursor.buf)
	}

	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56
	_ = queue.Push(buf, nil)
	err = queue.Pop(cursor)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if cursor.buf[0] != buf[0] || cursor.buf[10] != buf[10] || cursor.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", cursor.buf)
	}
}

func TestReplicationBufferQueue_Head(t *testing.T) {
	cursor := &ReplicationBufferQueueCursor{nil, [16]byte{}, make([]byte, 64), nil, 0, true}
	queue := NewReplicationBufferQueue(nil, 1024*1024, 4*1024*1024)

	buf := make([]byte, 64)
	_ = queue.Push(buf, nil)
	_ = queue.Push(buf, nil)
	err := queue.Head(cursor)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if cursor.seq != 1 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", cursor.seq)
	}

	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56

	_ = queue.Push(buf, nil)
	err = queue.Head(cursor)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if cursor.seq != 2 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", cursor.seq)
	}

	if cursor.buf[0] != buf[0] || cursor.buf[10] != buf[10] || cursor.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", cursor.buf)
	}
}

func TestReplicationBufferQueue_Search(t *testing.T) {
	cursor := &ReplicationBufferQueueCursor{nil, [16]byte{}, make([]byte, 64), nil, 0, true}
	queue := NewReplicationBufferQueue(nil, 1024*1024, 4*1024*1024)

	buf := make([]byte, 64)
	_ = queue.Push(buf, nil)
	_ = queue.Push(buf, nil)
	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56
	_ = queue.Push(buf, nil)

	aofLock := &AofLock{buf: make([]byte, 64)}
	aofLock.AofIndex = 43
	aofLock.AofId = 343294329
	_ = aofLock.Encode()
	_ = queue.Push(aofLock.buf, nil)

	err := queue.Search(aofLock.GetRequestId(), cursor)
	if err != nil {
		t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
	}

	if cursor.seq != 3 {
		t.Errorf("ReplicationBufferQueue Push Index Error Fail %v", cursor.seq)
	}

	aofLock.buf = cursor.buf
	_ = aofLock.Decode()
	if aofLock.AofIndex != 43 || aofLock.AofId != 343294329 {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", aofLock)
	}
}

func TestReplicationBufferQueue_Run(t *testing.T) {
	cursor := &ReplicationBufferQueueCursor{nil, [16]byte{}, make([]byte, 64), nil, 0, true}
	queue := NewReplicationBufferQueue(nil, 1024*1024, 4*1024*1024)

	go func() {
		index := uint32(0)
		buf := make([]byte, 64)
		for i := 0; i < 100000; i++ {
			buf[0], buf[1], buf[2], buf[3] = uint8(index), uint8(index>>8), uint8(index>>16), uint8(index>>24)
			index++
			_ = queue.Push(buf, nil)
			if index%1000 == 0 {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	rindex := uint64(0)
	for {
		for {
			err := queue.Pop(cursor)
			if err == io.EOF {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			if err != nil {
				t.Errorf("ReplicationBufferQueue Run Pop Error Fail %v", err)
			}
			break
		}
		bindex := uint64(cursor.buf[0]) | uint64(cursor.buf[1])<<8 | uint64(cursor.buf[2])<<16 | uint64(cursor.buf[3])<<24
		if rindex != bindex {
			t.Errorf("ReplicationBufferQueue Run Data Error Fail %d %d", rindex, bindex)
		}

		rindex++
		if rindex >= 100000 {
			break
		}
	}
}

func TestReplicationBufferQueue_Reduplicated(t *testing.T) {
	cursor := &ReplicationBufferQueueCursor{nil, [16]byte{}, make([]byte, 64), nil, 0, true}
	queue := NewReplicationBufferQueue(nil, 640, 4*1024*1024)
	queue.AddPoll(cursor)

	buf := make([]byte, 64)
	for i := 0; i < 16; i++ {
		_ = queue.Push(buf, nil)
	}
	buf[0] = 0xa5
	buf[10] = 0x34
	buf[54] = 0x56
	_ = queue.Push(buf, nil)

	for i := 0; i < 17; i++ {
		err := queue.Pop(cursor)
		if err != nil {
			t.Errorf("ReplicationBufferQueue Pop Error Fail %v", err)
		}
	}
	if cursor.buf[0] != buf[0] || cursor.buf[10] != buf[10] || cursor.buf[54] != buf[54] {
		t.Errorf("ReplicationBufferQueue Pop Buf Error Fail %v", cursor.buf)
	}
	if queue.bufferSize != 1280 {
		t.Errorf("ReplicationBufferQueue Buf Size Error Fail %v", queue.bufferSize)
	}
}
