package server

import "testing"

func TestReplicationBufferQueue_Push(t *testing.T) {
	queue := NewReplicationBufferQueue()

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
	queue := NewReplicationBufferQueue()

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
	queue := NewReplicationBufferQueue()

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
	queue := NewReplicationBufferQueue()

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