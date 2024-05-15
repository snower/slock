package protocol

import "testing"

func TestMemBytesArrayStream(t *testing.T) {
	stream := NewMemBytesArrayStream()

	_, _ = stream.Write([]byte("hello"))
	_, _ = stream.Write([]byte("world"))

	size := stream.Size()
	if size != 10 {
		t.Errorf("size should be 10, but %d", size)
		return
	}
	buf := make([]byte, 10)
	_, _ = stream.Read(buf)
	if string(buf) != "helloworld" {
		t.Errorf("buf should be \"helloworld\", but \"%s\"", string(buf))
		return
	}
	size = stream.Size()
	if size != 0 {
		t.Errorf("size should be 0, but %d", size)
		return
	}

	_, _ = stream.Write([]byte("hello"))
	_, _ = stream.Write([]byte("world"))

	size = stream.Size()
	if size != 10 {
		t.Errorf("size should be 10, but %d", size)
		return
	}

	buf = make([]byte, 2)
	_, _ = stream.Read(buf)
	if string(buf) != "he" {
		t.Errorf("buf should be \"he\", but \"%s\"", string(buf))
		return
	}
	buf = make([]byte, 6)
	_, _ = stream.Read(buf)
	if string(buf) != "llowor" {
		t.Errorf("buf should be \"llowor\", but \"%s\"", string(buf))
		return
	}
	buf = make([]byte, 2)
	_, _ = stream.Read(buf)
	if string(buf) != "ld" {
		t.Errorf("buf should be \"ld\", but \"%s\"", string(buf))
		return
	}
	size = stream.Size()
	if size != 0 {
		t.Errorf("size should be 0, but %d", size)
		return
	}
}
