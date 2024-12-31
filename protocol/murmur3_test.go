package protocol

import (
	"testing"
)

func Benchmark32(b *testing.B) {
	buf := [16]byte{}
	b.SetBytes(int64(16))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MurmurHash3KeySum(buf)
	}
}
