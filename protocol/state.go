package protocol

type LockDBState struct {
	_padding0        [14]uint32
	LockCount        uint64
	_padding1        [14]uint32
	UnLockCount      uint64
	_padding2        [15]uint32
	LockedCount      uint32
	_padding3        [15]uint32
	KeyCount         uint32
	_padding4        [15]uint32
	WaitCount        uint32
	_padding5        [15]uint32
	TimeoutedCount   uint32
	_padding6        [15]uint32
	ExpriedCount     uint32
	_padding7        [15]uint32
	UnlockErrorCount uint32
	_padding8        [15]uint32
}
