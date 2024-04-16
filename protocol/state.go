package protocol

type LockDBState struct {
	LockCount        uint64
	UnLockCount      uint64
	LockedCount      uint32
	KeyCount         uint32
	WaitCount        uint32
	TimeoutedCount   uint32
	ExpriedCount     uint32
	UnlockErrorCount uint32
}
