// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.6.1
// source: db.proto

package protobuf

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LockDBLock struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockKey     []byte `protobuf:"bytes,1,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
	LockedCount uint32 `protobuf:"varint,2,opt,name=locked_count,json=lockedCount,proto3" json:"locked_count,omitempty"`
}

func (x *LockDBLock) Reset() {
	*x = LockDBLock{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBLock) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBLock) ProtoMessage() {}

func (x *LockDBLock) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBLock.ProtoReflect.Descriptor instead.
func (*LockDBLock) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{0}
}

func (x *LockDBLock) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

func (x *LockDBLock) GetLockedCount() uint32 {
	if x != nil {
		return x.LockedCount
	}
	return 0
}

type LockDBLockCommand struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RequestId   []byte `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	Flag        uint32 `protobuf:"varint,2,opt,name=flag,proto3" json:"flag,omitempty"`
	LockId      []byte `protobuf:"bytes,3,opt,name=lock_id,json=lockId,proto3" json:"lock_id,omitempty"`
	LockKey     []byte `protobuf:"bytes,4,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
	TimeoutFlag uint32 `protobuf:"varint,5,opt,name=timeout_flag,json=timeoutFlag,proto3" json:"timeout_flag,omitempty"`
	Timeout     uint32 `protobuf:"varint,6,opt,name=timeout,proto3" json:"timeout,omitempty"`
	ExpriedFlag uint32 `protobuf:"varint,7,opt,name=expried_flag,json=expriedFlag,proto3" json:"expried_flag,omitempty"`
	Expried     uint32 `protobuf:"varint,8,opt,name=expried,proto3" json:"expried,omitempty"`
	Count       uint32 `protobuf:"varint,9,opt,name=count,proto3" json:"count,omitempty"`
	Rcount      uint32 `protobuf:"varint,10,opt,name=rcount,proto3" json:"rcount,omitempty"`
}

func (x *LockDBLockCommand) Reset() {
	*x = LockDBLockCommand{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBLockCommand) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBLockCommand) ProtoMessage() {}

func (x *LockDBLockCommand) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBLockCommand.ProtoReflect.Descriptor instead.
func (*LockDBLockCommand) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{1}
}

func (x *LockDBLockCommand) GetRequestId() []byte {
	if x != nil {
		return x.RequestId
	}
	return nil
}

func (x *LockDBLockCommand) GetFlag() uint32 {
	if x != nil {
		return x.Flag
	}
	return 0
}

func (x *LockDBLockCommand) GetLockId() []byte {
	if x != nil {
		return x.LockId
	}
	return nil
}

func (x *LockDBLockCommand) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

func (x *LockDBLockCommand) GetTimeoutFlag() uint32 {
	if x != nil {
		return x.TimeoutFlag
	}
	return 0
}

func (x *LockDBLockCommand) GetTimeout() uint32 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

func (x *LockDBLockCommand) GetExpriedFlag() uint32 {
	if x != nil {
		return x.ExpriedFlag
	}
	return 0
}

func (x *LockDBLockCommand) GetExpried() uint32 {
	if x != nil {
		return x.Expried
	}
	return 0
}

func (x *LockDBLockCommand) GetCount() uint32 {
	if x != nil {
		return x.Count
	}
	return 0
}

func (x *LockDBLockCommand) GetRcount() uint32 {
	if x != nil {
		return x.Rcount
	}
	return 0
}

type LockDBLockLocked struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockId      []byte             `protobuf:"bytes,1,opt,name=lock_id,json=lockId,proto3" json:"lock_id,omitempty"`
	StartTime   uint64             `protobuf:"varint,2,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	TimeoutTime uint64             `protobuf:"varint,3,opt,name=timeout_time,json=timeoutTime,proto3" json:"timeout_time,omitempty"`
	ExpriedTime uint64             `protobuf:"varint,4,opt,name=expried_time,json=expriedTime,proto3" json:"expried_time,omitempty"`
	LockedCount uint32             `protobuf:"varint,5,opt,name=locked_count,json=lockedCount,proto3" json:"locked_count,omitempty"`
	AofTime     uint32             `protobuf:"varint,6,opt,name=aof_time,json=aofTime,proto3" json:"aof_time,omitempty"`
	IsTimeouted bool               `protobuf:"varint,7,opt,name=is_timeouted,json=isTimeouted,proto3" json:"is_timeouted,omitempty"`
	IsExpried   bool               `protobuf:"varint,8,opt,name=is_expried,json=isExpried,proto3" json:"is_expried,omitempty"`
	IsAof       bool               `protobuf:"varint,9,opt,name=is_aof,json=isAof,proto3" json:"is_aof,omitempty"`
	IsLongTime  bool               `protobuf:"varint,10,opt,name=is_long_time,json=isLongTime,proto3" json:"is_long_time,omitempty"`
	Command     *LockDBLockCommand `protobuf:"bytes,11,opt,name=command,proto3" json:"command,omitempty"`
}

func (x *LockDBLockLocked) Reset() {
	*x = LockDBLockLocked{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBLockLocked) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBLockLocked) ProtoMessage() {}

func (x *LockDBLockLocked) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBLockLocked.ProtoReflect.Descriptor instead.
func (*LockDBLockLocked) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{2}
}

func (x *LockDBLockLocked) GetLockId() []byte {
	if x != nil {
		return x.LockId
	}
	return nil
}

func (x *LockDBLockLocked) GetStartTime() uint64 {
	if x != nil {
		return x.StartTime
	}
	return 0
}

func (x *LockDBLockLocked) GetTimeoutTime() uint64 {
	if x != nil {
		return x.TimeoutTime
	}
	return 0
}

func (x *LockDBLockLocked) GetExpriedTime() uint64 {
	if x != nil {
		return x.ExpriedTime
	}
	return 0
}

func (x *LockDBLockLocked) GetLockedCount() uint32 {
	if x != nil {
		return x.LockedCount
	}
	return 0
}

func (x *LockDBLockLocked) GetAofTime() uint32 {
	if x != nil {
		return x.AofTime
	}
	return 0
}

func (x *LockDBLockLocked) GetIsTimeouted() bool {
	if x != nil {
		return x.IsTimeouted
	}
	return false
}

func (x *LockDBLockLocked) GetIsExpried() bool {
	if x != nil {
		return x.IsExpried
	}
	return false
}

func (x *LockDBLockLocked) GetIsAof() bool {
	if x != nil {
		return x.IsAof
	}
	return false
}

func (x *LockDBLockLocked) GetIsLongTime() bool {
	if x != nil {
		return x.IsLongTime
	}
	return false
}

func (x *LockDBLockLocked) GetCommand() *LockDBLockCommand {
	if x != nil {
		return x.Command
	}
	return nil
}

type LockDBLockWait struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockId      []byte             `protobuf:"bytes,1,opt,name=lock_id,json=lockId,proto3" json:"lock_id,omitempty"`
	StartTime   uint64             `protobuf:"varint,2,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	TimeoutTime uint64             `protobuf:"varint,3,opt,name=timeout_time,json=timeoutTime,proto3" json:"timeout_time,omitempty"`
	IsLongTime  bool               `protobuf:"varint,4,opt,name=is_long_time,json=isLongTime,proto3" json:"is_long_time,omitempty"`
	Command     *LockDBLockCommand `protobuf:"bytes,5,opt,name=command,proto3" json:"command,omitempty"`
}

func (x *LockDBLockWait) Reset() {
	*x = LockDBLockWait{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBLockWait) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBLockWait) ProtoMessage() {}

func (x *LockDBLockWait) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBLockWait.ProtoReflect.Descriptor instead.
func (*LockDBLockWait) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{3}
}

func (x *LockDBLockWait) GetLockId() []byte {
	if x != nil {
		return x.LockId
	}
	return nil
}

func (x *LockDBLockWait) GetStartTime() uint64 {
	if x != nil {
		return x.StartTime
	}
	return 0
}

func (x *LockDBLockWait) GetTimeoutTime() uint64 {
	if x != nil {
		return x.TimeoutTime
	}
	return 0
}

func (x *LockDBLockWait) GetIsLongTime() bool {
	if x != nil {
		return x.IsLongTime
	}
	return false
}

func (x *LockDBLockWait) GetCommand() *LockDBLockCommand {
	if x != nil {
		return x.Command
	}
	return nil
}

type LockDBListLockRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DbId uint32 `protobuf:"varint,1,opt,name=db_id,json=dbId,proto3" json:"db_id,omitempty"`
}

func (x *LockDBListLockRequest) Reset() {
	*x = LockDBListLockRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListLockRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListLockRequest) ProtoMessage() {}

func (x *LockDBListLockRequest) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListLockRequest.ProtoReflect.Descriptor instead.
func (*LockDBListLockRequest) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{4}
}

func (x *LockDBListLockRequest) GetDbId() uint32 {
	if x != nil {
		return x.DbId
	}
	return 0
}

type LockDBListLockResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Locks []*LockDBLock `protobuf:"bytes,1,rep,name=locks,proto3" json:"locks,omitempty"`
}

func (x *LockDBListLockResponse) Reset() {
	*x = LockDBListLockResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListLockResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListLockResponse) ProtoMessage() {}

func (x *LockDBListLockResponse) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListLockResponse.ProtoReflect.Descriptor instead.
func (*LockDBListLockResponse) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{5}
}

func (x *LockDBListLockResponse) GetLocks() []*LockDBLock {
	if x != nil {
		return x.Locks
	}
	return nil
}

type LockDBListLockedRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DbId    uint32 `protobuf:"varint,1,opt,name=db_id,json=dbId,proto3" json:"db_id,omitempty"`
	LockKey []byte `protobuf:"bytes,2,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
}

func (x *LockDBListLockedRequest) Reset() {
	*x = LockDBListLockedRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListLockedRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListLockedRequest) ProtoMessage() {}

func (x *LockDBListLockedRequest) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListLockedRequest.ProtoReflect.Descriptor instead.
func (*LockDBListLockedRequest) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{6}
}

func (x *LockDBListLockedRequest) GetDbId() uint32 {
	if x != nil {
		return x.DbId
	}
	return 0
}

func (x *LockDBListLockedRequest) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

type LockDBListLockedResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockKey     []byte              `protobuf:"bytes,1,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
	LockedCount uint32              `protobuf:"varint,2,opt,name=locked_count,json=lockedCount,proto3" json:"locked_count,omitempty"`
	Locks       []*LockDBLockLocked `protobuf:"bytes,3,rep,name=locks,proto3" json:"locks,omitempty"`
}

func (x *LockDBListLockedResponse) Reset() {
	*x = LockDBListLockedResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListLockedResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListLockedResponse) ProtoMessage() {}

func (x *LockDBListLockedResponse) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListLockedResponse.ProtoReflect.Descriptor instead.
func (*LockDBListLockedResponse) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{7}
}

func (x *LockDBListLockedResponse) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

func (x *LockDBListLockedResponse) GetLockedCount() uint32 {
	if x != nil {
		return x.LockedCount
	}
	return 0
}

func (x *LockDBListLockedResponse) GetLocks() []*LockDBLockLocked {
	if x != nil {
		return x.Locks
	}
	return nil
}

type LockDBListWaitRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DbId    uint32 `protobuf:"varint,1,opt,name=db_id,json=dbId,proto3" json:"db_id,omitempty"`
	LockKey []byte `protobuf:"bytes,2,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
}

func (x *LockDBListWaitRequest) Reset() {
	*x = LockDBListWaitRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListWaitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListWaitRequest) ProtoMessage() {}

func (x *LockDBListWaitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListWaitRequest.ProtoReflect.Descriptor instead.
func (*LockDBListWaitRequest) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{8}
}

func (x *LockDBListWaitRequest) GetDbId() uint32 {
	if x != nil {
		return x.DbId
	}
	return 0
}

func (x *LockDBListWaitRequest) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

type LockDBListWaitResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockKey     []byte            `protobuf:"bytes,1,opt,name=lock_key,json=lockKey,proto3" json:"lock_key,omitempty"`
	LockedCount uint32            `protobuf:"varint,2,opt,name=locked_count,json=lockedCount,proto3" json:"locked_count,omitempty"`
	Locks       []*LockDBLockWait `protobuf:"bytes,3,rep,name=locks,proto3" json:"locks,omitempty"`
}

func (x *LockDBListWaitResponse) Reset() {
	*x = LockDBListWaitResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockDBListWaitResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockDBListWaitResponse) ProtoMessage() {}

func (x *LockDBListWaitResponse) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockDBListWaitResponse.ProtoReflect.Descriptor instead.
func (*LockDBListWaitResponse) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{9}
}

func (x *LockDBListWaitResponse) GetLockKey() []byte {
	if x != nil {
		return x.LockKey
	}
	return nil
}

func (x *LockDBListWaitResponse) GetLockedCount() uint32 {
	if x != nil {
		return x.LockedCount
	}
	return 0
}

func (x *LockDBListWaitResponse) GetLocks() []*LockDBLockWait {
	if x != nil {
		return x.Locks
	}
	return nil
}

var File_db_proto protoreflect.FileDescriptor

var file_db_proto_rawDesc = []byte{
	0x0a, 0x08, 0x64, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x22, 0x4a, 0x0a, 0x0a, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f,
	0x63, 0x6b, 0x12, 0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x21, 0x0a,
	0x0c, 0x6c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x0b, 0x6c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74,
	0x22, 0xa2, 0x02, 0x0a, 0x11, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x43,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x66, 0x6c, 0x61, 0x67, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x04, 0x66, 0x6c, 0x61, 0x67, 0x12, 0x17, 0x0a, 0x07, 0x6c, 0x6f, 0x63,
	0x6b, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x6c, 0x6f, 0x63, 0x6b,
	0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x21, 0x0a,
	0x0c, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x5f, 0x66, 0x6c, 0x61, 0x67, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x0b, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x46, 0x6c, 0x61, 0x67,
	0x12, 0x18, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x65, 0x78,
	0x70, 0x72, 0x69, 0x65, 0x64, 0x5f, 0x66, 0x6c, 0x61, 0x67, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x0b, 0x65, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64, 0x46, 0x6c, 0x61, 0x67, 0x12, 0x18, 0x0a,
	0x07, 0x65, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x07,
	0x65, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	0x18, 0x09, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x16, 0x0a,
	0x06, 0x72, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x72,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x80, 0x03, 0x0a, 0x10, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42,
	0x4c, 0x6f, 0x63, 0x6b, 0x4c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x6c, 0x6f,
	0x63, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x6c, 0x6f, 0x63,
	0x6b, 0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x69, 0x6d,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69,
	0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75,
	0x74, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x65, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x65, 0x78, 0x70,
	0x72, 0x69, 0x65, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x6c, 0x6f, 0x63, 0x6b,
	0x65, 0x64, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0b,
	0x6c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x61,
	0x6f, 0x66, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x07, 0x61,
	0x6f, 0x66, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x69, 0x73, 0x5f, 0x74, 0x69, 0x6d,
	0x65, 0x6f, 0x75, 0x74, 0x65, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x69, 0x73,
	0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x65, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x69, 0x73, 0x5f,
	0x65, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x69,
	0x73, 0x45, 0x78, 0x70, 0x72, 0x69, 0x65, 0x64, 0x12, 0x15, 0x0a, 0x06, 0x69, 0x73, 0x5f, 0x61,
	0x6f, 0x66, 0x18, 0x09, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x69, 0x73, 0x41, 0x6f, 0x66, 0x12,
	0x20, 0x0a, 0x0c, 0x69, 0x73, 0x5f, 0x6c, 0x6f, 0x6e, 0x67, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x0a, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x69, 0x73, 0x4c, 0x6f, 0x6e, 0x67, 0x54, 0x69, 0x6d,
	0x65, 0x12, 0x35, 0x0a, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x18, 0x0b, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4c, 0x6f,
	0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52,
	0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x22, 0xc4, 0x01, 0x0a, 0x0e, 0x4c, 0x6f, 0x63,
	0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x57, 0x61, 0x69, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x6c,
	0x6f, 0x63, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x6c, 0x6f,
	0x63, 0x6b, 0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54,
	0x69, 0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x74, 0x69, 0x6d, 0x65, 0x6f,
	0x75, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x20, 0x0a, 0x0c, 0x69, 0x73, 0x5f, 0x6c, 0x6f, 0x6e,
	0x67, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x69, 0x73,
	0x4c, 0x6f, 0x6e, 0x67, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x35, 0x0a, 0x07, 0x63, 0x6f, 0x6d, 0x6d,
	0x61, 0x6e, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x43,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x22,
	0x2c, 0x0a, 0x15, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x69, 0x73, 0x74, 0x4c, 0x6f, 0x63,
	0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x13, 0x0a, 0x05, 0x64, 0x62, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04, 0x64, 0x62, 0x49, 0x64, 0x22, 0x44, 0x0a,
	0x16, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x69, 0x73, 0x74, 0x4c, 0x6f, 0x63, 0x6b, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2a, 0x0a, 0x05, 0x6c, 0x6f, 0x63, 0x6b, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x52, 0x05, 0x6c, 0x6f,
	0x63, 0x6b, 0x73, 0x22, 0x49, 0x0a, 0x17, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x69, 0x73,
	0x74, 0x4c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x13,
	0x0a, 0x05, 0x64, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04, 0x64,
	0x62, 0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x22, 0x8a,
	0x01, 0x0a, 0x18, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x69, 0x73, 0x74, 0x4c, 0x6f, 0x63,
	0x6b, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x6c,
	0x6f, 0x63, 0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6c,
	0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x21, 0x0a, 0x0c, 0x6c, 0x6f, 0x63, 0x6b, 0x65, 0x64,
	0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0b, 0x6c, 0x6f,
	0x63, 0x6b, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x30, 0x0a, 0x05, 0x6c, 0x6f, 0x63,
	0x6b, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f, 0x63, 0x6b, 0x4c, 0x6f,
	0x63, 0x6b, 0x65, 0x64, 0x52, 0x05, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x22, 0x47, 0x0a, 0x15, 0x4c,
	0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x69, 0x73, 0x74, 0x57, 0x61, 0x69, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x13, 0x0a, 0x05, 0x64, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x04, 0x64, 0x62, 0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x63,
	0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6c, 0x6f, 0x63,
	0x6b, 0x4b, 0x65, 0x79, 0x22, 0x86, 0x01, 0x0a, 0x16, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c,
	0x69, 0x73, 0x74, 0x57, 0x61, 0x69, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x19, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x21, 0x0a, 0x0c, 0x6c, 0x6f,
	0x63, 0x6b, 0x65, 0x64, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x0b, 0x6c, 0x6f, 0x63, 0x6b, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x2e, 0x0a,
	0x05, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x44, 0x42, 0x4c, 0x6f,
	0x63, 0x6b, 0x57, 0x61, 0x69, 0x74, 0x52, 0x05, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x42, 0x2b, 0x5a,
	0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x73, 0x6e, 0x6f, 0x77,
	0x65, 0x72, 0x2f, 0x73, 0x6c, 0x6f, 0x63, 0x6b, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f,
	0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_db_proto_rawDescOnce sync.Once
	file_db_proto_rawDescData = file_db_proto_rawDesc
)

func file_db_proto_rawDescGZIP() []byte {
	file_db_proto_rawDescOnce.Do(func() {
		file_db_proto_rawDescData = protoimpl.X.CompressGZIP(file_db_proto_rawDescData)
	})
	return file_db_proto_rawDescData
}

var file_db_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_db_proto_goTypes = []interface{}{
	(*LockDBLock)(nil),               // 0: protobuf.LockDBLock
	(*LockDBLockCommand)(nil),        // 1: protobuf.LockDBLockCommand
	(*LockDBLockLocked)(nil),         // 2: protobuf.LockDBLockLocked
	(*LockDBLockWait)(nil),           // 3: protobuf.LockDBLockWait
	(*LockDBListLockRequest)(nil),    // 4: protobuf.LockDBListLockRequest
	(*LockDBListLockResponse)(nil),   // 5: protobuf.LockDBListLockResponse
	(*LockDBListLockedRequest)(nil),  // 6: protobuf.LockDBListLockedRequest
	(*LockDBListLockedResponse)(nil), // 7: protobuf.LockDBListLockedResponse
	(*LockDBListWaitRequest)(nil),    // 8: protobuf.LockDBListWaitRequest
	(*LockDBListWaitResponse)(nil),   // 9: protobuf.LockDBListWaitResponse
}
var file_db_proto_depIdxs = []int32{
	1, // 0: protobuf.LockDBLockLocked.command:type_name -> protobuf.LockDBLockCommand
	1, // 1: protobuf.LockDBLockWait.command:type_name -> protobuf.LockDBLockCommand
	0, // 2: protobuf.LockDBListLockResponse.locks:type_name -> protobuf.LockDBLock
	2, // 3: protobuf.LockDBListLockedResponse.locks:type_name -> protobuf.LockDBLockLocked
	3, // 4: protobuf.LockDBListWaitResponse.locks:type_name -> protobuf.LockDBLockWait
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_db_proto_init() }
func file_db_proto_init() {
	if File_db_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_db_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBLock); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBLockCommand); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBLockLocked); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBLockWait); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListLockRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListLockResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListLockedRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListLockedResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListWaitRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockDBListWaitResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_db_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_db_proto_goTypes,
		DependencyIndexes: file_db_proto_depIdxs,
		MessageInfos:      file_db_proto_msgTypes,
	}.Build()
	File_db_proto = out.File
	file_db_proto_rawDesc = nil
	file_db_proto_goTypes = nil
	file_db_proto_depIdxs = nil
}
