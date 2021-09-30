package protocol

import (
	"bytes"
	"testing"
)

func TestLockCommand_Encode(t *testing.T) {
	rid := [16]byte{0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	command := Command{MAGIC, VERSION, COMMAND_LOCK, rid}
	lockCommand := LockCommand{command, 0, 0, rid, rid, 0, 5, 0, 5, 1, 0}
	buf := make([]byte, 64)
	if lockCommand.Encode(buf) != nil {
		t.Error("TestLockCommand_Encode Test Return Nil Fail")
		return
	}

	rbuf := []byte{MAGIC, VERSION, COMMAND_LOCK,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 0, 0,
		5, 0, 0, 0,
		1, 0, 0,
	}

	if !bytes.Equal(buf, rbuf) {
		t.Errorf("TestLockCommand_Encode Test Fail \n%v \n%v", buf, rbuf)
		return
	}
}

func TestLockCommand_Decode(t *testing.T) {
	rid := [16]byte{0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	buf := []byte{MAGIC, VERSION, COMMAND_LOCK,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 0, 0,
		5, 0, 0, 0,
		1, 0, 0,
	}

	lockCommand := LockCommand{}
	err := lockCommand.Decode(buf)
	if err != nil {
		t.Error("TestLockCommand_Decode Test Return Nil Fail")
		return
	}

	if lockCommand.Magic != MAGIC {
		t.Error("TestLockCommand_Decode Test Magic Fail")
		return
	}

	if lockCommand.Version != VERSION {
		t.Error("TestLockCommand_Decode Test Version Fail")
		return
	}

	if lockCommand.CommandType != COMMAND_LOCK {
		t.Error("TestLockCommand_Decode Test CommandType Fail")
		return
	}

	if lockCommand.RequestId != rid {
		t.Error("TestLockCommand_Decode Test RequestId Fail")
		return
	}

	if lockCommand.Flag != 0 {
		t.Error("TestLockCommand_Decode Test Flag Fail")
		return
	}

	if lockCommand.DbId != 0 {
		t.Error("TestLockCommand_Decode Test DbId Fail")
		return
	}

	if lockCommand.LockId != rid {
		t.Error("TestLockCommand_Decode Test LockId Fail")
		return
	}

	if lockCommand.LockKey != rid {
		t.Error("TestLockCommand_Decode Test LockKey Fail")
		return
	}

	if lockCommand.Timeout != 5 {
		t.Error("TestLockCommand_Decode Test Timeout Fail")
		return
	}

	if lockCommand.Expried != 5 {
		t.Error("TestLockCommand_Decode Test Expried Fail")
		return
	}

	if lockCommand.Count != 1 {
		t.Error("TestLockCommand_Decode Test Count Fail")
		return
	}
}

func TestLockResultCommand_Encode(t *testing.T) {
	rid := [16]byte{0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	command := ResultCommand{MAGIC, VERSION, COMMAND_LOCK, rid, 0}
	lockCommand := LockResultCommand{command, 0, 0, rid, rid, 0, 0, 0, 0, [4]byte{0, 0, 0, 0}}
	buf := make([]byte, 64)
	if lockCommand.Encode(buf) != nil {
		t.Error("TestLockResultCommand_Encode Test Return Nil Fail")
		return
	}

	rbuf := []byte{MAGIC, VERSION, COMMAND_LOCK,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	}

	if !bytes.Equal(buf, rbuf) {
		t.Errorf("TestLockResultCommand_Encode Test Fail \n%v \n%v", buf, rbuf)
		return
	}
}

func TestLockResultCommand_Decode(t *testing.T) {
	rid := [16]byte{0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	buf := []byte{MAGIC, VERSION, COMMAND_LOCK,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	}

	lockCommand := LockResultCommand{}
	err := lockCommand.Decode(buf)
	if err != nil {
		t.Error("TestLockResultCommand_Decode Test Return Nil Fail")
		return
	}

	if lockCommand.Magic != MAGIC {
		t.Error("TestLockResultCommand_Decode Test Magic Fail")
		return
	}

	if lockCommand.Version != VERSION {
		t.Error("TestLockResultCommand_Decode Test Version Fail")
		return
	}

	if lockCommand.CommandType != COMMAND_LOCK {
		t.Error("TestLockResultCommand_Decode Test CommandType Fail")
		return
	}

	if lockCommand.RequestId != rid {
		t.Error("TestLockResultCommand_Decode Test RequestId Fail")
		return
	}

	if lockCommand.Result != 0 {
		t.Error("TestLockResultCommand_Decode Test Result Fail")
		return
	}

	if lockCommand.Flag != 0 {
		t.Error("TestLockResultCommand_Decode Test Flag Fail")
		return
	}

	if lockCommand.DbId != 0 {
		t.Error("TestLockResultCommand_Decode Test DbId Fail")
		return
	}

	if lockCommand.LockId != rid {
		t.Error("TestLockResultCommand_Decode Test LockId Fail")
		return
	}

	if lockCommand.LockKey != rid {
		t.Error("TestLockResultCommand_Decode Test LockKey Fail")
		return
	}
}
