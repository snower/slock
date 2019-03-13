package protocol

import (
    "bytes"
    "testing"
)

func TestLockCommand_Encode(t *testing.T) {
    rid := [2]uint64{2, 3}
    command := Command{MAGIC, VERSION, COMMAND_LOCK, rid}
    lock_command := LockCommand{command, 0, 0, rid, rid, 5, 5, 1, [1]byte{0}}
    buf := make([]byte,  64)
    if lock_command.Encode(buf) != nil {
        t.Error("TestLockCommand_Encode Test Return Nil Fail")
        return
    }

    rbuf := []byte{MAGIC, VERSION, COMMAND_LOCK,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
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
    rid := [2]uint64{2, 3}
    buf := []byte{MAGIC, VERSION, COMMAND_LOCK,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        5, 0, 0, 0,
        5, 0, 0, 0,
        1, 0, 0,
    }

    lock_command := NewLockCommand(buf)
    if lock_command == nil {
        t.Error("TestLockCommand_Decode Test Return Nil Fail")
        return
    }

    if lock_command.Magic != MAGIC {
        t.Error("TestLockCommand_Decode Test Magic Fail")
        return
    }

    if lock_command.Version != VERSION {
        t.Error("TestLockCommand_Decode Test Version Fail")
        return
    }

    if lock_command.CommandType != COMMAND_LOCK {
        t.Error("TestLockCommand_Decode Test CommandType Fail")
        return
    }

    if lock_command.RequestId != rid {
        t.Error("TestLockCommand_Decode Test RequestId Fail")
        return
    }

    if lock_command.Flag != 0 {
        t.Error("TestLockCommand_Decode Test Flag Fail")
        return
    }

    if lock_command.DbId != 0 {
        t.Error("TestLockCommand_Decode Test DbId Fail")
        return
    }

    if lock_command.LockId != rid {
        t.Error("TestLockCommand_Decode Test LockId Fail")
        return
    }

    if lock_command.LockKey != rid {
        t.Error("TestLockCommand_Decode Test LockKey Fail")
        return
    }

    if lock_command.Timeout != 5 {
        t.Error("TestLockCommand_Decode Test Timeout Fail")
        return
    }

    if lock_command.Expried != 5 {
        t.Error("TestLockCommand_Decode Test Expried Fail")
        return
    }

    if lock_command.Count != 1 {
        t.Error("TestLockCommand_Decode Test Count Fail")
        return
    }
}

func TestLockResultCommand_Encode(t *testing.T) {
    rid := [2]uint64{2, 3}
    command := ResultCommand{MAGIC, VERSION, COMMAND_LOCK, rid, 0}
    lock_command := LockResultCommand{command, 0, 0, rid, rid, [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}
    buf := make([]byte,  64)
    if lock_command.Encode(buf) != nil {
        t.Error("TestLockResultCommand_Encode Test Return Nil Fail")
        return
    }

    rbuf := []byte{MAGIC, VERSION, COMMAND_LOCK,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    }

    if !bytes.Equal(buf, rbuf) {
        t.Errorf("TestLockResultCommand_Encode Test Fail \n%v \n%v", buf, rbuf)
        return
    }
}

func TestLockResultCommand_Decode(t *testing.T) {
    rid := [2]uint64{2, 3}
    buf := []byte{MAGIC, VERSION, COMMAND_LOCK,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    }

    lock_command := LockResultCommand{}
    err := lock_command.Decode(buf)
    if err != nil {
        t.Error("TestLockResultCommand_Decode Test Return Nil Fail")
        return
    }

    if lock_command.Magic != MAGIC {
        t.Error("TestLockResultCommand_Decode Test Magic Fail")
        return
    }

    if lock_command.Version != VERSION {
        t.Error("TestLockResultCommand_Decode Test Version Fail")
        return
    }

    if lock_command.CommandType != COMMAND_LOCK {
        t.Error("TestLockResultCommand_Decode Test CommandType Fail")
        return
    }

    if lock_command.RequestId != rid {
        t.Error("TestLockResultCommand_Decode Test RequestId Fail")
        return
    }

    if lock_command.Result != 0 {
        t.Error("TestLockResultCommand_Decode Test Result Fail")
        return
    }

    if lock_command.Flag != 0 {
        t.Error("TestLockResultCommand_Decode Test Flag Fail")
        return
    }

    if lock_command.DbId != 0 {
        t.Error("TestLockResultCommand_Decode Test DbId Fail")
        return
    }

    if lock_command.LockId != rid {
        t.Error("TestLockResultCommand_Decode Test LockId Fail")
        return
    }

    if lock_command.LockKey != rid {
        t.Error("TestLockResultCommand_Decode Test LockKey Fail")
        return
    }
}