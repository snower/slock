package client

import (
    "strings"
    "testing"
)

func TestTextClientProtocolParser_ParseOK(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    data := []byte("+OK\r\n")
    copy(admin_parse.buf, data)
    admin_parse.buf_len = len(data)
    r := admin_parse.Parse()
    if r != nil {
        t.Errorf("TextClientProtocolParser Parse OK Fail %v", r)
        return
    }

    if admin_parse.arg_type != 1 {
        t.Errorf("TextClientProtocolParser Parse OK arg_type Fail %d", admin_parse.arg_type)
        return
    }

    if len(admin_parse.args) != 1 {
        t.Errorf("TextClientProtocolParser Parse OK args len Fail %d", len(admin_parse.args))
        return
    }

    if admin_parse.args[0] != "OK" {
        t.Errorf("TextClientProtocolParser Parse OK args Fail %v", admin_parse.args)
        return
    }
}

func TestTextClientProtocolParser_ParseERR(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    data := []byte("-ERR Unknown Command\r\n")
    copy(admin_parse.buf, data)
    admin_parse.buf_len = len(data)
    r := admin_parse.Parse()
    if r != nil {
        t.Errorf("TextClientProtocolParser Parse ERR Fail %v", r)
        return
    }

    if admin_parse.arg_type != 2 {
        t.Errorf("TextClientProtocolParser Parse ERR arg_type Fail %d", admin_parse.arg_type)
        return
    }

    if len(admin_parse.args) != 1 {
        t.Errorf("TextClientProtocolParser Parse ERR args len Fail %d", len(admin_parse.args))
        return
    }

    if admin_parse.args[0] != "ERR Unknown Command" {
        t.Errorf("TextClientProtocolParser Parse ERR args Fail %v", admin_parse.args)
        return
    }
}

func TestTextClientProtocolParser_Parse(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    data := []byte("$7\r\nSuccess\r\n")
    copy(admin_parse.buf, data)
    admin_parse.buf_len = len(data)
    r := admin_parse.Parse()
    if r != nil {
        t.Errorf("TextClientProtocolParser Parse Fail %v", r)
        return
    }

    if admin_parse.arg_type != 3 {
        t.Errorf("TextClientProtocolParser Parse arg_type Fail %d", admin_parse.arg_type)
        return
    }

    if len(admin_parse.args) != 1 {
        t.Errorf("TextClientProtocolParser Parse args len Fail %d", len(admin_parse.args))
        return
    }

    if admin_parse.args[0] != "Success" {
        t.Errorf("TextClientProtocolParser Parse args Fail %v", admin_parse.args)
        return
    }
}

func TestTextClientProtocolParser_ParseARY(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    data := []byte("*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n")
    copy(admin_parse.buf, data)
    admin_parse.buf_len = len(data)
    r := admin_parse.Parse()
    if r != nil {
        t.Errorf("TextClientProtocolParser Parse ARY Fail %v", r)
        return
    }

    if admin_parse.arg_type != 4 {
        t.Errorf("TextClientProtocolParser Parse ARY arg_type Fail %d", admin_parse.arg_type)
        return
    }

    if len(admin_parse.args) != 2 {
        t.Errorf("TextClientProtocolParser Parse ARY args len Fail %d", len(admin_parse.args))
        return
    }

    if admin_parse.args[0] != "Success" || admin_parse.args[1] != "test" {
        t.Errorf("TextClientProtocolParser Parse ARY args Fail %v", admin_parse.args)
        return
    }
}

func TestTextClientProtocolParser_ParseMulti(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    result := ""

    datas := [][]byte{
        []byte("+OK\r\n-ERR U"),
        []byte("nknown Comm"),
        []byte("and\r\n$7\r\n"),
        []byte("Success\r\n*2\r\n$7\r\nSucce"),
        []byte("ss\r\n$4\r\ntest\r\n"),
    }

    for _, data := range datas {
        copy(admin_parse.buf, data)
        admin_parse.buf_len = len(data)
        admin_parse.buf_index = 0

        for ; admin_parse.buf_index < admin_parse.buf_len; {
            r := admin_parse.Parse()
            if r != nil {
                t.Errorf("TextClientProtocolParser ParseMulti Fail %v", r)
                return
            }

            if admin_parse.stage == 0 {
                result += strings.Join(admin_parse.args, "")
                admin_parse.args = admin_parse.args[:0]
                admin_parse.args_count = 0
            }
        }
    }

    if result != "OKERR Unknown CommandSuccessSuccesstest" {
        t.Errorf("TextClientProtocolParser ParseMulti Result Fail %s", result)
        return
    }
}

func TestTextClientProtocolParser_Build(t *testing.T) {
    admin_parse := &TextClientProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0, 0}

    r := admin_parse.Build([]string{"LOCK", "test"})
    if string(r) != "*2\r\n$4\r\nLOCK\r\n$4\r\ntest\r\n" {
        t.Errorf("TextClientProtocolParser Build Command Fail %s", string(r))
    }
}