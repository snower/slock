package protocol

import (
	"strings"
	"testing"
)

func TestTextParser_ParseRequest(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	admin_parse.CopyToReadBuf(data)

	err := admin_parse.ParseRequest()
	if err != nil {
		t.Errorf("Text Request Command Parse Fail %v %v", err, admin_parse.GetArgs())
		return
	}

	if len(admin_parse.args) != 3 {
		t.Errorf("Admin Parse Fail %v", admin_parse.args)
		return
	}

	for i, arg := range []string{"SET", "mykey", "myvalue"} {
		if admin_parse.args[i] != arg {
			t.Errorf("Text Request Command Parse Arg Fail %v %s", admin_parse.args, arg)
			return
		}
	}

	if len(admin_parse.args) != admin_parse.args_count {
		t.Errorf("Text Request Command Parse Args Count Fail %v %d", admin_parse.args, admin_parse.args_count)
		return
	}
}

func TestTextParser_MultiParseRequest(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	datas := [][]byte{
		[]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r"),
		[]byte("\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyv"),
		[]byte("alue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"),
	}

	cmd_count := 0
	for _, data := range datas {
		copy(admin_parse.rbuf, data)
		admin_parse.buf_len = len(data)
		admin_parse.buf_index = 0

		for ; admin_parse.buf_index < admin_parse.buf_len; {
			err := admin_parse.ParseRequest()
			if err != nil {
				t.Errorf("Text Request Command Parse Fail %v %v", err, admin_parse.args)
				return
			}

			if admin_parse.stage != 0 {
				continue
			}
			cmd_count++

			if len(admin_parse.args) != 3 {
				t.Errorf("Text Request Command Parse Fail %v", admin_parse.args)
				return
			}

			for i, arg := range []string{"SET", "mykey", "myvalue"} {
				if admin_parse.args[i] != arg {
					t.Errorf("Text Request Command Parse Arg Fail %v %s", admin_parse.args, arg)
					return
				}
			}

			admin_parse.args = admin_parse.args[:0]
			admin_parse.args_count = 0
		}
	}

	if cmd_count != 4 {
		t.Errorf("Text Request Command Multi Parse Fail %d", cmd_count)
		return
	}
}

func TestTextParser_BuildResponse(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))
	r := admin_parse.BuildResponse(true, "OK", nil)
	if string(r) != "+OK\r\n" {
		t.Errorf("Text Response Command  Build Success Result Fail %s", string(r))
		return
	}

	r = admin_parse.BuildResponse(false, "ERR Unknown Command", nil)
	if string(r) != "-ERR Unknown Command\r\n" {
		t.Errorf("Text Response Command Build Error Result Fail %s", string(r))
		return
	}

	r = admin_parse.BuildResponse(true, "", []string{"Success"})
	if string(r) != "$7\r\nSuccess\r\n" {
		t.Errorf("Text Response Command  Build Result Fail %s", string(r))
		return
	}

	r = admin_parse.BuildResponse(true, "", []string{"Success", "test"})
	if string(r) != "*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n" {
		t.Errorf("Text Response Command Build Multi Result Fail %s", string(r))
		return
	}
}


func TestTextParser_ParseResponseOK(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("+OK\r\n")
	copy(admin_parse.rbuf, data)
	admin_parse.buf_len = len(data)
	r := admin_parse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse OK Fail %v", r)
		return
	}

	if admin_parse.args_type != 1 {
		t.Errorf("Text Response Command Parse OK arg_type Fail %d", admin_parse.args_type)
		return
	}

	if len(admin_parse.args) != 1 {
		t.Errorf("Text Response Command Parse OK args len Fail %d", len(admin_parse.args))
		return
	}

	if admin_parse.args[0] != "OK" {
		t.Errorf("Text Response Command Parse OK args Fail %v", admin_parse.args)
		return
	}
}

func TestTextParser_ParseResponseERR(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("-ERR\r\n")
	copy(admin_parse.rbuf, data)
	admin_parse.buf_len = len(data)
	r := admin_parse.ParseResponse()
	if r != nil {
		t.Errorf("TextClientProtocolParser Parse ERR Fail %v", r)
		return
	}

	if admin_parse.args_type != 2 {
		t.Errorf("Text Response Command Parse ERR arg_type Fail %d", admin_parse.args_type)
		return
	}

	if len(admin_parse.args) != 2 {
		t.Errorf("Text Response Command Parse ERR args len Fail %v", len(admin_parse.args))
		return
	}

	if admin_parse.args[0] != "ERR" || admin_parse.args[1] != "" {
		t.Errorf("Text Response Command Parse ERR type Fail %v", admin_parse.args)
		return
	}
}


func TestTextParser_ParseResponseERRMessage(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("-ERR Unknown Command\r\n")
	copy(admin_parse.rbuf, data)
	admin_parse.buf_len = len(data)
	r := admin_parse.ParseResponse()
	if r != nil {
		t.Errorf("TextClientProtocolParser Parse ERR Fail %v", r)
		return
	}

	if admin_parse.args_type != 2 {
		t.Errorf("Text Response Command Parse ERR arg_type Fail %d", admin_parse.args_type)
		return
	}

	if len(admin_parse.args) != 2 {
		t.Errorf("Text Response Command Parse ERR args len Fail %d", len(admin_parse.args))
		return
	}

	if admin_parse.args[0] != "ERR" {
		t.Errorf("Text Response Command Parse ERR type Fail %v", admin_parse.args)
		return
	}

	if admin_parse.args[1] != "Unknown Command" {
		t.Errorf("Text Response Command Parse ERR args Fail %v", admin_parse.args)
		return
	}
}

func TestTextParser_ParseResponse(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("$7\r\nSuccess\r\n")
	copy(admin_parse.rbuf, data)
	admin_parse.buf_len = len(data)
	r := admin_parse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse Fail %v", r)
		return
	}

	if admin_parse.args_type != 3 {
		t.Errorf("Text Response Command Parse arg_type Fail %d", admin_parse.args_type)
		return
	}

	if len(admin_parse.args) != 1 {
		t.Errorf("Text Response Command Parse args len Fail %d", len(admin_parse.args))
		return
	}

	if admin_parse.args[0] != "Success" {
		t.Errorf("Text Response Command Parse args Fail %v", admin_parse.args)
		return
	}
}

func TestTextParser_ParseResponseARY(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n")
	copy(admin_parse.rbuf, data)
	admin_parse.buf_len = len(data)
	r := admin_parse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse ARY Fail %v", r)
		return
	}

	if admin_parse.args_type != 4 {
		t.Errorf("Text Response Command Parse ARY arg_type Fail %d", admin_parse.args_type)
		return
	}

	if len(admin_parse.args) != 2 {
		t.Errorf("Text Response Command Parse ARY args len Fail %d", len(admin_parse.args))
		return
	}

	if admin_parse.args[0] != "Success" || admin_parse.args[1] != "test" {
		t.Errorf("Text Response Command Parse ARY args Fail %v", admin_parse.args)
		return
	}
}

func TestTextParser_ParseMulti(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	result := ""

	datas := [][]byte{
		[]byte("+OK\r\n-ERR U"),
		[]byte("nknown Comm"),
		[]byte("and\r\n$7\r\n"),
		[]byte("Success\r\n*2\r\n$7\r\nSucce"),
		[]byte("ss\r\n$4\r\ntest\r\n"),
	}

	for _, data := range datas {
		copy(admin_parse.rbuf, data)
		admin_parse.buf_len = len(data)
		admin_parse.buf_index = 0

		for ; admin_parse.buf_index < admin_parse.buf_len; {
			r := admin_parse.ParseResponse()
			if r != nil {
				t.Errorf("Text Response Command ParseMulti Fail %v", r)
				return
			}

			if admin_parse.stage == 0 {
				result += strings.Join(admin_parse.args, "")
				admin_parse.args = admin_parse.args[:0]
				admin_parse.args_count = 0
			}
		}
	}

	if result != "OKERRUnknown CommandSuccessSuccesstest" {
		t.Errorf("Text Response Command ParseMulti Result Fail %s", result)
		return
	}
}

func TestTextParser_BuildRequest(t *testing.T) {
	admin_parse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	r := admin_parse.BuildRequest([]string{"LOCK", "test"})
	if string(r) != "*2\r\n$4\r\nLOCK\r\n$4\r\ntest\r\n" {
		t.Errorf("Text Request Command Build Command Fail %s", string(r))
	}
}