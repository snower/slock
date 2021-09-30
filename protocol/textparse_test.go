package protocol

import (
	"strings"
	"testing"
)

func TestTextParser_ParseRequest(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	adminParse.CopyToReadBuf(data)

	err := adminParse.ParseRequest()
	if err != nil {
		t.Errorf("Text Request Command Parse Fail %v %v", err, adminParse.GetArgs())
		return
	}

	if len(adminParse.args) != 3 {
		t.Errorf("Admin Parse Fail %v", adminParse.args)
		return
	}

	for i, arg := range []string{"SET", "mykey", "myvalue"} {
		if adminParse.args[i] != arg {
			t.Errorf("Text Request Command Parse Arg Fail %v %s", adminParse.args, arg)
			return
		}
	}

	if len(adminParse.args) != adminParse.argsCount {
		t.Errorf("Text Request Command Parse Args Count Fail %v %d", adminParse.args, adminParse.argsCount)
		return
	}
}

func TestTextParser_ParseZeroArgsRequest(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*3\r\n$7\r\nslaveof\r\n$0\r\n\r\n$0\r\n\r\n")
	adminParse.CopyToReadBuf(data)

	err := adminParse.ParseRequest()
	if err != nil {
		t.Errorf("Text Request Command Parse Fail %v %v", err, adminParse.GetArgs())
		return
	}

	if len(adminParse.args) != 3 {
		t.Errorf("Admin Parse Fail %v", adminParse.args)
		return
	}

	for i, arg := range []string{"slaveof", "", ""} {
		if adminParse.args[i] != arg {
			t.Errorf("Text Request Command Parse Arg Fail %v %s", adminParse.args, arg)
			return
		}
	}

	if len(adminParse.args) != adminParse.argsCount {
		t.Errorf("Text Request Command Parse Args Count Fail %v %d", adminParse.args, adminParse.argsCount)
		return
	}
}

func TestTextParser_MultiParseRequest(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	datas := [][]byte{
		[]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r"),
		[]byte("\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyv"),
		[]byte("alue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"),
	}

	cmdCount := 0
	for _, data := range datas {
		copy(adminParse.rbuf, data)
		adminParse.bufLen = len(data)
		adminParse.bufIndex = 0

		for adminParse.bufIndex < adminParse.bufLen {
			err := adminParse.ParseRequest()
			if err != nil {
				t.Errorf("Text Request Command Parse Fail %v %v", err, adminParse.args)
				return
			}

			if adminParse.stage != 0 {
				continue
			}
			cmdCount++

			if len(adminParse.args) != 3 {
				t.Errorf("Text Request Command Parse Fail %v", adminParse.args)
				return
			}

			for i, arg := range []string{"SET", "mykey", "myvalue"} {
				if adminParse.args[i] != arg {
					t.Errorf("Text Request Command Parse Arg Fail %v %s", adminParse.args, arg)
					return
				}
			}

			adminParse.args = adminParse.args[:0]
			adminParse.argsCount = 0
		}
	}

	if cmdCount != 4 {
		t.Errorf("Text Request Command Multi Parse Fail %d", cmdCount)
		return
	}
}

func TestTextParser_BuildResponse(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))
	r := adminParse.BuildResponse(true, "OK", nil)
	if string(r) != "+OK\r\n" {
		t.Errorf("Text Response Command  Build Success Result Fail %s", string(r))
		return
	}

	r = adminParse.BuildResponse(false, "ERR Unknown Command", nil)
	if string(r) != "-ERR Unknown Command\r\n" {
		t.Errorf("Text Response Command Build Error Result Fail %s", string(r))
		return
	}

	r = adminParse.BuildResponse(true, "", []string{"Success"})
	if string(r) != "$7\r\nSuccess\r\n" {
		t.Errorf("Text Response Command  Build Result Fail %s", string(r))
		return
	}

	r = adminParse.BuildResponse(true, "", []string{"Success", "test"})
	if string(r) != "*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n" {
		t.Errorf("Text Response Command Build Multi Result Fail %s", string(r))
		return
	}
}

func TestTextParser_ParseResponseOK(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("+OK\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse OK Fail %v", r)
		return
	}

	if adminParse.argsType != 1 {
		t.Errorf("Text Response Command Parse OK arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 1 {
		t.Errorf("Text Response Command Parse OK args len Fail %d", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "OK" {
		t.Errorf("Text Response Command Parse OK args Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseResponseERR(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("-ERR\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("TextClientProtocolParser Parse ERR Fail %v", r)
		return
	}

	if adminParse.argsType != 2 {
		t.Errorf("Text Response Command Parse ERR arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 2 {
		t.Errorf("Text Response Command Parse ERR args len Fail %v", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "ERR" || adminParse.args[1] != "" {
		t.Errorf("Text Response Command Parse ERR type Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseResponseERRMessage(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("-ERR Unknown Command\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("TextClientProtocolParser Parse ERR Fail %v", r)
		return
	}

	if adminParse.argsType != 2 {
		t.Errorf("Text Response Command Parse ERR arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 2 {
		t.Errorf("Text Response Command Parse ERR args len Fail %d", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "ERR" {
		t.Errorf("Text Response Command Parse ERR type Fail %v", adminParse.args)
		return
	}

	if adminParse.args[1] != "Unknown Command" {
		t.Errorf("Text Response Command Parse ERR args Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseResponse(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("$7\r\nSuccess\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse Fail %v", r)
		return
	}

	if adminParse.argsType != 3 {
		t.Errorf("Text Response Command Parse arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 1 {
		t.Errorf("Text Response Command Parse args len Fail %d", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "Success" {
		t.Errorf("Text Response Command Parse args Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseResponseARY(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse ARY Fail %v", r)
		return
	}

	if adminParse.argsType != 4 {
		t.Errorf("Text Response Command Parse ARY arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 2 {
		t.Errorf("Text Response Command Parse ARY args len Fail %d", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "Success" || adminParse.args[1] != "test" {
		t.Errorf("Text Response Command Parse ARY args Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseResponseZeroArg(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	data := []byte("*2\r\n$0\r\n\r\n$4\r\ntest\r\n")
	copy(adminParse.rbuf, data)
	adminParse.bufLen = len(data)
	r := adminParse.ParseResponse()
	if r != nil {
		t.Errorf("Text Response Command Parse ARY Fail %v", r)
		return
	}

	if adminParse.argsType != 4 {
		t.Errorf("Text Response Command Parse ARY arg_type Fail %d", adminParse.argsType)
		return
	}

	if len(adminParse.args) != 2 {
		t.Errorf("Text Response Command Parse ARY args len Fail %d", len(adminParse.args))
		return
	}

	if adminParse.args[0] != "" || adminParse.args[1] != "test" {
		t.Errorf("Text Response Command Parse ARY args Fail %v", adminParse.args)
		return
	}
}

func TestTextParser_ParseMulti(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	result := ""

	datas := [][]byte{
		[]byte("+OK\r\n-ERR U"),
		[]byte("nknown Comm"),
		[]byte("and\r\n$7\r\n"),
		[]byte("Success\r\n*2\r\n$7\r\nSucce"),
		[]byte("ss\r\n$4\r\ntest\r\n"),
	}

	for _, data := range datas {
		copy(adminParse.rbuf, data)
		adminParse.bufLen = len(data)
		adminParse.bufIndex = 0

		for adminParse.bufIndex < adminParse.bufLen {
			r := adminParse.ParseResponse()
			if r != nil {
				t.Errorf("Text Response Command ParseMulti Fail %v", r)
				return
			}

			if adminParse.stage == 0 {
				result += strings.Join(adminParse.args, "")
				adminParse.args = adminParse.args[:0]
				adminParse.argsCount = 0
			}
		}
	}

	if result != "OKERRUnknown CommandSuccessSuccesstest" {
		t.Errorf("Text Response Command ParseMulti Result Fail %s", result)
		return
	}
}

func TestTextParser_BuildRequest(t *testing.T) {
	adminParse := NewTextParser(make([]byte, 1024), make([]byte, 1024))

	r := adminParse.BuildRequest([]string{"LOCK", "test"})
	if string(r) != "*2\r\n$4\r\nLOCK\r\n$4\r\ntest\r\n" {
		t.Errorf("Text Request Command Build Command Fail %s", string(r))
	}
}
