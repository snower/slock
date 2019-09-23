package server

import "testing"

func TestTextServerProtocolParser_Parse(t *testing.T) {
    admin_parse := &TextServerProtocolParser{make([]byte, 4096), 0, 0, 0, make([]string, 0), 0, make([]byte, 0), 0}

    data := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")

    copy(admin_parse.buf, data)
    admin_parse.buf_len = len(data)

    err := admin_parse.Parse()
    if err != nil {
        t.Errorf("Admin Parse Fail %v %v", err, admin_parse.args)
        return
    }

    if len(admin_parse.args) != 3 {
        t.Errorf("Admin Parse Fail %v", admin_parse.args)
        return
    }

    for i, arg := range []string{"SET", "mykey", "myvalue"} {
        if admin_parse.args[i] != arg {
            t.Errorf("Admin Parse Arg Fail %v %s", admin_parse.args, arg)
            return
        }
    }

    if len(admin_parse.args) != admin_parse.args_count {
        t.Errorf("Admin Parse Args Count Fail %v %d", admin_parse.args, admin_parse.args_count)
        return
    }
}

func TestTextServerProtocolParser_MultiParse(t *testing.T) {
    admin_parse := &TextServerProtocolParser{make([]byte, 4096), 0, 0, 0, make([]string, 0), 0, make([]byte, 0), 0}

    datas := [][]byte{
        []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r"),
        []byte("\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$"),
        []byte("7\r\nmyvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"),
    }

    cmd_count := 0
    for _, data := range datas {
        copy(admin_parse.buf, data)
        admin_parse.buf_len = len(data)
        admin_parse.buf_index = 0

        for ; admin_parse.buf_index < admin_parse.buf_len; {
            err := admin_parse.Parse()
            if err != nil {
                t.Errorf("Admin Parse Fail %v %v", err, admin_parse.args)
                return
            }

            if len(admin_parse.args) != admin_parse.args_count {
                continue
            }
            cmd_count++

            if len(admin_parse.args) != 3 {
                t.Errorf("Admin Parse Fail %v", admin_parse.args)
                return
            }

            for i, arg := range []string{"SET", "mykey", "myvalue"} {
                if admin_parse.args[i] != arg {
                    t.Errorf("Admin Parse Arg Fail %v %s", admin_parse.args, arg)
                    return
                }
            }

            admin_parse.args = admin_parse.args[:0]
            admin_parse.args_count = 0
        }
    }

    if cmd_count != 4 {
        t.Errorf("Admin Multi Parse Fail %d", cmd_count)
        return
    }
}

func TestTextServerProtocolParser_Build(t *testing.T) {
    admin_parse := &TextServerProtocolParser{make([]byte, 4096), 0, 0, 0, make([]string, 0), 0, make([]byte, 0), 0}
    r := admin_parse.Build(true, "OK", nil)
    if string(r) != "+OK\r\n" {
        t.Errorf("Admin Build Success Result Fail %s", string(r))
        return
    }

    r = admin_parse.Build(false, "Unknown Command", nil)
    if string(r) != "-ERR Unknown Command\r\n" {
        t.Errorf("Admin Build Error Result Fail %s", string(r))
        return
    }

    r = admin_parse.Build(true, "", []string{"Success"})
    if string(r) != "$7\r\nSuccess\r\n" {
        t.Errorf("Admin Build Result Fail %s", string(r))
        return
    }

    r = admin_parse.Build(true, "", []string{"Success", "test"})
    if string(r) != "*2\r\n$7\r\nSuccess\r\n$4\r\ntest\r\n" {
        t.Errorf("Admin Build Multi Result Fail %s", string(r))
        return
    }
}