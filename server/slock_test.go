package server

import "testing"

func TestSLockAddServerProtocolReturnsSession(t *testing.T) {
	serverConfig := &ServerConfig{DBConcurrent: 1}
	slock := NewSLock(serverConfig, nil)
	serverProtocol := NewDefaultServerProtocol(slock)

	session := slock.addServerProtocol(serverProtocol)
	if session == nil {
		t.Fatal("expected session to be returned")
	}
	if session.serverProtocol != serverProtocol {
		t.Fatal("expected session to keep protocol reference")
	}
	if slock.protocolSessions[session.sessionId] != session {
		t.Fatal("expected session to be stored in protocolSessions")
	}
}
