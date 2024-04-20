package server

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	slock           *SLock
	server          net.Listener
	streams         *Stream
	glock           *sync.Mutex
	connectedCount  uint32
	connectingCount uint32
	stoped          bool
	stopedWaiter    chan bool
}

func NewServer(slock *SLock) *Server {
	server := &Server{slock, nil, nil, &sync.Mutex{},
		0, 0, false, make(chan bool, 1)}
	admin := slock.GetAdmin()
	admin.server = server
	return server
}

func (self *Server) Listen() error {
	address := fmt.Sprintf("%s:%d", Config.Bind, Config.Port)
	server, err := net.Listen("tcp", address)
	if err != nil {
		self.slock.Log().Errorf("Server listen %s error %v", address, err)
		return err
	}
	self.server = server
	self.slock.Log().Infof("Server listen %s", address)
	return nil
}

func (self *Server) Close() {
	self.glock.Lock()
	self.stoped = true
	err := self.server.Close()
	if err != nil {
		self.slock.Log().Errorf("Server close error %v", err)
	}
	streams := make([]*Stream, 0)
	currentStream := self.streams
	for currentStream != nil {
		streams = append(streams, currentStream)
		currentStream = currentStream.nextStream
	}
	self.glock.Unlock()

	for _, stream := range streams {
		if stream.streamType != STREAM_TYPE_NORMAL {
			continue
		}

		err = stream.Close()
		if err != nil {
			self.slock.Log().Errorf("Server connection close error %v", err)
		}
		<-stream.closedWaiter
	}
	self.slock.Close()
	close(self.stopedWaiter)
}

func (self *Server) addStream(stream *Stream) error {
	self.glock.Lock()
	if self.streams == nil {
		self.streams = stream
	} else {
		self.streams.lastStream = stream
		stream.nextStream = self.streams
		self.streams = stream
	}
	self.connectingCount++
	self.connectedCount++
	self.glock.Unlock()
	return nil
}

func (self *Server) removeStream(stream *Stream) error {
	self.glock.Lock()
	if stream.nextStream == nil && stream.lastStream == nil {
		self.glock.Unlock()
		return nil
	}

	if self.streams == stream {
		self.streams = stream.nextStream
	}
	if stream.lastStream != nil {
		stream.lastStream.nextStream = stream.nextStream
	}
	if stream.nextStream != nil {
		stream.nextStream.lastStream = stream.lastStream
	}
	self.connectingCount--
	self.glock.Unlock()
	return nil
}

func (self *Server) CloseStreams() error {
	self.glock.Lock()
	streams := make([]*Stream, 0)
	currentStream := self.streams
	for currentStream != nil {
		streams = append(streams, currentStream)
		currentStream = currentStream.nextStream
	}
	self.glock.Unlock()

	for _, stream := range streams {
		if stream.streamType != STREAM_TYPE_NORMAL {
			continue
		}
		_ = stream.Close()
	}
	return nil
}

func (self *Server) GetStreams() []*Stream {
	self.glock.Lock()
	streams := make([]*Stream, 0)
	currentStream := self.streams
	for currentStream != nil {
		streams = append(streams, currentStream)
		currentStream = currentStream.nextStream
	}
	self.glock.Unlock()
	return streams
}

func (self *Server) Serve() {
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stopSignal
		self.slock.Log().Infof("Server shutdown start")
		self.Close()
	}()

	self.slock.Log().Infof("Server start serve %s", fmt.Sprintf("%s:%d", Config.Bind, Config.Port))
	go self.slock.Start()
	go self.checkProtocolFreeCommandQueue()
	for !self.stoped {
		conn, err := self.server.Accept()
		if err != nil {
			continue
		}

		stream := NewStream(conn)
		err = self.addStream(stream)
		if err != nil {
			err = stream.Close()
			if err != nil {
				self.slock.Log().Errorf("Server connection close error %v", err)
			}
			close(stream.closedWaiter)
			continue
		}
		go self.handle(stream)
	}
	<-self.stopedWaiter
	self.slock.Log().Infof("Server shutdown finish")
}

func (self *Server) checkProtocolFreeCommandQueue() {
	for !self.stoped {
		select {
		case <-self.stopedWaiter:
			return
		case <-time.After(120 * time.Second):
			if self.slock != nil {
				_ = self.slock.checkServerProtocolSession()
			}
		}
	}
}

func (self *Server) checkProtocol(stream *Stream) (ServerProtocol, error) {
	var serverProtocol ServerProtocol
	buf := make([]byte, 64)
	n, err := stream.Read(buf)
	if err != nil {
		return nil, err
	}

	mv := uint16(buf[0]) | uint16(buf[1])<<8
	if n == 64 && mv == 0x0156 {
		if self.slock.state == STATE_LEADER {
			serverProtocol = NewBinaryServerProtocol(self.slock, stream)
		} else {
			serverProtocol = NewTransparencyBinaryServerProtocol(self.slock, stream, NewBinaryServerProtocol(self.slock, stream))
		}
		self.slock.Log().Infof("Server binary protocol connection connected %s", serverProtocol.RemoteAddr().String())

		err = serverProtocol.ProcessParse(buf)
		if err != nil {
			cerr := serverProtocol.Close()
			if cerr != nil {
				self.slock.Log().Errorf("Server binary protocol connection close error %v", cerr)
			}
			return nil, err
		}
		return serverProtocol, nil
	}

	if self.slock.state == STATE_LEADER {
		serverProtocol = NewTextServerProtocol(self.slock, stream)
	} else {
		serverProtocol = NewTransparencyTextServerProtocol(self.slock, stream, NewTextServerProtocol(self.slock, stream))
	}
	self.slock.Log().Infof("Server text protocol connection connected %s", serverProtocol.RemoteAddr().String())

	err = serverProtocol.ProcessParse(buf[:n])
	if err != nil {
		cerr := serverProtocol.Close()
		if cerr != nil {
			self.slock.Log().Errorf("Server text protocol connection close error %v", err)
		}
		return nil, err
	}
	return serverProtocol, nil
}

func (self *Server) handle(stream *Stream) {
	defer func() {
		err := self.removeStream(stream)
		if err != nil {
			self.slock.Log().Errorf("Server remove connection error %v", err)
		}
		close(stream.closedWaiter)
	}()

	serverProtocol, err := self.checkProtocol(stream)
	if err != nil {
		cerr := stream.Close()
		if cerr != nil {
			self.slock.Log().Errorf("Server connection close error %v", cerr)
		}

		if err != io.EOF {
			self.slock.Log().Errorf("Server protocol connection start process error %v", err)
		}
		return
	}

	for {
		switch serverProtocol.(type) {
		case *BinaryServerProtocol:
			if self.slock.state != STATE_LEADER {
				if err == AGAIN {
					binaryServerProtocol := serverProtocol.(*BinaryServerProtocol)
					serverProtocol = NewTransparencyBinaryServerProtocol(self.slock, stream, binaryServerProtocol)
					for binaryServerProtocol.stream.readerBuffer.GetSize() >= 64 {
						buf, rerr := binaryServerProtocol.stream.ReadBytesSize(64)
						if rerr != nil {
							break
						}
						err = serverProtocol.ProcessParse(buf)
						if err != nil {
							break
						}
					}
					if err == nil {
						err = serverProtocol.Process()
					}
				} else {
					serverProtocol = NewTransparencyBinaryServerProtocol(self.slock, stream, serverProtocol.(*BinaryServerProtocol))
					err = serverProtocol.Process()
				}
			} else {
				err = serverProtocol.Process()
			}
		case *TextServerProtocol:
			if self.slock.state != STATE_LEADER {
				if err == AGAIN {
					textServerProtocol := serverProtocol.(*TextServerProtocol)
					transparencyServerProtocol := NewTransparencyTextServerProtocol(self.slock, stream, textServerProtocol)
					serverProtocol = transparencyServerProtocol
					err = transparencyServerProtocol.RunCommand()
					if err == nil {
						err = serverProtocol.Process()
					}
				} else {
					serverProtocol = NewTransparencyTextServerProtocol(self.slock, stream, serverProtocol.(*TextServerProtocol))
					if err == nil {
						err = serverProtocol.Process()
					}
				}
			} else {
				err = serverProtocol.Process()
			}
		case *TransparencyBinaryServerProtocol:
			if self.slock.state == STATE_LEADER {
				transparencyServerProtocol := serverProtocol.(*TransparencyBinaryServerProtocol)
				binaryServerProtocol := transparencyServerProtocol.serverProtocol
				if err == AGAIN {
					for binaryServerProtocol.stream.readerBuffer.GetSize() >= 64 {
						buf, rerr := binaryServerProtocol.stream.ReadBytesSize(64)
						if rerr != nil {
							break
						}
						err = binaryServerProtocol.ProcessParse(buf)
						if err != nil {
							break
						}
					}
					if err == nil {
						err = binaryServerProtocol.Process()
					}
				} else {
					err = binaryServerProtocol.Process()
				}
			} else {
				err = serverProtocol.Process()
			}
		case *TransparencyTextServerProtocol:
			if self.slock.state == STATE_LEADER {
				transparencyServerProtocol := serverProtocol.(*TransparencyTextServerProtocol)
				err = transparencyServerProtocol.serverProtocol.Process()
			} else {
				err = serverProtocol.Process()
			}
		default:
			err = serverProtocol.Process()
		}

		if err != nil {
			if err == AGAIN {
				continue
			}
			if err != io.EOF && !self.stoped {
				self.slock.Log().Errorf("Server protocol connection process error %v", err)
			}
		}
		break
	}

	err = serverProtocol.Close()
	if err != nil {
		self.slock.Log().Errorf("Server protocol connection close error %v", err)
	}
	self.slock.Log().Infof("Server protocol connection closed %s", serverProtocol.RemoteAddr().String())
}
