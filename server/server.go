package server

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/netbrain/dlog/log"
	"github.com/netbrain/dlog/scanner_util"
)

type Server struct {
	listener  net.Listener
	logWriter *dlog.LogWriter
	closed    bool
	port      int
}

func NewServer(logWriter *dlog.LogWriter, port int) *Server {
	s := &Server{
		logWriter: logWriter,
		port:      port,
	}
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	s.listener = l
	return s
}

func (s *Server) Start() {
	s.listen()
}

func (s *Server) listen() {
	log.Printf("Listening on %s", s.listener.Addr())
	for {
		conn, err := s.listener.Accept()

		if s.closed {
			log.Println("Server closed")
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Accepted connection from %v", conn.RemoteAddr())
		if err != nil {
			log.Fatalf("Error when accepting connection: %s", err)
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Split(scanner_util.ScanPayloadSplitFunc)

	for scanner.Scan() {
		s.logWriter.Write(scanner.Bytes())
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}

func (s *Server) Stop() {
	s.listener.Close()
	s.closed = true
}

func (s *Server) Address() net.Addr {
	return s.listener.Addr()
}
