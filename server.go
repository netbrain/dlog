package dlog

import (
	"bufio"
	"fmt"
	"log"
	"net"

	. "github.com/netbrain/dlog/encoder"

	"github.com/netbrain/dlog/model"
)

//Server handles the server side functionality
type Server struct {
	listener      net.Listener
	subscribeChan chan net.Conn
	subscribers   []net.Conn
	logger        *Logger
	closed        bool
	port          int
}

//NewServer creates a new Server instance
func NewServer(logger *Logger, port int) *Server {
	s := &Server{
		logger:        logger,
		port:          port,
		subscribeChan: make(chan net.Conn),
		subscribers:   make([]net.Conn, 0),
	}
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	s.listener = l
	go s.subscriptionRoutine()
	return s
}

//Start stars the server
func (s *Server) Start() {
	s.listen()
}

func (s *Server) listen() {
	for {
		conn, err := s.listener.Accept()

		if s.closed {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		if err != nil {
			log.Fatalf("Error when accepting connection: %s", err)
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanPayloadSplitFunc)

	for scanner.Scan() {
		request := model.Request(scanner.Bytes())
		switch request.Type() {
		case model.TypeWriteRequest:
			s.write(request)
		case model.TypeReplayRequest:
			s.replay(conn)
		case model.TypeSubscribeRequest:
			s.subscribe(conn)
		default:
			log.Fatalf("Unknown request type: %b", request.Type())
		}
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}

}

func (s *Server) write(request model.Request) {
	logEntry, err := request.LogEntry()
	if err != nil {
		log.Fatal(err)
	}

	s.logger.Write(logEntry)
	s.notify(logEntry)
}

func (s *Server) replay(conn net.Conn) {
	for logEntry := range s.logger.Read() {
		conn.Write(EncodePayload(logEntry))
	}
	WriteEOT(conn)
}

func (s *Server) subscriptionRoutine() {
	for subscriber := range s.subscribeChan {
		s.subscribers = append(s.subscribers, subscriber)
	}
}

func (s *Server) subscribe(conn net.Conn) {
	s.subscribeChan <- conn
	select {} //block forever
}

func (s *Server) notify(logEntries ...model.LogEntry) {
	for _, conn := range s.subscribers {
		for _, logEntry := range logEntries {
			conn.Write(EncodePayload(logEntry))
		}
		WriteEOT(conn)
	}
}

//Stop stops the server
func (s *Server) Stop() {
	close(s.subscribeChan)
	s.listener.Close()
	s.closed = true
}

//Address returns the servers address the server is listening on
func (s *Server) Address() net.Addr {
	return s.listener.Addr()
}
