package dlog

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/netbrain/dlog/model"
)

//Server handles the server side functionality
type Server struct {
	listener net.Listener
	logger   *Logger
	closed   bool
	port     int
}

//NewServer creates a new Server instance
func NewServer(logger *Logger, port int) *Server {
	s := &Server{
		logger: logger,
		port:   port,
	}
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	s.listener = l
	return s
}

//Start stars the server
func (s *Server) Start() {
	s.listen()
}

func (s *Server) listen() {
	//log.Printf("Listening on %s", s.listener.Addr())
	for {
		conn, err := s.listener.Accept()

		if s.closed {
			//log.Println("Server closed")
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		//log.Printf("Accepted connection from %v", conn.RemoteAddr())
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
			logEntry, err := request.LogEntry()
			if err != nil {
				log.Fatal(err)
			}

			s.logger.Write(logEntry)
		case model.TypeReplayRequest:
			for logEntry := range s.logger.Read() {
				conn.Write(EncodePayload(logEntry))
			}
			WriteEOT(conn)
		default:
			log.Fatalf("Unknown request type: %b", request.Type())
		}
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}

}

//Stop stops the server
func (s *Server) Stop() {
	s.listener.Close()
	s.closed = true
}

//Address returns the servers address the server is listening on
func (s *Server) Address() net.Addr {
	return s.listener.Addr()
}
