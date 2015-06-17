package server

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/netbrain/dlog/api"
	"github.com/netbrain/dlog/payload"

	"github.com/netbrain/dlog/log"
)

type Server struct {
	listener  net.Listener
	logWriter *dlog.LogWriter
	logReader *dlog.LogReader
	closed    bool
	port      int
}

func NewServer(logWriter *dlog.LogWriter, logReader *dlog.LogReader, port int) *Server {
	s := &Server{
		logWriter: logWriter,
		logReader: logReader,
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
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Split(payload.ScanPayloadSplitFunc)

	for scanner.Scan() {
		data := scanner.Bytes()

		request := &api.ClientRequest{}
		err := proto.Unmarshal(data, request)
		if err != nil {
			log.Fatal(err)
		}

		switch request.GetType() {
		case api.ClientRequest_WriteRequest:
			rawRequest, err := proto.GetExtension(request, api.E_ClientWriteRequest_Request)
			if err != nil {
				log.Fatal(err)
			}
			writeRequest := rawRequest.(*api.ClientWriteRequest)
			s.logWriter.Write(writeRequest.GetPayload())
		case api.ClientRequest_ReplayRequest:

			for entry := range s.logReader.Read() {
				replayResponse := &api.ServerResponse{
					Type: api.ServerResponse_ReplayResponse.Enum(),
				}

				if err != nil {
					e := err.Error()
					replayResponse.Status = api.ServerResponse_Error.Enum()
					replayResponse.Error = &e
				} else {
					replayResponse.Status = api.ServerResponse_OK.Enum()
					err := proto.SetExtension(replayResponse, api.E_ServerReplayResponse_Response, &api.ServerReplayResponse{
						Entry: entry,
					})
					if err != nil {
						log.Fatal(err)
					}
					data, err := proto.Marshal(replayResponse)
					if err != nil {
						log.Fatal(err)
					}
					data = payload.EncodePayload(data)
					conn.Write(data)
				}

			}
			payload.WriteEOT(conn)
		}
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
