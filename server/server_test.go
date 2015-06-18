package server

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"os"

	"github.com/netbrain/dlog/payload"

	"github.com/golang/protobuf/proto"
	"github.com/netbrain/dlog/api"
	"github.com/netbrain/dlog/log"

	"testing"
)

var buffer *bytes.Buffer
var logWriter *dlog.LogWriter
var logReader *dlog.LogReader
var server *Server

func TestMain(m *testing.M) {
	log.SetFlags(log.Flags() | log.Lshortfile)
	i := m.Run()
	os.Exit(i)
}

func setup() {
	buffer = &bytes.Buffer{}
	logWriter = dlog.NewWriter(buffer)
	logReader = dlog.NewReader(buffer)
	server = NewServer(logWriter, logReader, 0)
	go server.Start()
}

func teardown() {
	server.Stop()
}

func dial() net.Conn {
	client, err := net.Dial("tcp", server.Address().String())
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func sendWriteRequest(conn net.Conn, data []byte) {
	writeRequest := &api.ClientRequest{
		Type: api.ClientRequest_WriteRequest.Enum(),
	}
	count := int64(1)
	err := proto.SetExtension(writeRequest, api.E_ClientWriteRequest_Request, &api.ClientWriteRequest{
		Payload: data,
		Count:   &count,
	})
	if err != nil {
		log.Fatal(err)
	}
	data, err = proto.Marshal(writeRequest)
	if err != nil {
		log.Fatal(err)
	}

	data = payload.EncodePayload(data)

	if _, err := conn.Write(data); err != nil {
		log.Fatal(err)
	}
}

func sendReplayRequest(conn net.Conn) {
	replayRequest := &api.ClientRequest{
		Type: api.ClientRequest_ReplayRequest.Enum(),
	}
	err := proto.SetExtension(replayRequest, api.E_ClientReplayRequest_Request, &api.ClientReplayRequest{})
	if err != nil {
		log.Fatal(err)
	}
	data, err := proto.Marshal(replayRequest)
	if err != nil {
		log.Fatal(err)
	}

	data = payload.EncodePayload(data)
	if _, err := conn.Write(data); err != nil {
		log.Fatal(err)
	}
}

func TestCanSendWriteRequest(t *testing.T) {
	setup()
	defer teardown()

	sendWriteRequest(dial(), []byte{1, 2, 3})
}

func TestCanSendReplayRequest(t *testing.T) {
	setup()
	defer teardown()

	conn := dial()

	for x := 0; x < 100; x++ {
		sendWriteRequest(conn, []byte{byte(x)})
	}

	sendReplayRequest(conn)

	scanner := bufio.NewScanner(conn)
	scanner.Split(payload.ScanPayloadSplitFunc)
	expected := 0
	for scanner.Scan() {
		response := &api.ServerResponse{}
		if err := proto.Unmarshal(scanner.Bytes(), response); err != nil {
			t.Fatal(err)
		}

		rawReplayResponse, err := proto.GetExtension(response, api.E_ServerReplayResponse_Response)
		if err != nil {
			log.Fatal(err)
		}
		replayResponse := rawReplayResponse.(*api.ServerReplayResponse)

		logEntry := replayResponse.GetEntry()
		if byte(expected) != logEntry.Payload[0] {
			log.Fatalf("%v != %v", byte(expected), logEntry.Payload[0])
		}
		expected++
	}

	if expected != 100 {
		t.Fatal("Expected 100")
	}

	if scanner.Err() != nil {
		t.Fatal(scanner.Err())
	}

}
