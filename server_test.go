package dlog

import (
	"bufio"
	"bytes"
	"log"
	"net"

	"github.com/netbrain/dlog/model"

	"github.com/netbrain/dlog/encoder"
	. "github.com/netbrain/dlog/testdata"

	"testing"
)

var buffer *bytes.Buffer
var logger *Logger
var server *Server

func setup() {
	var err error
	buffer = &bytes.Buffer{}
	if logger, err = NewLogger(""); err != nil {
		log.Fatal(err)
	}
	server = NewServer(logger, 0)
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
	request := NewRequestTestData().
		WithLogEntry(
		NewLogEntryTestData().
			WithPayload(data).
			Build()).
		Build()
	data = encoder.EncodePayload(request)
	conn.Write(data)
}

func sendReplayRequest(conn net.Conn) {
	request := NewRequestTestData().
		WithType(model.TypeReplayRequest).
		Build()
	data := encoder.EncodePayload(request)
	conn.Write(data)
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

	expected := 100
	for x := 0; x < expected; x++ {
		sendWriteRequest(conn, []byte{byte(x)})
	}
	logger.Close()
	sendReplayRequest(conn)

	scanner := bufio.NewScanner(conn)
	scanner.Split(encoder.ScanPayloadSplitFunc)
	actual := 0
	for scanner.Scan() {
		logEntry := model.LogEntry(scanner.Bytes())
		payload := logEntry.Payload()
		if byte(actual) != payload[0] {
			t.Fatalf("%v != %v", byte(actual), payload[0])
		}
		actual++
	}

	if actual != expected {
		t.Fatalf("Expected %d but got %d", expected, actual)
	}

	if scanner.Err() != nil {
		t.Fatal(scanner.Err())
	}

}
