package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"net"

	"github.com/netbrain/dlog/log"

	"testing"
)

func TestCanWriteToServer(t *testing.T) {
	logWriter := dlog.NewWriter(&bytes.Buffer{})
	s := NewServer(logWriter, 0)
	go s.Start()
	client, err := net.Dial("tcp", s.Address().String())
	if err != nil {
		log.Fatal("dialing:", err)
	}
	payload := []byte{1, 2, 3}
	client.Write(payload)
}

func TestScanner(t *testing.T) {
	buffer := &bytes.Buffer{}

	payload := []byte{1, 2, 3}
	payloadLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(payloadLen, uint64(len(payload)))
	buffer.Write(payloadLen[:numBytes])
	buffer.Write(payload)

	scanner := bufio.NewScanner(buffer)
	scanner.Split(scanPayloadSplitFunc)

	for scanner.Scan() {
		bytes := scanner.Bytes()
		log.Println(bytes)
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}
