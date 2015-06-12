package client

import (
	"bytes"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/netbrain/dlog/log"
	"github.com/netbrain/dlog/server"
)

type serverTest struct {
	buffer    *bytes.Buffer
	logWriter *dlog.LogWriter
	server    *server.Server
}

func createAndStartServer() *serverTest {
	buffer := &bytes.Buffer{}
	lw := dlog.NewWriter(buffer)
	server := server.NewServer(lw, 0)

	s := &serverTest{
		server:    server,
		buffer:    buffer,
		logWriter: lw,
	}
	go s.server.Start()
	log.Printf("Starting TCP server @ %v", s.server.Address())
	return s
}

func TestClientCanWriteToServer(t *testing.T) {
	log.SetFlags(log.Flags() | log.Lshortfile)

	numServers := 2
	addresses := make([]string, numServers)
	servers := make([]*serverTest, numServers)
	for x := 0; x < numServers; x++ {
		s := createAndStartServer()
		servers[x] = s
		addresses[x] = s.server.Address().String()
	}
	client1 := NewClient(addresses)

	client1.Write([]byte{0})
	client1.Write([]byte{1})
	client1.Write([]byte{2})
	client1.Write([]byte{3})
	client1.Close()

	expected := make(map[int][][]byte)
	expected[0] = [][]byte{
		[]byte{0},
		[]byte{2},
	}
	expected[1] = [][]byte{
		[]byte{1},
		[]byte{3},
	}

	time.Sleep(200 * time.Millisecond)
	for i, s := range servers {
		s.logWriter.Close()
		s.server.Stop()
		lr := dlog.NewReader(s.buffer)
		entry, _ := lr.ReadEntry()
		if !reflect.DeepEqual(entry.GetPayload(), expected[i][0]) {
			t.Fatalf("%v != %v", entry.GetPayload(), expected[i][0])
		}

		entry, _ = lr.ReadEntry()
		if !reflect.DeepEqual(entry.GetPayload(), expected[i][1]) {
			t.Fatalf("%v != %v", entry.GetPayload(), expected[i][1])
		}
	}

}
