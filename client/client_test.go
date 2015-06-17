package client

import (
	"bytes"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/netbrain/dlog/log"
	"github.com/netbrain/dlog/server"
)

type serverTest struct {
	buffer    *bytes.Buffer
	logWriter *dlog.LogWriter
	logReader *dlog.LogReader
	server    *server.Server
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Flags() | log.Lshortfile)
	i := m.Run()
	os.Exit(i)
}

func createAndStartServer() *serverTest {
	buffer := &bytes.Buffer{}
	lw := dlog.NewWriter(buffer)
	lr := dlog.NewReader(buffer)
	server := server.NewServer(lw, lr, 0)

	s := &serverTest{
		server:    server,
		buffer:    buffer,
		logWriter: lw,
		logReader: lr,
	}
	go s.server.Start()
	log.Printf("Starting TCP server @ %v", s.server.Address())
	return s
}

func TestClientCanWriteToServer(t *testing.T) {
	numClients := 4
	numServers := 1

	addresses := make([]string, numServers)
	servers := make([]*serverTest, numServers)
	for x := 0; x < numServers; x++ {
		s := createAndStartServer()
		servers[x] = s
		addresses[x] = s.server.Address().String()
	}

	readChan := make(chan byte)
	go func() {
		defer close(readChan)
		for x := 1; x <= 100; x++ {
			readChan <- byte(x)
		}
	}()
	wg := &sync.WaitGroup{}
	for x := 0; x < numClients; x++ {
		wg.Add(1)
		client := NewClient(addresses)
		go func(client *Client) {
			defer client.Close()
			defer wg.Done()
			for b := range readChan {
				client.write([]byte{b})
			}
		}(client)
	}
	wg.Wait()

	readClient := NewClient(addresses)
	for data := range readClient.Replay() {
		log.Println(data)
	}

}
