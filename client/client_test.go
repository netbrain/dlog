package client

import (
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/netbrain/dlog"
	"github.com/netbrain/dlog/model"
)

type serverTest struct {
	logger *dlog.Logger
	server *dlog.Server
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Flags() | log.Lshortfile)
	i := m.Run()
	os.Exit(i)
}

func createAndStartServer() *serverTest {
	var err error
	var logger *dlog.Logger

	if logger, err = dlog.NewLogger(""); err != nil {
		log.Fatal(err)
	}
	server := dlog.NewServer(logger, 0)

	s := &serverTest{
		server: server,
		logger: logger,
	}
	go s.server.Start()
	return s
}

func TestClientCanSubscribeToServer(t *testing.T) {
	numServers := 2
	addresses := make([]string, numServers)
	servers := make([]*serverTest, numServers)
	for x := 0; x < numServers; x++ {
		s := createAndStartServer()
		servers[x] = s
		addresses[x] = s.server.Address().String()
	}
	payload := []byte{1, 2, 3}

	writeClient := NewWriteClient(addresses)
	readClient := NewReadClient(addresses)
	subscription := readClient.Subscribe()
	time.Sleep(time.Second) //Todo have no idea why i need to sleep
	writeClient.Write(payload)
	var logEntry model.LogEntry
	select {
	case logEntry = <-subscription:
	case <-time.After(time.Millisecond * 200):
		t.Fatal("Timed out")
	}

	if logEntry == nil {
		t.Fatal("no logentry received")
	}

	if !reflect.DeepEqual(logEntry.Payload(), payload) {
		t.Fatal("not equal")
	}

}

func TestClientCanWriteAndReplay(t *testing.T) {

	numClients := 4
	numServers := 10

	addresses := make([]string, numServers)
	servers := make([]*serverTest, numServers)
	for x := 0; x < numServers; x++ {
		s := createAndStartServer()
		servers[x] = s
		addresses[x] = s.server.Address().String()
	}

	expected := make([]byte, 256)
	readChan := make(chan byte, 256)
	for x := 0; x < 256; x++ {
		expected[x] = byte(x)
		readChan <- expected[x]
	}
	close(readChan)

	wg := &sync.WaitGroup{}
	for x := 0; x < numClients; x++ {
		wg.Add(1)
		client := NewWriteClient(addresses)
		go func(client *WriteClient) {
			defer client.Close()
			defer wg.Done()
			for b := range readChan {
				client.write([]byte{b})
			}
		}(client)
	}
	wg.Wait()

	readClient := NewReadClient(addresses)
	i := 0
	for data := range readClient.Replay() {
		if !reflect.DeepEqual(data[0], expected[i]) {
			t.Fatalf("%v != %v", data[0], expected[i])
		}
		i++
	}

}
