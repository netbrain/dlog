package client

import (
	"bufio"
	"io"
	"log"
	"net"
	"sync"

	"github.com/netbrain/dlog/encoder"
	"github.com/netbrain/dlog/model"
)

//ReadClient is the logging client which handles correctlu replaying the log
//and realtime subscribing to the log
type ReadClient struct {
	connectionPool *RoundRobinConnectionPool
}

//NewReadClient creates a new ReadClient instance
func NewReadClient(servers []string) *ReadClient {
	client := &ReadClient{
		connectionPool: NewRoundRobinConnectionPool(servers),
	}

	return client
}

//Replay replays the servers log entry by entry
func (r *ReadClient) Replay() <-chan []byte {
	outChan := make(chan []byte, 100)
	replayer := r.newReplayStreams()

	go func(outChan chan<- []byte) {
		defer close(outChan)
		for {
			entry, err := replayer.next()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			outChan <- entry.Payload()
		}
	}(outChan)
	return outChan

}

//Subscribe creates a subsciption on the log, which in realtime outputs all written log entries to the return channel from the time of subscription
func (r *ReadClient) Subscribe() <-chan model.LogEntry {
	subscribeChan := make(chan model.LogEntry)

	go func(chan<- model.LogEntry) {
		for _, conn := range r.connectionPool.AllConnections() {
			req := model.NewSubscribeRequest()
			if _, err := conn.Write(encoder.EncodePayload(req)); err != nil {
				log.Fatal(err)
			}
			//TODO replace with worker channel
			go r.connectionSubscriptionListenRoutine(conn, subscribeChan)
		}
	}(subscribeChan)
	return subscribeChan
}

func (r *ReadClient) connectionSubscriptionListenRoutine(conn net.Conn, subscribeChan chan<- model.LogEntry) {
	writeLogEntryToChan(subscribeChan, conn)
}

//Close closes the client for further reading
func (r *ReadClient) Close() {
	r.connectionPool.Close()
}

type replayStream struct {
	conn         net.Conn
	responseChan chan model.LogEntry
	once         *sync.Once
}

func (r *replayStream) next() (model.LogEntry, error) {
	r.once.Do(func() {
		r.sendReplayRequest()
		go r.readReplayResponse()
	})

	response, open := <-r.responseChan
	if !open {
		return nil, io.EOF
	}

	return response, nil
}

func writeLogEntryToChan(ch chan<- model.LogEntry, conn net.Conn) {
	defer close(ch)

	scanner := bufio.NewScanner(conn)
	scanner.Split(encoder.ScanPayloadSplitFunc)

	for scanner.Scan() {
		ch <- scanner.Bytes()
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}
