package dlog

import (
	"bufio"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/netbrain/dlog/model"
)

//Client is the logging client which handles logwriting
//and correctly replays log entries
type Client struct {
	id          model.UUID
	connections []net.Conn
	numClients  uint8
	current     uint8
	max         uint8
	wChan       chan []byte
	quitChan    chan bool
	msgCount    uint64
}

//NewClient creates a new Client instance
func NewClient(servers []string) *Client {
	connections := make([]net.Conn, len(servers))
	for i, s := range servers {
		log.Printf("Connecting to server @ %s", s)
		conn, err := net.Dial("tcp", s)
		if err != nil {
			log.Fatalf("err connecting to '%s': %s", s, err)
		}
		if tcpcon, ok := conn.(*net.TCPConn); ok {
			tcpcon.SetKeepAlive(true)
		}
		connections[i] = conn

	}
	numClients := uint8(len(connections))
	client := &Client{
		id:          model.NewUUID(),
		connections: connections,
		numClients:  numClients,
		max:         math.MaxUint8 / numClients * numClients,
		wChan:       make(chan []byte),
		quitChan:    make(chan bool),
	}

	go func(c *Client) {
		for {
			select {
			case data := <-c.wChan:
				if err := c.write(data); err != nil {
					log.Fatal(err)
				}
			case <-c.quitChan:
				c.close()
				return
			}
		}
	}(client)

	return client
}

//Write adds data to the write queue
func (c *Client) Write(data []byte) {
	c.wChan <- data
}

func (c *Client) write(data []byte) error {
	msgCount := atomic.AddUint64(&c.msgCount, 1)

	md := model.NewMetaData(c.id, msgCount, model.NewUUID()) //TODO  transactionid should be supplied
	entry := model.NewLogEntry(md, data)
	request := model.NewWriteRequest(entry)

	conn := c.nexConnection()
	if _, err := conn.Write(EncodePayload(request)); err != nil {
		return err
	}

	return nil
}

//Close closes the client for further reading/writing
func (c *Client) Close() {
	c.quitChan <- true
}

func (c *Client) close() {
	close(c.wChan)
	close(c.quitChan)
	for _, conn := range c.connections {
		conn.Close()
	}
}

func (c *Client) incrementCurrent() {
	c.current++
	if c.current > c.max {
		c.current = 0
	}
}

func (c *Client) nexConnection() net.Conn {
	defer c.incrementCurrent()
	return c.connections[c.current%c.numClients]
}

//Replay replays the servers log entry by entry
func (c *Client) Replay() <-chan []byte {
	outChan := make(chan []byte, 100)
	replayer := c.newReplayStreams()

	go func(outChan chan<- []byte) {
		defer close(outChan)
		for {
			entry, err := replayer.next()
			if err == io.EOF {
				log.Printf("Nothing left to replay, EOF")
				break
			} else if err != nil {
				log.Fatal(err)
			}
			outChan <- entry.Payload()
		}
	}(outChan)
	return outChan

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

func newReplayStream(conn net.Conn) *replayStream {
	r := &replayStream{
		conn:         conn,
		responseChan: make(chan model.LogEntry),
		once:         &sync.Once{},
	}
	return r
}

func (r *replayStream) sendReplayRequest() error {
	request := model.NewReplayRequest()
	_, err := r.conn.Write(EncodePayload(request))
	return err
}

func (r *replayStream) readReplayResponse() {
	defer close(r.responseChan)

	scanner := bufio.NewScanner(r.conn)
	scanner.Split(ScanPayloadSplitFunc)

	for scanner.Scan() {
		r.responseChan <- model.LogEntry(scanner.Bytes())
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}

type replayStreams struct {
	streams []*replayStream
	entries map[int]model.LogEntry
}

func (c *Client) newReplayStreams() *replayStreams {
	streams := make([]*replayStream, len(c.connections))
	for i, conn := range c.connections {
		streams[i] = newReplayStream(conn)
	}
	return &replayStreams{
		streams: streams,
	}
}

func (r *replayStreams) next() (model.LogEntry, error) {
	entryIndex := -1

	if r.entries == nil {
		r.entries = make(map[int]model.LogEntry)

		for i, stream := range r.streams {
			e, err := stream.next()
			if err != nil {
				return nil, err
			}

			r.entries[i] = e
		}
	}

	for i, e := range r.entries {
		if e == nil {
			continue
		} else if entryIndex == -1 {
			entryIndex = i
		} else if e.MetaData().ClientMessageNumber() < r.entries[entryIndex].MetaData().ClientMessageNumber() {
			entryIndex = i
		}
	}

	if entryIndex == -1 {
		return nil, io.EOF
	}
	stream := r.streams[entryIndex]
	nextEntry, err := stream.next()
	if err != nil && err != io.EOF {
		return nil, err
	}

	entry := r.entries[entryIndex]
	r.entries[entryIndex] = nextEntry

	return entry, nil

}
