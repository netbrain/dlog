package client

import (
	"bufio"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"

	"github.com/netbrain/dlog/api"
	"github.com/netbrain/dlog/payload"
)

type Client struct {
	connections []net.Conn
	numClients  uint8
	current     uint8
	max         uint8
	wChan       chan []byte
	quitChan    chan bool
	msgCount    int64
}

func NewClient(servers []string) *Client {
	connections := make([]net.Conn, len(servers))
	for i, s := range servers {
		//log.Printf("Connecting to server @ %s", s)
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

func (c *Client) Write(data []byte) {
	c.wChan <- data
}

func (c *Client) write(data []byte) error {
	msgCount := atomic.AddInt64(&c.msgCount, 1)
	writeRequest := &api.ClientRequest{
		Type: api.ClientRequest_WriteRequest.Enum(),
	}
	err := proto.SetExtension(writeRequest, api.E_ClientWriteRequest_Request, &api.ClientWriteRequest{
		Payload: data,
		Count:   &msgCount,
	})
	if err != nil {
		panic(err)
	}
	data, err = proto.Marshal(writeRequest)
	if err != nil {
		return err
	}

	conn := c.nexConnection()
	if _, err := conn.Write(payload.EncodePayload(data)); err != nil {
		return err
	}

	return nil
}

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
			outChan <- entry.GetPayload()
		}
	}(outChan)
	return outChan

}

type replayStream struct {
	conn         net.Conn
	responseChan chan *api.ServerResponse
	once         *sync.Once
}

func (r *replayStream) next() (*api.LogEntry, error) {
	r.once.Do(func() {
		r.sendReplayRequest()
		go r.readReplayResponse()
	})

	response, open := <-r.responseChan
	if !open {
		return nil, io.EOF
	}
	if response.GetStatus() == api.ServerResponse_Error {
		return nil, errors.New(response.GetError())
	}
	extension, err := proto.GetExtension(response, api.E_ServerReplayResponse_Response)
	if err != nil {
		log.Fatal(err)
	}
	replayResponse := extension.(*api.ServerReplayResponse)

	return replayResponse.GetEntry(), nil
}

func newReplayStream(conn net.Conn) *replayStream {
	r := &replayStream{
		conn:         conn,
		responseChan: make(chan *api.ServerResponse),
		once:         &sync.Once{},
	}
	return r
}

func (r *replayStream) sendReplayRequest() error {
	replayRequest := &api.ClientRequest{
		Type: api.ClientRequest_ReplayRequest.Enum(),
	}
	err := proto.SetExtension(replayRequest, api.E_ClientReplayRequest_Request, &api.ClientReplayRequest{})
	if err != nil {
		return err
	}
	data, err := proto.Marshal(replayRequest)
	if err != nil {
		return err
	}

	data = payload.EncodePayload(data)
	if _, err := r.conn.Write(data); err != nil {
		return err
	}

	return nil
}

func (r *replayStream) readReplayResponse() {
	defer close(r.responseChan)

	scanner := bufio.NewScanner(r.conn)
	scanner.Split(payload.ScanPayloadSplitFunc)

	for scanner.Scan() {
		response := &api.ServerResponse{}
		if err := proto.Unmarshal(scanner.Bytes(), response); err != nil {
			log.Fatal(err)
		}
		r.responseChan <- response
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}

type replayStreams struct {
	streams []*replayStream
	entries map[int]*api.LogEntry
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

func (r *replayStreams) next() (*api.LogEntry, error) {
	entryIndex := -1

	if r.entries == nil {
		r.entries = make(map[int]*api.LogEntry)

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
		} else if e.GetClientCount() < r.entries[entryIndex].GetClientCount() {
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
