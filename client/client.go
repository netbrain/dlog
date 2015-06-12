package client

import (
	"encoding/binary"
	"log"
	"math"
	"net"
	"sync"
)

type Client struct {
	connections []net.Conn
	numClients  uint8
	current     uint8
	max         uint8
	mutex       *sync.Mutex
}

func NewClient(servers []string) *Client {
	connections := make([]net.Conn, len(servers))
	for i, s := range servers {
		log.Printf("Connecting to server @ %s", s)
		conn, err := net.Dial("tcp", s)
		if err != nil {
			log.Fatalf("err connecting to '%s': %s", s, err)
		}
		if tcpcon, ok := conn.(*net.TCPConn); ok {
			tcpcon.CloseRead()
			tcpcon.SetKeepAlive(true)
		}
		connections[i] = conn

	}
	numClients := uint8(len(connections))
	return &Client{
		connections: connections,
		numClients:  numClients,
		max:         math.MaxUint8 / numClients * numClients,
		mutex:       &sync.Mutex{},
	}
}

func (c *Client) Write(payload []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	conn := c.nexConnection()

	payloadLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(payloadLen, uint64(len(payload)))

	conn.Write(payloadLen[:numBytes])
	conn.Write(payload)
	//log.Printf("Wiring %d bytes to connection %s<->%s", len(payload), conn.LocalAddr(), conn.RemoteAddr())
}

func (c *Client) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
