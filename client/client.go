package client

import (
	"encoding/binary"
	"log"
	"math"
	"net"
)

type Client struct {
	connections []net.Conn
	numClients  uint8
	current     uint8
	max         uint8
	wChan       chan []byte
	quitChan    chan bool
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
			case payload := <-c.wChan:
				c.write(payload)
			case <-c.quitChan:
				c.close()
				return
			}
		}
	}(client)

	return client
}

func (c *Client) Write(payload []byte) {
	c.wChan <- payload
}

func (c *Client) write(payload []byte) {
	conn := c.nexConnection()

	payloadLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(payloadLen, uint64(len(payload)))

	conn.Write(payloadLen[:numBytes])
	conn.Write(payload)
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
