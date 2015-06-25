package client

import (
	"log"
	"math"
	"net"
)

//RoundRobinConnectionPool holds a number of connections and data needed for round robin mechanics
type RoundRobinConnectionPool struct {
	connections []net.Conn
	numClients  uint8
	current     uint8
	max         uint8
}

//NewRoundRobinConnectionPool creates a connection pool which retrieves connections in a round robin fashion
func NewRoundRobinConnectionPool(servers []string) *RoundRobinConnectionPool {
	connections := make([]net.Conn, len(servers))
	for i, s := range servers {
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
	pool := &RoundRobinConnectionPool{
		connections: connections,
		numClients:  numClients,
		max:         math.MaxUint8 / numClients * numClients,
	}

	return pool
}

func (r *RoundRobinConnectionPool) incrementCurrent() {
	r.current++
	if r.current > r.max {
		r.current = 0
	}
}

//Connection returns the next connection in the round robin order
func (r *RoundRobinConnectionPool) Connection() net.Conn {
	defer r.incrementCurrent()
	return r.connections[r.current%r.numClients]
}

//AllConnections returns all connections in this pool
func (r *RoundRobinConnectionPool) AllConnections() []net.Conn {
	return r.connections
}

//Close closes all connections in this pool
func (r *RoundRobinConnectionPool) Close() {
	for _, conn := range r.connections {
		conn.Close()
	}
}

//Len returns the number of connections in this pool
func (r *RoundRobinConnectionPool) Len() int {
	return len(r.connections)
}
