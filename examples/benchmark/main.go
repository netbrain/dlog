package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/netbrain/dlog/client"
)

var hostsList string
var numWrites int

func init() {
	flag.StringVar(&hostsList, "hosts", "localhost:1234", "comma separated list of hosts to connect to")
	flag.IntVar(&numWrites, "numWrites", 1000, "how many writes to perform")
}

func main() {
	flag.PrintDefaults()
	flag.Parse()

	servers := strings.Split(hostsList, ",")
	c := client.NewClient(servers)
	defer c.Close()

	payload := make([]byte, 1024)
	rand.Read(payload)

	start := time.Now()
	for x := numWrites; x > 0; x-- {
		c.Write(payload)
	}
	elapsed := time.Since(start)
	fmt.Printf("Writing %d entries took %s\n\n", numWrites, elapsed)
	fmt.Printf("%f entries pr second\n", float64(numWrites)/elapsed.Seconds())
}
