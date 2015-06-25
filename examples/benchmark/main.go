/*
Example of the dlog client in the form of a benchmarking test command line utility

Usage:
	./benchmark -hostsList=localhost:1234,localhost:1235 -numWrites=1000
*/
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
var servers []string

func init() {
	flag.StringVar(&hostsList, "hosts", "localhost:1234", "comma separated list of hosts to connect to")
	flag.IntVar(&numWrites, "numWrites", 10000, "how many writes to perform")
}

func main() {
	flag.PrintDefaults()
	flag.Parse()

	servers = strings.Split(hostsList, ",")

	writeBench()
	time.Sleep(time.Second)
	readBench()

}

func writeBench() {
	payload := make([]byte, 1024)
	rand.Read(payload)

	c := client.NewWriteClient(servers)
	defer c.Close()

	fmt.Println("\n Starting write benchmark")
	start := time.Now()
	for x := numWrites; x > 0; x-- {
		c.Write(payload)
	}
	elapsed := time.Since(start)

	fmt.Printf("Writing %d entries took %s\n\n", numWrites, elapsed)
	fmt.Printf("%f entries pr second\n", float64(numWrites)/elapsed.Seconds())

}

func readBench() {
	c := client.NewReadClient(servers)
	defer c.Close()

	fmt.Println("\n Starting read benchmark")

	start := time.Now()
	replayChan := c.Replay()
	numReads := 0
	for range replayChan {
		numReads++
	}
	elapsed := time.Since(start)
	fmt.Printf("Reading %d entries took %s\n\n", numReads, elapsed)
	fmt.Printf("%f entries pr second\n", float64(numReads)/elapsed.Seconds())
}
