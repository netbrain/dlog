/*
A simple command line utility to start a dlog server

Example usage
	./server -port=1234 -dir=/tmp
*/
package main

import (
	"flag"
	"log"

	"github.com/netbrain/dlog"
)

var port int
var dir string

func init() {
	flag.IntVar(&port, "port", 1234, "port number to use for incoming tcp connections")
	flag.StringVar(&dir, "dir", ".", "the directory to write log files to")
}

func main() {
	flag.PrintDefaults()
	flag.Parse()

	logger, err := dlog.NewLogger(".")
	if err != nil {
		log.Fatal(err)
	}
	s := dlog.NewServer(logger, port)
	s.Start()
}
