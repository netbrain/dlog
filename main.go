package main

import (
	"flag"
	"log"
	"os"

	"github.com/netbrain/dlog/log"

	"github.com/netbrain/dlog/server"
)

var port int
var logfile string

func init() {
	flag.IntVar(&port, "port", 1234, "port number to use for incoming tcp connections")
	flag.StringVar(&logfile, "logfile", "logfile.log", "the logfile to write to")
}

func main() {
	flag.PrintDefaults()
	flag.Parse()

	file, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	logwriter := dlog.NewWriter(file)
	defer file.Close()
	defer logwriter.Close()

	s := server.NewServer(logwriter, port)
	s.Start()
}
