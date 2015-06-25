package dlog

import (
	"bufio"
	"compress/flate"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	. "github.com/netbrain/dlog/encoder"
	"github.com/netbrain/dlog/model"
)

//Logger handles reads and writes to the logfile
type Logger struct {
	wg    sync.WaitGroup
	wChan chan model.LogEntry
	wFile *os.File
}

//NewLogger creates a new Logger instance
func NewLogger(directory string) (*Logger, error) {
	var err error
	if directory == "" {
		if directory, err = ioutil.TempDir("", "dlog"); err != nil {
			return nil, err
		}
	}
	os.MkdirAll(directory, 0755)
	filePath := filepath.Join(directory, "dlog.bin")

	wFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	w, _ := flate.NewWriter(wFile, flate.BestCompression)

	l := &Logger{
		wChan: make(chan model.LogEntry, 1000),
		wFile: wFile,
	}

	go l.writeRoutine(w)

	return l, nil
}

//Write writes a LogEntry to the log
func (l *Logger) Write(logEntry model.LogEntry) {
	l.wg.Add(1)
	l.wChan <- logEntry
}

//Close closes the log
func (l *Logger) Close() {
	close(l.wChan)
	l.wg.Wait()

}

//Read returns a channel which logentries are appended to
//in sequential order
func (l *Logger) Read() <-chan model.LogEntry {
	c := make(chan model.LogEntry)

	go func(c chan<- model.LogEntry) {
		defer close(c)
		rFile, err := os.OpenFile(l.wFile.Name(), os.O_RDONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer rFile.Close()

		reader := flate.NewReader(rFile)
		defer reader.Close()

		scanner := bufio.NewScanner(reader)
		scanner.Split(ScanPayloadSplitFunc)
		for scanner.Scan() {
			entry := model.LogEntry(scanner.Bytes())
			c <- entry
		}
		err = scanner.Err()
		if err != nil && err != io.ErrUnexpectedEOF {
			log.Fatal(err)
		}
	}(c)
	return c
}

func (l *Logger) writeRoutine(w io.Writer) {
	for entry := range l.wChan {
		l.writeEntry(w, entry)
	}

	if closer, ok := w.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			log.Fatal(err)
		}
	}

	if err := l.wFile.Close(); err != nil {
		log.Fatal(err)
	}
}

func (l *Logger) writeEntry(w io.Writer, entry model.LogEntry) {
	if entry == nil {
		return
	}
	if _, err := w.Write(EncodePayload(entry)); err != nil {
		log.Fatal(err)
	}

	if flusher, ok := w.(model.Flusher); ok {
		if err := flusher.Flush(); err != nil {
			log.Fatal(err)
		}
	}

	l.wg.Done()
}
