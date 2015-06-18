package dlog

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"log"

	"github.com/golang/protobuf/proto"

	"github.com/netbrain/dlog/api"
	"github.com/netbrain/dlog/payload"
)

type LogWriter struct {
	wChan chan *api.LogEntry
}

type LogReader struct {
	reader io.Reader
}

func NewWriter(w io.Writer) *LogWriter {
	l := &LogWriter{
		wChan: make(chan *api.LogEntry),
	}

	go l.writeRoutine(w)

	return l
}

func (l *LogWriter) writeRoutine(w io.Writer) {
	gw := gzip.NewWriter(w)

	for entry := range l.wChan {
		l.writeEntry(gw, entry)
	}

	if err := gw.Close(); err != nil {
		log.Fatal(err)
	}

	if wc, ok := w.(io.Closer); ok {
		if err := wc.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func (l *LogWriter) writeEntry(w *gzip.Writer, entry *api.LogEntry) {
	if entry == nil {
		log.Println("Cannot write nil value")
		return
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		log.Fatal(err)
	}

	payloadLenBuf := make([]byte, binary.MaxVarintLen32)
	payloadLen := uint64(len(payload))
	numBytes := binary.PutUvarint(payloadLenBuf, payloadLen)

	_, err = w.Write(append(payloadLenBuf[:numBytes], payload...))
	w.Flush()
}

func NewReader(r io.Reader) *LogReader {
	logReader := &LogReader{
		reader: r,
	}
	return logReader

}

func (l *LogWriter) Write(clientId, clientCount int64, payload []byte) (int, error) {
	l.wChan <- &api.LogEntry{
		ClientId:    &clientId,
		ClientCount: &clientCount,
		Payload:     payload,
	}
	return 0, nil
}

func (l *LogWriter) Close() error {
	close(l.wChan)
	return nil
}

func (l *LogReader) Read() <-chan *api.LogEntry {
	c := make(chan *api.LogEntry)
	if seeker, ok := l.reader.(io.Seeker); ok {
		seeker.Seek(0, 0)
	}

	gzReader, err := gzip.NewReader(l.reader)
	if err == io.EOF {
		close(c)
		return c
	} else if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(gzReader)
	scanner.Split(payload.ScanPayloadSplitFunc)

	go func(c chan<- *api.LogEntry, scanner *bufio.Scanner) {
		defer close(c)
		defer gzReader.Close()
		for scanner.Scan() {
			entry := &api.LogEntry{}
			err := proto.Unmarshal(scanner.Bytes(), entry)
			if err != nil {
				log.Fatal(err)
			}
			c <- entry
		}
		err := scanner.Err()
		if err == io.ErrUnexpectedEOF {
			log.Println("Unexpected EOF: might occur when writer is open")
		} else if err != nil {
			log.Fatal(err)
		}
	}(c, scanner)
	return c
}
