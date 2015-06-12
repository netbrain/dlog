package dlog

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"log"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/netbrain/dlog/log/entry"
	"github.com/netbrain/dlog/scanner_util"
)

type LogWriter struct {
	wChan      chan *entry.Entry
	writer     io.Writer
	gzipWriter *gzip.Writer
}

type LogReader struct {
	reader     io.Reader
	gzipReader *gzip.Reader
	scanner    *bufio.Scanner
}

func NewWriter(w io.Writer) *LogWriter {
	gw := gzip.NewWriter(w)
	l := &LogWriter{
		wChan:      make(chan *entry.Entry),
		writer:     w,
		gzipWriter: gw,
	}

	go func(r <-chan *entry.Entry, w io.Writer) {
		for entry := range r {
			if entry == nil {
				log.Println("Cannot write nil value")
				continue
			}
			payload, err := proto.Marshal(entry)
			if err != nil {
				log.Fatal(err)
			}

			payloadLenBuf := make([]byte, binary.MaxVarintLen32)
			payloadLen := uint64(len(payload))
			numBytes := binary.PutUvarint(payloadLenBuf, payloadLen)

			w.Write(payloadLenBuf[:numBytes])
			w.Write(payload)

		}
	}(l.wChan, l.gzipWriter)

	return l
}

func NewReader(r io.Reader) *LogReader {
	gzReader, err := gzip.NewReader(r)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(gzReader)
	scanner.Split(scanner_util.ScanPayloadSplitFunc)

	logReader := &LogReader{
		reader:     r,
		gzipReader: gzReader,
		scanner:    scanner,
	}
	return logReader

}

func (l *LogWriter) Write(payload []byte) (int, error) {
	timestamp := time.Now().UnixNano()
	l.wChan <- &entry.Entry{
		Timestamp: &timestamp,
		Payload:   payload,
	}
	return 0, nil
}

func (l *LogWriter) Close() error {
	defer close(l.wChan)
	if err := l.gzipWriter.Close(); err != nil {
		return err
	}

	if wc, ok := l.writer.(io.WriteCloser); ok {
		if err := wc.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *LogReader) ReadEntry() (*entry.Entry, error) {
	var err error
	for l.scanner.Scan() {
		entry := &entry.Entry{}
		err = proto.Unmarshal(l.scanner.Bytes(), entry)
		return entry, err
	}

	return nil, l.scanner.Err()
}
