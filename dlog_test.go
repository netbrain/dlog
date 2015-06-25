package dlog

import (
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"

	"testing"

	. "github.com/netbrain/dlog/testdata"
)

func TestCanWriteAndReadEntry(t *testing.T) {
	payload := []byte{1, 2, 3}
	logger, _ := NewLogger("")

	logEntry := NewLogEntryTestData().
		WithPayload(payload).
		Build()

	logger.Write(logEntry)
	logger.Close()

	entry := <-logger.Read()

	if !reflect.DeepEqual(entry, logEntry) {
		t.Fatalf("%v !=  %v", entry, logEntry)
	}
}

func TestCanReadEntries(t *testing.T) {
	payload := []byte{1, 2, 3}
	logger, _ := NewLogger("")
	for x := 0; x < 10; x++ {
		logger.Write(NewLogEntryTestData().Build())
	}
	c := logger.Read()
	numElems := 0
	for entry := range c {
		numElems++
		if !reflect.DeepEqual(entry.Payload(), payload) {
			t.Fatalf("Not equal, %v != %v", entry.Payload(), payload)
		}
	}

	if numElems != 10 {
		t.Fatalf("expected 10 entries but got %d", numElems)
	}

}

func benchFile() *os.File {
	file, err := ioutil.TempFile(os.TempDir(), "benchfile")
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func benchmarkWrite(b *testing.B, w io.Writer) {
	b.StopTimer()

	payload := make([]byte, 1024)
	rand.Read(payload)

	logger, _ := NewLogger("")
	defer logger.Close()

	logEntry := NewLogEntryTestData().
		WithPayload(payload).
		Build()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		logger.Write(logEntry)
	}

}

func BenchmarkWriteNoCompression(b *testing.B) {
	file := benchFile()
	benchmarkWrite(b, file)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithBesttGzipCompression(b *testing.B) {
	file := benchFile()
	w, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithFastestGzipCompression(b *testing.B) {
	file := benchFile()
	w, _ := gzip.NewWriterLevel(file, gzip.BestSpeed)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithFastestDeflateCompression(b *testing.B) {
	file := benchFile()
	w, _ := flate.NewWriter(file, flate.BestSpeed)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithBestDeflateCompression(b *testing.B) {
	file := benchFile()
	w, _ := flate.NewWriter(file, flate.BestSpeed)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithFastestZlibCompression(b *testing.B) {
	file := benchFile()
	w, _ := zlib.NewWriterLevel(file, zlib.BestSpeed)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}

func BenchmarkWriteWithBestZlibCompression(b *testing.B) {
	file := benchFile()
	w, _ := zlib.NewWriterLevel(file, zlib.BestCompression)
	benchmarkWrite(b, w)
	fi, _ := file.Stat()
	b.SetBytes(int64(int(fi.Size()) / 1024 / 1024))
}
