package dlog

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"testing"
)

func TestCanWriteAndReadEntry(t *testing.T) {
	payload := []byte{1, 2, 3}
	buf := &bytes.Buffer{}
	lw := NewWriter(buf)
	lw.Write(payload)
	lw.Close()

	lr := NewReader(buf)
	entry, err := lr.ReadEntry()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(entry.GetPayload(), payload) {
		t.Fatalf("%v !=  %v", entry.GetPayload(), payload)
	}

}

func BenchmarkReadFromFile(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)

	file, _ := ioutil.TempFile(os.TempDir(), "read")
	defer file.Close()
	lw := NewWriter(file)
	lr := NewReader(file)
	fmt.Println(file.Name())

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}
	lw.Close()

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lr.ReadEntry()
	}

}

func BenchmarkWriteToFile(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)

	file, _ := ioutil.TempFile(os.TempDir(), "bin")
	defer file.Close()
	lw := NewWriter(file)
	fmt.Println(file.Name())
	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}

}

func BenchmarkWriteToGZFileDefault(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)
	file, _ := ioutil.TempFile(os.TempDir(), "gzd")
	defer file.Close()
	w, _ := gzip.NewWriterLevel(file, gzip.DefaultCompression)
	defer w.Close()
	lw := NewWriter(w)
	fmt.Println(file.Name())
	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}

}

func BenchmarkWriteToGZFileNone(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)
	file, _ := ioutil.TempFile(os.TempDir(), "gzn")
	defer file.Close()
	w, _ := gzip.NewWriterLevel(file, gzip.NoCompression)
	defer w.Close()
	lw := NewWriter(w)
	fmt.Println(file.Name())
	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}

}

func BenchmarkWriteToGZFileBestSpeed(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)
	file, _ := ioutil.TempFile(os.TempDir(), "gzs")
	defer file.Close()
	w, _ := gzip.NewWriterLevel(file, gzip.BestSpeed)
	defer w.Close()
	lw := NewWriter(w)
	fmt.Println(file.Name())
	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}

}

func BenchmarkWriteToGZFileBestCompression(b *testing.B) {
	b.SetParallelism(1)
	payload := make([]byte, 4096)
	rand.Read(payload)
	file, _ := ioutil.TempFile(os.TempDir(), "gzc")
	defer file.Close()
	w, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
	defer w.Close()
	lw := NewWriter(w)
	fmt.Println(file.Name())
	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lw.Write(payload)
	}

}
