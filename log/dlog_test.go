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
	entry := <-lr.Read()

	if !reflect.DeepEqual(entry.GetPayload(), payload) {
		t.Fatalf("%v !=  %v", entry.GetPayload(), payload)
	}
}

func TestCanReadEntries(t *testing.T) {
	payload := []byte{1, 2, 3}
	buf := &bytes.Buffer{}
	lw := NewWriter(buf)
	for x := 0; x < 10; x++ {
		lw.Write(payload)
	}

	lr := NewReader(buf)
	c := lr.Read()
	numElems := 0
	for entry := range c {
		numElems++
		if !reflect.DeepEqual(entry.GetPayload(), payload) {
			t.Fatalf("Not equal, %v != %v", entry.GetPayload(), payload)
		}
	}

	if numElems != 10 {
		t.Fatalf("expected 10 entries but got %d", numElems)
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

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-lr.Read()
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
