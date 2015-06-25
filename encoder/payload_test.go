package encoder

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"reflect"
	"testing"
)

func TestScanner(t *testing.T) {
	buffer := &bytes.Buffer{}

	payload := []byte{1, 2, 3}
	payloadLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(payloadLen, uint64(len(payload)))
	buffer.Write(payloadLen[:numBytes])
	buffer.Write(payload)

	scanner := bufio.NewScanner(buffer)
	scanner.Split(ScanPayloadSplitFunc)

	for scanner.Scan() {
		if !reflect.DeepEqual(scanner.Bytes(), payload) {
			t.Fatalf("%v != %v", payload, scanner.Bytes())
		}
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}

func TestEncodePayload(t *testing.T) {
	data := []byte{1, 2, 3}
	payload := EncodePayload(data)

	payloadLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(payloadLen, uint64(len(data)))

	expected := append(payloadLen[:numBytes], data...)

	if !reflect.DeepEqual(payload, expected) {
		t.Fatalf("%v != %v", payload, expected)
	}
}

func TestDecodePayload(t *testing.T) {
	data := []byte{1, 2, 3}
	payload := EncodePayload(data)
	decoded := DecodePayload(payload)

	if !reflect.DeepEqual(data, decoded) {
		t.Fatalf("%v != %v", decoded, data)
	}

}

func TestScannerDecodeEOT(t *testing.T) {
	data := []byte{1, 2, 3}
	buffer := &bytes.Buffer{}

	buffer.Write(EncodePayload(data))
	WriteEOT(buffer)

	scanner := bufio.NewScanner(buffer)
	scanner.Split(ScanPayloadSplitFunc)

	numScans := 0
	for scanner.Scan() {
		numScans++
		if !reflect.DeepEqual(scanner.Bytes(), data) {
			t.Fatalf("%v != %v", data, scanner.Bytes())
		}
	}

	if numScans > 1 {
		log.Fatal("expected 1 scan")
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
}
