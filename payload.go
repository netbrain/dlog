package dlog

import (
	"encoding/binary"
	"io"
)

//EOT (End of transmission)
var EOT = []byte{0}

//WriteEOT writes the end of transmission signal to the io.writer
func WriteEOT(w io.Writer) {
	w.Write(EOT)
}

//EncodePayload encodes a payload with data length information
func EncodePayload(data []byte) []byte {
	dataLen := make([]byte, binary.MaxVarintLen32)
	numBytes := binary.PutUvarint(dataLen, uint64(len(data)))
	return append(dataLen[:numBytes], data...)
}

//DecodePayload decodes a payload encoded with data length information
func DecodePayload(data []byte) []byte {
	rawLen, lenSize := binary.Uvarint(data)
	payloadLen := int(rawLen)
	offset := payloadLen + lenSize
	return data[lenSize:offset]
}

//ScanPayloadSplitFunc is a function intendend for bufio.Scanner's Split function
func ScanPayloadSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	rawLen, lenSize := binary.Uvarint(data)
	payloadLen := int(rawLen)
	if payloadLen == 0 {
		return 1, nil, io.EOF
	}
	if (len(data) - lenSize) < payloadLen {
		return 0, nil, nil
	}
	offset := payloadLen + lenSize
	return offset, data[lenSize:offset], nil
}
