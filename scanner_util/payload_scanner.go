package scanner_util

import "encoding/binary"

func ScanPayloadSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	rawLen, lenSize := binary.Uvarint(data)
	payloadLen := int(rawLen)
	if (len(data) - lenSize) < payloadLen {
		return 0, nil, nil
	}
	offset := payloadLen + lenSize

	return offset, data[lenSize:offset], nil
}
