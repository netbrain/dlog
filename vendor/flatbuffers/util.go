package flatbuffers

import (
	"math"
	"unsafe"
)

var (
	// See http://golang.org/ref/spec#Numeric_types

	// SizeUint8 is the byte size of a uint8.
	SizeUint8 = int(unsafe.Sizeof(uint8(0)))
	// SizeUint16 is the byte size of a uint16.
	SizeUint16 = int(unsafe.Sizeof(uint16(0)))
	// SizeUint32 is the byte size of a uint32.
	SizeUint32 = int(unsafe.Sizeof(uint32(0)))
	// SizeUint64 is the byte size of a uint64.
	SizeUint64 = int(unsafe.Sizeof(uint64(0)))

	// SizeInt8  is the byte size of a int8.
	SizeInt8 = int(unsafe.Sizeof(int8(0)))
	// SizeInt16 is the byte size of a int16.
	SizeInt16 = int(unsafe.Sizeof(int16(0)))
	// SizeInt32 is the byte size of a int32.
	SizeInt32 = int(unsafe.Sizeof(int32(0)))
	// SizeInt64 is the byte size of a int64.
	SizeInt64 = int(unsafe.Sizeof(int64(0)))

	// SizeFloat32 is the byte size of a float32.
	SizeFloat32 = int(unsafe.Sizeof(float32(0)))
	// SizeFloat64 is the byte size of a float64.
	SizeFloat64 = int(unsafe.Sizeof(float64(0)))

	// SizeByte is the byte size of a byte.
	// The `byte` type is aliased (by Go definition) to uint8.
	SizeByte = SizeUint8

	// SizeBool is the byte size of a bool.
	// The `bool` type is aliased (by flatbuffers convention) to uint8.
	SizeBool = SizeUint8

	// SizeSOffsetT is the byte size of an SOffsetT.
	SizeSOffsetT = int(unsafe.Sizeof(SOffsetT(0)))
	// SizeUOffsetT is the byte size of an UOffsetT.
	SizeUOffsetT = int(unsafe.Sizeof(UOffsetT(0)))
	// SizeVOffsetT is the byte size of an VOffsetT.
	SizeVOffsetT = int(unsafe.Sizeof(VOffsetT(0)))
)

type (
	// A SOffsetT stores a signed offset into arbitrary data.
	SOffsetT int32
	// A UOffsetT stores an unsigned offset into vector data.
	UOffsetT uint32
	// A VOffsetT stores an unsigned offset in a vtable.
	VOffsetT uint16
)

const (
	// VtableMetadataFields is the count of metadata fields in each vtable.
	VtableMetadataFields = 2
)

// GetByte decodes a little-endian byte from a byte slice.
func GetByte(buf []byte) byte {
	return byte(GetUint8(buf))
}

// GetBool decodes a little-endian bool from a byte slice.
func GetBool(buf []byte) bool {
	return buf[0] == 1
}

// GetUint8 decodes a little-endian uint8 from a byte slice.
func GetUint8(buf []byte) (n uint8) {
	n = uint8(buf[0])
	return
}

// GetUint16 decodes a little-endian uint16 from a byte slice.
func GetUint16(buf []byte) (n uint16) {
	n |= uint16(buf[0])
	n |= uint16(buf[1]) << 8
	return
}

// GetUint32 decodes a little-endian uint32 from a byte slice.
func GetUint32(buf []byte) (n uint32) {
	n |= uint32(buf[0])
	n |= uint32(buf[1]) << 8
	n |= uint32(buf[2]) << 16
	n |= uint32(buf[3]) << 24
	return
}

// GetUint64 decodes a little-endian uint64 from a byte slice.
func GetUint64(buf []byte) (n uint64) {
	n |= uint64(buf[0])
	n |= uint64(buf[1]) << 8
	n |= uint64(buf[2]) << 16
	n |= uint64(buf[3]) << 24
	n |= uint64(buf[4]) << 32
	n |= uint64(buf[5]) << 40
	n |= uint64(buf[6]) << 48
	n |= uint64(buf[7]) << 56
	return
}

// GetInt8 decodes a little-endian int8 from a byte slice.
func GetInt8(buf []byte) (n int8) {
	n = int8(buf[0])
	return
}

// GetInt16 decodes a little-endian int16 from a byte slice.
func GetInt16(buf []byte) (n int16) {
	n |= int16(buf[0])
	n |= int16(buf[1]) << 8
	return
}

// GetInt32 decodes a little-endian int32 from a byte slice.
func GetInt32(buf []byte) (n int32) {
	n |= int32(buf[0])
	n |= int32(buf[1]) << 8
	n |= int32(buf[2]) << 16
	n |= int32(buf[3]) << 24
	return
}

// GetInt64 decodes a little-endian int64 from a byte slice.
func GetInt64(buf []byte) (n int64) {
	n |= int64(buf[0])
	n |= int64(buf[1]) << 8
	n |= int64(buf[2]) << 16
	n |= int64(buf[3]) << 24
	n |= int64(buf[4]) << 32
	n |= int64(buf[5]) << 40
	n |= int64(buf[6]) << 48
	n |= int64(buf[7]) << 56
	return
}

// GetFloat32 decodes a little-endian float32 from a byte slice.
func GetFloat32(buf []byte) float32 {
	x := GetUint32(buf)
	return math.Float32frombits(x)
}

// GetFloat64 decodes a little-endian float64 from a byte slice.
func GetFloat64(buf []byte) float64 {
	x := GetUint64(buf)
	return math.Float64frombits(x)
}

// GetUOffsetT decodes a little-endian UOffsetT from a byte slice.
func GetUOffsetT(buf []byte) UOffsetT {
	return UOffsetT(GetInt32(buf))
}

// GetSOffsetT decodes a little-endian SOffsetT from a byte slice.
func GetSOffsetT(buf []byte) SOffsetT {
	return SOffsetT(GetInt32(buf))
}

// GetVOffsetT decodes a little-endian VOffsetT from a byte slice.
func GetVOffsetT(buf []byte) VOffsetT {
	return VOffsetT(GetUint16(buf))
}

// WriteByte encodes a little-endian uint8 into a byte slice.
func WriteByte(buf []byte, n byte) {
	WriteUint8(buf, uint8(n))
}

// WriteBool encodes a little-endian bool into a byte slice.
func WriteBool(buf []byte, b bool) {
	buf[0] = 0
	if b {
		buf[0] = 1
	}
}

// WriteUint8 encodes a little-endian uint8 into a byte slice.
func WriteUint8(buf []byte, n uint8) {
	buf[0] = byte(n)
}

// WriteUint16 encodes a little-endian uint16 into a byte slice.
func WriteUint16(buf []byte, n uint16) {
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
}

// WriteUint32 encodes a little-endian uint32 into a byte slice.
func WriteUint32(buf []byte, n uint32) {
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 24)
}

// WriteUint64 encodes a little-endian uint64 into a byte slice.
func WriteUint64(buf []byte, n uint64) {
	for i := uint(0); i < uint(SizeUint64); i++ {
		buf[i] = byte(n >> (i * 8))
	}
}

// WriteInt8 encodes a little-endian int8 into a byte slice.
func WriteInt8(buf []byte, n int8) {
	buf[0] = byte(n)
}

// WriteInt16 encodes a little-endian int16 into a byte slice.
func WriteInt16(buf []byte, n int16) {
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
}

// WriteInt32 encodes a little-endian int32 into a byte slice.
func WriteInt32(buf []byte, n int32) {
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 24)
}

// WriteInt64 encodes a little-endian int64 into a byte slice.
func WriteInt64(buf []byte, n int64) {
	for i := uint(0); i < uint(SizeInt64); i++ {
		buf[i] = byte(n >> (i * 8))
	}
}

// WriteFloat32 encodes a little-endian float32 into a byte slice.
func WriteFloat32(buf []byte, n float32) {
	WriteUint32(buf, math.Float32bits(n))
}

// WriteFloat64 encodes a little-endian float64 into a byte slice.
func WriteFloat64(buf []byte, n float64) {
	WriteUint64(buf, math.Float64bits(n))
}

// WriteVOffsetT encodes a little-endian VOffsetT into a byte slice.
func WriteVOffsetT(buf []byte, n VOffsetT) {
	WriteUint16(buf, uint16(n))
}

// WriteSOffsetT encodes a little-endian SOffsetT into a byte slice.
func WriteSOffsetT(buf []byte, n SOffsetT) {
	WriteInt32(buf, int32(n))
}

// WriteUOffsetT encodes a little-endian UOffsetT into a byte slice.
func WriteUOffsetT(buf []byte, n UOffsetT) {
	WriteUint32(buf, uint32(n))
}
