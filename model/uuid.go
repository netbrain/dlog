package model

import (
	"crypto/rand"
	"time"

	fb "github.com/netbrain/dlog/vendor/flatbuffers"
)

//UUID represents a unique id where the first 4 bytes is the timestamp
//(with second precision) when this id was created and the last 4 bytes
//is randomly set
type UUID uint64

//NewUUID creates a new UUID
func NewUUID() UUID {
	n := uint64(time.Now().Unix())
	buf := make([]byte, fb.SizeUint64)
	fb.WriteUint64(buf, n)

	rand.Read(buf[fb.SizeUint32:fb.SizeUint64])
	return UUID(fb.GetUint64(buf))
}

//Time returns a time.Time with second precision
func (u UUID) Time() time.Time {
	buf := make([]byte, fb.SizeUint64)
	fb.WriteUint64(buf, uint64(u))
	for x := fb.SizeUint32; x < fb.SizeUint64; x++ {
		buf[x] = 0
	}
	return time.Unix(int64(fb.GetUint64(buf)), 0)
}
