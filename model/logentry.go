package model

import (
	fb "github.com/google/flatbuffers/go"
)

/*
LogEntry is a byte array which has data ordered in the following sequence:
	|---------------------------------------------------------------|
	| MetaData | Payload (scalar)                                   |
	|---------------------------------------------------------------|
*/
type LogEntry []byte

//NewLogEntry creates a new LogEntry
func NewLogEntry(metaData MetaData, payload []byte) LogEntry {
	return LogEntry(append(metaData, payload...))
}

//Payload returns the payload part of the LogEntry byte array
func (l LogEntry) Payload() []byte {
	return l[len(l.MetaData()):]
}

//MetaData returns the MetaData part of the LogEntry byte array
func (l LogEntry) MetaData() MetaData {
	return MetaData(l[0 : fb.SizeUint64*3])
}
