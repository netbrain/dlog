package model

import (
	"errors"

	fb "github.com/google/flatbuffers/go"
)

const (
	//TypeWriteRequest is a flag which signals a write request
	TypeWriteRequest = 1<<iota - 1
	//TypeReplayRequest is a flag that signals a replay request
	TypeReplayRequest
	//TypeSubscribeRequest is a flag that signalst a subscription request
	TypeSubscribeRequest
)

/*
Request is a byte array which has data ordered in the following sequence:
	|---------------------------------------------------------------|
	| Type (1) | [LogEntry]                                         |
	|---------------------------------------------------------------|

a Request is the root type sent over the wire between client/server
*/
type Request []byte

var errWrongType = errors.New("request is not of correct type")

//NewReplayRequest creates a new replay request
func NewReplayRequest() Request {
	req := make(Request, 1)
	fb.WriteByte(req, TypeReplayRequest)
	return req
}

//NewWriteRequest creates a new write request
func NewWriteRequest(logEntry LogEntry) Request {
	req := make(Request, 1)
	fb.WriteByte(req, TypeWriteRequest)
	return append(req, logEntry...)
}

//NewSubscribeRequest creates a new subscription request
func NewSubscribeRequest() Request {
	req := make(Request, 1)
	fb.WriteByte(req, TypeSubscribeRequest)
	return req
}

//Type returns the type this reques is,
//either TypeWriteRequest or TypeReplayRequest
func (r Request) Type() byte {
	return fb.GetByte(r)
}

//LogEntry returns the LogEntry part of the Request byte array
//this will fail if the request is not a write request.
func (r Request) LogEntry() (LogEntry, error) {
	switch r.Type() {
	case TypeWriteRequest:
		return LogEntry(r[1:]), nil
	case TypeReplayRequest:
		return nil, errWrongType
	default:
		panic("Unexpected type!")
	}
}
