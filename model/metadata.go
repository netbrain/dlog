package model

import (
	fb "github.com/netbrain/dlog/_vendor/flatbuffers"
)

/*
MetaData is a byte array which has data ordered in the following sequence
	|---------------------------------------------------------------|
	| ClientID (64) | ClientMessageNumber (64) | TransactionId (64) |
	|---------------------------------------------------------------|
*/
type MetaData []byte

//NewMetaData creates a new MetaData
func NewMetaData(clientID UUID, clientMessageNumber uint64, transactionID UUID) MetaData {
	md := make(MetaData, fb.SizeUint64*3)
	fb.WriteUint64(md[0:fb.SizeUint64], uint64(clientID))
	fb.WriteUint64(md[fb.SizeUint64:fb.SizeUint64*2], clientMessageNumber)
	fb.WriteUint64(md[fb.SizeUint64*2:fb.SizeUint64*3], uint64(transactionID))
	return md
}

//ClientID returns the client id part off the MetaData byte array
func (m MetaData) ClientID() UUID {
	return UUID(fb.GetUint64(m[0:fb.SizeUint64]))
}

//ClientMessageNumber returns the message number part
//off the MetaData byte array
func (m MetaData) ClientMessageNumber() uint64 {
	return fb.GetUint64(m[fb.SizeUint64 : fb.SizeUint64*2])
}

//TransactionID returns the transaction id part off
//the MetaData byte array
func (m MetaData) TransactionID() UUID {
	return UUID(fb.GetUint64(m[fb.SizeUint64*2 : fb.SizeUint64*3]))
}
