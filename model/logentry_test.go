package model

import (
	"reflect"
	"testing"
)

func TestCanCreateLogEntry(t *testing.T) {
	clientID := NewUUID()
	clientMessageNumber := uint64(1)
	transactionID := NewUUID()

	md := NewMetaData(clientID, clientMessageNumber, transactionID)

	payload := []byte{1, 2, 3}
	logEntry := NewLogEntry(md, payload)

	actual := []bool{
		reflect.DeepEqual(md, logEntry.MetaData()),
		reflect.DeepEqual(payload, logEntry.Payload()),
	}

	expected := []bool{true, true}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("Not equal")
	}
}
