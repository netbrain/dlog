package model

import (
	"reflect"
	"testing"
)

func TestCanCreateMetaData(t *testing.T) {
	clientID := NewUUID()
	clientMessageNumber := uint64(1)
	transactionID := NewUUID()

	md := NewMetaData(clientID, clientMessageNumber, transactionID)

	actual := []bool{
		clientID == md.ClientID(),
		clientMessageNumber == md.ClientMessageNumber(),
		transactionID == md.TransactionID(),
	}

	expected := []bool{true, true, true}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("Not equal")
	}
}
