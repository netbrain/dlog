package model

import (
	"testing"
)

func TestCanCreateReplayRequest(t *testing.T) {
	req := NewReplayRequest()
	if req.Type() != TypeReplayRequest {
		t.Fatal("Unexpected type")
	}

}

func TestCanCreateWriteRequest(t *testing.T) {
	req := NewWriteRequest(nil)
	if req.Type() != TypeWriteRequest {
		t.Fatal("Unexpected type")
	}
}

func TestCanCreateSubscribeRequest(t *testing.T) {
	req := NewSubscribeRequest()
	if req.Type() != TypeSubscribeRequest {
		t.Fatal("Unexpected type")
	}
}
