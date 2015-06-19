package model

import (
	"testing"
)

func TestCanCreateReplayRequest(t *testing.T) {
	req := NewRequestTestData().
		WithType(TypeReplayRequest).
		Build()
	if req.Type() != TypeReplayRequest {
		t.Fatal("Unexpected type")
	}

}

func TestCanCreateWriteRequest(t *testing.T) {
	req := NewRequestTestData().Build()
	if req.Type() != TypeWriteRequest {
		t.Fatal("Unexpected type")
	}
}
