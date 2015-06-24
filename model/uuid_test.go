package model

import (
	"reflect"
	"testing"
	"time"
)

func TestCreateUUID(t *testing.T) {
	ts := time.Now().Unix()

	uuid := NewUUID()

	ts2 := time.Now().Unix()
	actual := []bool{
		ts <= uuid.Time().Unix(),
		ts2 >= uuid.Time().Unix(),
	}
	expected := []bool{true, true}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("not equal: %v, ts: %v < time: %v > ts2: %v", actual, uuid.Time().Unix(), ts, ts2)
	}
}

func TestCreateTwoUUIDs(t *testing.T) {
	uuid := NewUUID()
	uuid2 := NewUUID()

	if reflect.DeepEqual(uuid, uuid2) {
		t.Fatal("They are equal!")
	}
}

func BenchmarkCreateUUID(b *testing.B) {
	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		NewUUID()
	}
}
