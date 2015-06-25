package testdata

import (
	"log"
	"math/rand"

	. "github.com/netbrain/dlog/model"
)

type MetaDataTestData struct {
	metadata MetaData
}

func NewMetaDataTestData() *MetaDataTestData {
	return &MetaDataTestData{
		metadata: NewMetaData(NewUUID(), uint64(rand.Int()), NewUUID()),
	}
}

func (m *MetaDataTestData) WithClientID(clientId UUID) *MetaDataTestData {
	m.metadata = NewMetaData(
		clientId,
		m.metadata.ClientMessageNumber(),
		m.metadata.TransactionID(),
	)
	return m
}

func (m *MetaDataTestData) WithClientMessageNumber(clientMessageNumber uint64) *MetaDataTestData {
	m.metadata = NewMetaData(
		m.metadata.ClientID(),
		clientMessageNumber,
		m.metadata.TransactionID(),
	)
	return m
}

func (m *MetaDataTestData) WithTransactionID(transactionId UUID) *MetaDataTestData {
	m.metadata = NewMetaData(
		m.metadata.ClientID(),
		m.metadata.ClientMessageNumber(),
		transactionId,
	)
	return m
}

func (m *MetaDataTestData) Build() MetaData {
	return m.metadata
}

type LogEntryTestData struct {
	logEntry LogEntry
}

func NewLogEntryTestData() *LogEntryTestData {
	return &LogEntryTestData{
		logEntry: NewLogEntry(
			NewMetaDataTestData().Build(),
			[]byte{1, 2, 3},
		),
	}
}

func (l *LogEntryTestData) WithPayload(payload []byte) *LogEntryTestData {
	l.logEntry = NewLogEntry(l.logEntry.MetaData(), payload)
	return l
}

func (l *LogEntryTestData) WithMetaData(metadata MetaData) *LogEntryTestData {
	l.logEntry = NewLogEntry(metadata, l.logEntry.Payload())
	return l
}

func (l *LogEntryTestData) Build() LogEntry {
	return l.logEntry
}

type RequestTestData struct {
	request Request
}

func NewRequestTestData() *RequestTestData {
	return &RequestTestData{
		request: NewWriteRequest(NewLogEntryTestData().Build()),
	}
}

func (r *RequestTestData) WithType(t byte) *RequestTestData {
	switch t {
	case TypeWriteRequest:
		logEntry, err := r.request.LogEntry()
		if err != nil {
			log.Fatal(err)
		}
		r.request = NewWriteRequest(logEntry)
	case TypeReplayRequest:
		r.request = NewReplayRequest()
	default:
		log.Fatalf("Unexpected type: %b", t)
	}
	return r
}

func (r *RequestTestData) WithLogEntry(logEntry LogEntry) *RequestTestData {
	r.request = NewWriteRequest(logEntry)
	return r
}

func (r *RequestTestData) Build() Request {
	return r.request
}
