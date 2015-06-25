package client

import (
	"io"

	"github.com/netbrain/dlog/model"
)

type replayStreams struct {
	streams []*replayStream
	entries map[int]model.LogEntry
}

func (r *ReadClient) newReplayStreams() *replayStreams {
	streams := make([]*replayStream, r.connectionPool.Len())
	for i, conn := range r.connectionPool.AllConnections() {
		streams[i] = newReplayStream(conn)
	}
	return &replayStreams{
		streams: streams,
	}
}

func (r *replayStreams) next() (model.LogEntry, error) {
	entryIndex := -1

	if r.entries == nil {
		r.entries = make(map[int]model.LogEntry)

		for i, stream := range r.streams {
			e, err := stream.next()
			if err != nil {
				return nil, err
			}

			r.entries[i] = e
		}
	}

	for i, e := range r.entries {
		if e == nil {
			continue
		} else if entryIndex == -1 {
			entryIndex = i
		} else if e.MetaData().ClientMessageNumber() < r.entries[entryIndex].MetaData().ClientMessageNumber() {
			entryIndex = i
		}
	}

	if entryIndex == -1 {
		return nil, io.EOF
	}
	stream := r.streams[entryIndex]
	nextEntry, err := stream.next()
	if err != nil && err != io.EOF {
		return nil, err
	}

	entry := r.entries[entryIndex]
	r.entries[entryIndex] = nextEntry

	return entry, nil

}
