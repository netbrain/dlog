package client

import (
	"io"
	"net"
	"sync"

	"github.com/netbrain/dlog/encoder"
	"github.com/netbrain/dlog/model"
)

type replayStream struct {
	conn         net.Conn
	responseChan chan model.LogEntry
	once         *sync.Once
}

func newReplayStream(conn net.Conn) *replayStream {
	r := &replayStream{
		conn:         conn,
		responseChan: make(chan model.LogEntry),
		once:         &sync.Once{},
	}
	return r
}

func (r *replayStream) next() (model.LogEntry, error) {
	r.once.Do(func() {
		r.sendReplayRequest()
		go r.readReplayResponse()
	})

	response, open := <-r.responseChan
	if !open {
		return nil, io.EOF
	}

	return response, nil
}

func (r *replayStream) sendReplayRequest() error {
	request := model.NewReplayRequest()
	_, err := r.conn.Write(encoder.EncodePayload(request))
	return err
}

func (r *replayStream) readReplayResponse() {
	writeLogEntryToChan(r.responseChan, r.conn)
}
