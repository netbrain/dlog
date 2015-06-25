package client

import (
	"net"
	"sync"

	"github.com/netbrain/dlog/encoder"
	"github.com/netbrain/dlog/model"
)

func newReplayStream(conn net.Conn) *replayStream {
	r := &replayStream{
		conn:         conn,
		responseChan: make(chan model.LogEntry),
		once:         &sync.Once{},
	}
	return r
}

func (r *replayStream) sendReplayRequest() error {
	request := model.NewReplayRequest()
	_, err := r.conn.Write(encoder.EncodePayload(request))
	return err
}

func (r *replayStream) readReplayResponse() {
	writeLogEntryToChan(r.responseChan, r.conn)
}
