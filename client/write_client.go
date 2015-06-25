package client

import (
	"log"
	"sync/atomic"

	"github.com/netbrain/dlog/encoder"
	"github.com/netbrain/dlog/model"
)

//WriteClient is the logging client which handles logwriting
type WriteClient struct {
	id             model.UUID
	wChan          chan []byte
	quitChan       chan bool
	msgCount       uint64
	connectionPool *RoundRobinConnectionPool
}

//NewWriteClient creates a new WriteClient instance
func NewWriteClient(servers []string) *WriteClient {
	client := &WriteClient{
		id:             model.NewUUID(),
		wChan:          make(chan []byte),
		quitChan:       make(chan bool),
		connectionPool: NewRoundRobinConnectionPool(servers),
	}

	go func(w *WriteClient) {
		for {
			select {
			case data := <-w.wChan:
				if err := w.write(data); err != nil {
					log.Fatal(err)
				}
			case <-w.quitChan:
				w.close()
				return
			}
		}
	}(client)

	return client
}

//Write adds data to the write queue
func (w *WriteClient) Write(data []byte) {
	w.wChan <- data
}

func (w *WriteClient) write(data []byte) error {
	msgCount := atomic.AddUint64(&w.msgCount, 1)

	md := model.NewMetaData(w.id, msgCount, model.NewUUID()) //TODO  transactionid should be supplied
	entry := model.NewLogEntry(md, data)
	request := model.NewWriteRequest(entry)

	conn := w.connectionPool.Connection()
	if _, err := conn.Write(encoder.EncodePayload(request)); err != nil {
		return err
	}

	return nil
}

//Close closes the client for further writing
func (w *WriteClient) Close() {
	w.quitChan <- true
}

func (w *WriteClient) close() {
	close(w.wChan)
	close(w.quitChan)
	w.connectionPool.Close()
}
