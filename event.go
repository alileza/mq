package queue

import "time"

type kind string

const (
	// When queue initialize
	EventQueueInit kind = "queue_init"

	// When consumer initiated
	EventConsumerUp kind = "consumer_up"

	// When consumed a message
	EventConsumerProcessedMessage kind = "consumer_processed_message"
)

type Extension interface {
	Handle(*Event)
}

type Event struct {
	Kind            kind
	Consumer        Consumer
	Payload         []byte
	Err             error
	Status          string
	ProcessDuration time.Duration
}

func (q *Queue) eventListener(eventChan chan *Event) {
	for {
		e := <-eventChan
		for _, ext := range q.exts {
			ext.Handle(e)
		}
	}
}
