package queue

import "time"

type kind string

const (
	// When queue initialize
	eventQueueInit kind = "queue_init"

	// When consumer initiated
	eventConsumerUp kind = "consumer_up"

	// When consumed a message
	eventConsumerProcessedMessage kind = "consumer_processed_message"
)

type event struct {
	Kind            kind
	Consumer        Consumer
	Payload         []byte
	Err             error
	ProcessDuration time.Duration
}

func (q *Queue) background() {
	for {
		e := <-q.listenEventChan

		q.logger(e)
		q.prometheus(e)
	}
}

func (q *Queue) logger(e *event) {
	l := q.options.Logger.
		WithField("event", e.Kind).
		WithField("exchange", e.Consumer.Exchange).
		WithField("routing-key", e.Consumer.RoutingKey).
		WithField("queue-name", e.Consumer.QueueName).
		WithField("payload", string(e.Payload))

	if e.ProcessDuration != 0 {
		l.WithField("process_duration", e.ProcessDuration)
	}

	if e.Err != nil {
		l.WithError(e.Err).Error("ERR")
	}
}

func (q *Queue) prometheus(e *event) {
	if q.options.Prometheus == false {
		return
	}

	switch e.Kind {
	case eventQueueInit:
		prometheusRegister()
	case eventConsumerUp:
		var status float64
		if e.Err == nil {
			status = 1
		}
		queueConsumerUp.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, e.Consumer.QueueName).Set(status)
	case eventConsumerProcessedMessage:
		queueConsumerProcessDurations.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, q.options.Status(e.Err)).Observe(e.ProcessDuration.Seconds())
		queueConsumerProcessCount.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, q.options.Status(e.Err)).Inc()
	}
}
