package queue

import (
	"time"

	"github.com/rafaeljesus/rabbus"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	conn            *rabbus.Rabbus
	options         *Options
	consumers       []Consumer
	listenErrChan   chan error
	listenEventChan chan *Event
	exts            []Extension
}

type Options struct {
	Datasource string
	Retry      func(error) bool
	Status     func(error) string
	Logger     *logrus.Logger
	Prometheus bool
}

var DefaultOptions = &Options{
	Retry: func(err error) bool {
		if err != nil {
			return true
		}
		return false
	},
	Status: func(err error) string {
		if err != nil {
			return "0"
		}
		return "1"
	},
	Prometheus: false,
	Logger:     logrus.StandardLogger(),
}

func New(r *rabbus.Rabbus, o *Options) *Queue {
	if o == nil {
		o = DefaultOptions
	}
	if o.Retry == nil {
		o.Retry = DefaultOptions.Retry
	}
	if o.Status == nil {
		o.Status = DefaultOptions.Status
	}
	if o.Logger == nil {
		o.Logger = DefaultOptions.Logger
	}

	q := &Queue{
		conn:            r,
		listenErrChan:   make(chan error),
		listenEventChan: make(chan *Event),
		options:         o,
	}
	go q.eventListener(q.listenEventChan)

	return q
}

type Handler func([]byte) error

type Consumer struct {
	Exchange   string
	RoutingKey string
	QueueName  string
	Handle     Handler
}

func (q *Queue) Register(c Consumer) {
	q.consumers = append(q.consumers, c)
}

func (q *Queue) RegisterExt(e Extension) {
	q.exts = append(q.exts, e)
}

func (q *Queue) Run() {
	for _, c := range q.consumers {
		msgChan, err := q.conn.Listen(rabbus.ListenConfig{
			Exchange: c.Exchange,
			Kind:     "topic",
			Key:      c.RoutingKey,
			Queue:    c.QueueName,
		})
		if err != nil {
			q.listenEventChan <- &Event{EventConsumerUp, c, nil, err, q.options.Status(err), 0}
			continue
		}

		go q.handle(msgChan, c)
	}
}

func (q *Queue) handle(ch chan rabbus.ConsumerMessage, c Consumer) {
	for {
		msg := <-ch

		t := time.Now()
		err := c.Handle(msg.Body)
		processTime := time.Since(t)

		if q.options.Retry(err) {
			msg.Ack(false)
		} else {
			msg.Ack(true)
		}

		q.listenEventChan <- &Event{
			EventConsumerProcessedMessage,
			c,
			msg.Body,
			err,
			q.options.Status(err),
			processTime,
		}

	}
}

func (q *Queue) Close() error {
	if q.conn == nil {
		return nil
	}
	return q.conn.Close()
}

// ListenError returns the receive-only channel that signals errors while starting the mq server.
func (q *Queue) ListenError() <-chan error {
	return q.listenErrChan
}
