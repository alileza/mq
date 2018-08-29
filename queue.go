package queue

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Queue struct {
	options   *Options
	consumers []Consumer
}

type Options struct {
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

func New(o *Options) *Queue {
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

	if o.Prometheus {
		prometheusRegister()
	}

	return &Queue{
		options: o,
	}
}

type Handler func([]byte) error

type Consumer struct {
	Exchange   string
	RoutingKey string
	Handle     Handler
}

func (q *Queue) Register(c Consumer) {
	q.consumers = append(q.consumers, c)
}

func (q *Queue) Run() {
	for _, c := range q.consumers {
		ch := listen(c.Exchange, c.RoutingKey)

		go func(c Consumer, ch <-chan *Message) {
			log := q.options.Logger.
				WithField("exchange", c.Exchange).
				WithField("routing-key", c.RoutingKey)
			for {
				msg := <-ch

				t := time.Now()
				err := c.Handle(msg.Body)
				processDuration := time.Since(t)
				if q.options.Retry(err) {
					msg.Ack(false)
				}
				if err != nil {
					log.
						WithField("payload", msg.Body).
						WithField("process_duration", processDuration).
						WithError(err).
						Error("failed to process queue")
				}

				if q.options.Prometheus {
					queueConsumerProcessDurations.WithLabelValues(c.Exchange, c.RoutingKey, q.options.Status(err)).Observe(processDuration.Seconds())
					queueConsumerProcessCount.WithLabelValues(c.Exchange, c.RoutingKey, q.options.Status(err)).Inc()
				}

			}
		}(c, ch)
	}
}

type Message struct {
	Body []byte
}

func (m *Message) Ack(bool) {}

func listen(exchange, routingKey string) <-chan *Message {
	return make(chan *Message)
}
