// mq stands for Message Queue which a package to create message queue consumers
package mq

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/streadway/amqp"
)

var (
	// consumerUp a gauger consumer currently up
	consumerUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mq",
		Name:      "consumer_up",
		Help:      "A gauge of requests currently being served by the wrapped handler.",
	}, []string{"queue"})

	// messageInFlight a gauger of messages currently being served
	messageInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mq",
		Name:      "messages_in_flight",
		Help:      "A gauge of messages currently being process by the consumers.",
	})

	// messageCounter is the counter for the messages processed
	messageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mq",
			Name:      "messages_total",
			Help:      "A counter for messages to the wrapped handler",
		},
		[]string{"queue", "status"},
	)

	// processDuration a histogram of processes latencies
	processDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mq",
			Name:      "process_duration_seconds",
			Help:      "A histogram of request latencies",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"queue", "status"},
	)
)

func init() {
	prometheus.MustRegister(
		consumerUp,
		messageInFlight,
		messageCounter,
		processDuration,
	)
}

type MQ struct {
	conn          *amqp.Connection
	consumers     []Consumer
	listenErrChan chan error
	log           *log.Logger

	eventAfterProcess EventHandler
}

type EventHandler func(c Consumer, msg []byte, code int, err error)

type Options struct {
	// for each message that mq server consumed
	// AfterProcess callback will be called in case the user need feedback
	AfterProcess EventHandler

	Logger *log.Logger
}

func New(conn *amqp.Connection, opts Options) *MQ {
	if opts.AfterProcess == nil {
		opts.AfterProcess = func(c Consumer, msg []byte, code int, err error) {}
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, "[mq] ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	}
	mq := &MQ{
		conn:              conn,
		listenErrChan:     make(chan error),
		eventAfterProcess: opts.AfterProcess,
		log:               opts.Logger,
	}

	return mq
}

type Binding struct {
	Exchange   string
	RoutingKey string
}

type Handler func(msg []byte) (int, error)

type Consumer struct {
	Queue    string
	Bindings []Binding
	Handle   Handler
}

// Register a new consumer for mq server
func (mq *MQ) Register(c Consumer) {
	mq.consumers = append(mq.consumers, c)
}

// Run will start mq server with all registered consumer
func (mq *MQ) Run() {
	for _, consumer := range mq.consumers {
		go mq.consume(consumer)
	}
}

// ExecHandler allows you to execute handler for a given queueName
func (mq *MQ) ExecHandler(queueName string, body []byte) (int, error) {
	for _, c := range mq.consumers {
		if queueName == c.Queue {
			return c.Handle(body)
		}
	}
	return 0, fmt.Errorf("queue %s not found", queueName)
}

// consume responsible of managing amqp connection for each consumer
func (mq *MQ) consume(c Consumer) {
	errConnClose := mq.conn.NotifyClose(make(chan *amqp.Error))
	for i := 0; i < 30; i++ {
		consumerUp.WithLabelValues(c.Queue).Set(0)

		// in case of failed to create channel, having a second delay would help reduce cpu leak
		time.Sleep(time.Second * time.Duration(i))

		ch, err := mq.conn.Channel()
		if err != nil {
			mq.log.Printf("failed to open amqp.channel : %v", err)
			continue
		}
		defer ch.Close()

		errChanClose := ch.NotifyClose(make(chan *amqp.Error))

		go mq.process(ch, c)

		consumerUp.WithLabelValues(c.Queue).Set(1)

		select {
		case err := <-errConnClose:
			mq.listenErrChan <- fmt.Errorf("[%s] %s", c.Queue, err)
			return
		case err := <-errChanClose:
			mq.log.Printf("channel closed : %v", err)
			continue
		}
	}
	mq.listenErrChan <- fmt.Errorf("[%s] giving up on consuming message", c.Queue)
	return
}

func (mq *MQ) process(ch *amqp.Channel, c Consumer) error {
	q, err := ch.QueueDeclare(c.Queue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	for _, b := range c.Bindings {
		if err := ch.ExchangeDeclare(b.Exchange, "topic", true, false, false, false, nil); err != nil {
			return err
		}

		if err := ch.QueueBind(q.Name, b.RoutingKey, b.Exchange, false, nil); err != nil {
			return err
		}
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		msg, ok := <-msgs
		if !ok {
			break
		}
		handle := instrumentHandler(getCountry(msg.RoutingKey), q.Name, c.Handle)
		code, err := handle(msg.Body)
		mq.eventAfterProcess(c, msg.Body, code, err)

		if err := msg.Ack(false); err != nil {
			mq.log.Printf("failed to ack message : %v", err)
		}
	}
	return fmt.Errorf("[%s] channel closed", q.Name)
}

func (q *MQ) Close() error {
	return q.conn.Close()
}

func (q *MQ) ListenError() <-chan error {
	return q.listenErrChan
}

func instrumentHandler(name string, h Handler) Handler {
	return func(msg []byte) (int, error) {
		messageInFlight.Inc()
		t := time.Now()
		code, err := h(msg)
		processTime := time.Since(t)
		messageInFlight.Dec()

		messageCounter.WithLabelValues(name, fmt.Sprintf("%d", code)).Inc()
		processDuration.WithLabelValues(name, fmt.Sprintf("%d", code)).Observe(processTime.Seconds())

		return code, err
	}
}
