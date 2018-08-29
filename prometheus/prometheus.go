package prometheus

import (
	"github.com/alileza/queue"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "queue"

var (
	queueConsumerUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumer_up",
			Help:      "Queue consumer status up",
		},
		[]string{"exchange", "key", "queue"},
	)

	queueConsumerProcessDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "consumer_process_durations_seconds",
			Help:      "Queue consumer process time",
		},
		[]string{"exchange", "key", "queue", "status"},
	)

	queueConsumerProcessCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "consumer_process_count",
			Help:      "Queue consumer process count",
		},
		[]string{"exchange", "key", "queue", "status"},
	)
)

func init() {
	prometheus.MustRegister(queueConsumerUp)
	prometheus.MustRegister(queueConsumerProcessDurations)
	prometheus.MustRegister(queueConsumerProcessCount)
}

type Prometheus struct{}

func New() *Prometheus {
	return &Prometheus{}
}

func (p *Prometheus) Handle(e *queue.Event) {
	switch e.Kind {
	case queue.EventConsumerUp:
		var status float64
		if e.Err == nil {
			status = 1
		}
		queueConsumerUp.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, e.Consumer.QueueName).Set(status)

	case queue.EventConsumerProcessedMessage:
		queueConsumerProcessDurations.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, e.Status).Observe(e.ProcessDuration.Seconds())
		queueConsumerProcessCount.WithLabelValues(e.Consumer.Exchange, e.Consumer.RoutingKey, e.Status).Inc()
	}
}
