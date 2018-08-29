package queue

import "github.com/prometheus/client_golang/prometheus"

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

func prometheusRegister() {
	prometheus.MustRegister(queueConsumerUp)
	prometheus.MustRegister(queueConsumerProcessDurations)
	prometheus.MustRegister(queueConsumerProcessCount)
}
