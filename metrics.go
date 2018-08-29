package queue

import "github.com/prometheus/client_golang/prometheus"

var (
	queueConsumerProcessDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "queue_consumer_process_durations_seconds",
			Help: "Queue consumer process time",
		},
		[]string{"exchange", "kind", "key", "queue", "status"},
	)

	queueConsumerProcessCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_consumer_process_count",
			Help: "Queue consumer process count",
		},
		[]string{"exchange", "kind", "key", "queue", "status"},
	)
)

func prometheusRegister() {
	prometheus.MustRegister(queueConsumerProcessDurations)
	prometheus.MustRegister(queueConsumerProcessCount)

}
