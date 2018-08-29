package logger

import (
	"github.com/alileza/queue"
	"github.com/sirupsen/logrus"
)

type Logger struct {
	logger *logrus.Logger
}

func New() *Logger {
	return &Logger{
		logrus.StandardLogger(),
	}
}

func (l *Logger) Handle(e *queue.Event) {
	log := l.logger.
		WithField("event", e.Kind).
		WithField("exchange", e.Consumer.Exchange).
		WithField("routing-key", e.Consumer.RoutingKey).
		WithField("queue-name", e.Consumer.QueueName).
		WithField("payload", string(e.Payload))

	if e.ProcessDuration != 0 {
		log.WithField("process_duration", e.ProcessDuration)
	}

	if e.Err != nil {
		log.WithError(e.Err).Error("ERR")
	}
}
