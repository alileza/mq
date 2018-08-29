package main

import (
	"github.com/alileza/queue"
	"github.com/alileza/queue/logger"
	"github.com/alileza/queue/prometheus"
	"github.com/rafaeljesus/rabbus"
)

func main() {
	r, _ := rabbus.New("")

	q := queue.New(r, &queue.Options{})

	q.RegisterExt(prometheus.New())
	q.RegisterExt(logger.New())

	q.Register(queue.Consumer{
		Exchange:   "test",
		RoutingKey: "test",
		Handle: func(b []byte) error {
			return nil
		},
	})

	go q.Run()
}
