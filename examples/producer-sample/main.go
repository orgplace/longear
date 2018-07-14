package main

import (
	"bufio"
	"context"
	"os"
	"time"

	"github.com/streadway/amqp"

	"github.com/orgplace/longear"
	"github.com/sirupsen/logrus"
)

type loggingWrapper struct {
	backoff longear.Backoff
}

func (w loggingWrapper) Duration(err error) time.Duration {
	logrus.WithError(err).Warn()
	return w.backoff.Duration(err)
}

func (w loggingWrapper) Reset() {
	logrus.Info("Connected")
	w.backoff.Reset()
}

func main() {
	producer := longear.NewProducer()

	connection := longear.NewConnection(
		"amqp://guest:guest@localhost:5672/",
		[]longear.Channel{
			producer,
		},
		&loggingWrapper{longear.FixedBackoff{Interval: 1 * time.Second}},
	)

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		err := producer.Publish("test-ex", "test-key", &amqp.Publishing{
			Body: s.Bytes(),
		})
		if err != nil {
			logrus.WithError(err).Error()
		}
	}
	if s.Err() != nil {
		logrus.WithError(s.Err()).Fatal("Scanner error")
	}

	logrus.Info("Shutting down the server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := connection.Shutdown(ctx); err != nil {
		logrus.WithError(err).Fatal("Error during shutdown")
	}

	logrus.Info("Server gracefully stopped")
}
