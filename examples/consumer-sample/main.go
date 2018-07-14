package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/orgplace/longear"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
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
	connection := longear.NewConnection(
		"amqp://guest:guest@localhost:5672/",
		[]longear.Channel{
			longear.NewConsumer(
				"test-queue",
				"consumer-sample",
				func(d *amqp.Delivery) error {
					logrus.Infof(
						"got %dB delivery: [%v] %q",
						len(d.Body), d.DeliveryTag, d.Body,
					)

					return nil
				},
			),
		},
		&loggingWrapper{longear.FixedBackoff{Interval: 1 * time.Second}},
	)

	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logrus.Info("Shutting down the server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := connection.Shutdown(ctx); err != nil {
		logrus.WithError(err).Fatal("Error during shutdown")
	}

	logrus.Info("Server gracefully stopped")
}
