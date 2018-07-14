package longear

import (
	"github.com/streadway/amqp"
)

type DeliveryHandler func(*amqp.Delivery) error

type Consumer struct {
	queueName string
	tag       string
	handler   DeliveryHandler

	channel *amqp.Channel
	handled chan error
}

func NewConsumer(queueName string, tag string, handler DeliveryHandler) *Consumer {
	return &Consumer{
		queueName: queueName,
		tag:       tag,
		handler:   handler,
		channel:   nil,
		handled:   make(chan error, 1),
	}
}

func (c *Consumer) Listen(ch *amqp.Channel, errorCh chan<- error) error {
	if c.channel != nil {
		return ErrAlreadyListeining
	}
	c.channel = ch

	deliveries, err := ch.Consume(
		c.queueName,
		c.tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			c.handled <- ch.Close()
		}()

		for d := range deliveries {
			if err := c.handle(&d); err != nil {
				errorCh <- err
				break
			}
		}
	}()

	return nil
}

func (c *Consumer) handle(d *amqp.Delivery) (resErr error) {
	defer func() {
		if err := recover(); err != nil {
			resErr = d.Reject(false)
		}
	}()

	if handlerErr := c.handler(d); handlerErr != nil {
		return d.Nack(false, true)
	}
	return d.Ack(false)
}

func (c *Consumer) Close() error {
	if c.channel == nil {
		return nil
	}
	defer func() {
		c.channel = nil
	}()

	err := c.channel.Cancel(c.tag, false)
	if err != nil {
		return err
	}

	return <-c.handled
}
