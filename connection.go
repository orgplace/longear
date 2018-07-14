package longear

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type Channel interface {
	Listen(ch *amqp.Channel, errorCh chan<- error) error
	Close() error
}

var (
	ErrAlreadyListeining = errors.New("This consumer is already listening a channel")
)

type CompositeError struct {
	Err           error
	ChannelErrors []error
}

func (e *CompositeError) Error() string {
	msgs := make([]string, len(e.ChannelErrors))
	for i, err := range e.ChannelErrors {
		if err != nil {
			msgs[i] = err.Error()
		} else {
			msgs[i] = "<nil>"
		}
	}

	return fmt.Sprintf("%v (%s)", e.Err, strings.Join(msgs, ", "))
}

func (e *CompositeError) empty() bool {
	if e.Err != nil {
		return false
	}

	for _, err := range e.ChannelErrors {
		if err != nil {
			return false
		}
	}

	return true
}

type Connection struct {
	amqpURL  string
	channels []Channel
	backoff  Backoff

	conn          *amqp.Connection
	stopping      int32
	stopWaitGroup *sync.WaitGroup
}

func NewConnection(amqpURL string, channels []Channel, backoff Backoff) *Connection {
	conn := &Connection{
		amqpURL:       amqpURL,
		channels:      channels,
		backoff:       backoff,
		stopWaitGroup: &sync.WaitGroup{},
	}

	go conn.reconnectLoop()

	return conn
}

func (c *Connection) reconnectLoop() {
RECONNECT:
	for atomic.LoadInt32(&c.stopping) == 0 {
		conn, err := amqp.Dial(c.amqpURL)
		if err != nil {
			<-time.NewTimer(c.backoff.Duration(err)).C
			continue RECONNECT
		}
		c.conn = conn

		errCh := make(chan error, len(c.channels))
		for _, ch := range c.channels {
			amqpCh, err := conn.Channel()
			if err != nil {
				if closeErr := conn.Close(); closeErr != nil {
					err = closeErr // Overwrite channels error
				}
				c.closeChannelsAndWait(err)
				continue RECONNECT
			}

			if err := ch.Listen(amqpCh, errCh); err != nil {
				if closeErr := conn.Close(); closeErr != nil {
					err = closeErr // Overwrite channels error
				}
				c.closeChannelsAndWait(err)
				continue RECONNECT
			}
		}

		c.backoff.Reset()

		select {
		case err := <-errCh:
			if err != nil {
				if closeErr := conn.Close(); closeErr != nil {
					err = closeErr // Overwrite channels error
				}
				c.closeChannelsAndWait(err)
			}
		case err := <-conn.NotifyClose(make(chan *amqp.Error)):
			if err != nil {
				c.closeChannelsAndWait(err)
			}
		}
	}
}

func (c *Connection) closeChannelsAndWait(err error) {
	<-time.NewTimer(c.backoff.Duration(&CompositeError{
		Err:           err,
		ChannelErrors: c.closeChannels(),
	})).C
}

func (c *Connection) closeChannels() (errors []error) {
	errors = make([]error, len(c.channels))
	for i, c := range c.channels {
		errors[i] = c.Close()
	}
	return
}

func (c *Connection) Close() error {
	if c.conn == nil {
		return nil
	}

	atomic.StoreInt32(&c.stopping, 1)

	errors := c.closeChannels()
	err := c.conn.Close()
	cerr := &CompositeError{
		Err:           err,
		ChannelErrors: errors,
	}

	if cerr.empty() {
		return nil
	}
	return cerr
}

func (c *Connection) Shutdown(ctx context.Context) error {
	errChan := make(chan error, 1)

	go func() {
		errChan <- c.Close()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}
