package longear

import (
	"errors"
	"sync"

	"github.com/streadway/amqp"
)

type envelop struct {
	exchange   string
	key        string
	publishing *amqp.Publishing
	errCh      chan error
}

type Producer struct {
	publish    chan *envelop
	publishMux *sync.RWMutex
	handled    chan error
}

var (
	ErrAlreadyCloased = errors.New("Already closed")
	ErrAckFailed      = errors.New("Ack failed")
)

func NewProducer() *Producer {
	return &Producer{
		publishMux: &sync.RWMutex{},
	}
}

func (p *Producer) initPublish() error {
	p.publishMux.Lock()
	defer p.publishMux.Unlock()

	if p.publish != nil {
		return ErrAlreadyListeining
	}
	p.publish = make(chan *envelop)

	return nil
}

func (p *Producer) Listen(ch *amqp.Channel, _ chan<- error) error {
	if err := p.initPublish(); err != nil {
		return err
	}

	p.handled = make(chan error, 1)

	if err := ch.Confirm(false); err != nil {
		return err
	}

	confirm := ch.NotifyPublish(make(chan amqp.Confirmation))

	confirmWaiting := sync.Map{}
	go func() {
		defer func() {
			p.handled <- ch.Close() // confirm channel is closed in this close
		}()

		sent := uint64(0)
		for env := range p.publish {
			sent++
			confirmWaiting.Store(sent, env.errCh)

			err := ch.Publish(env.exchange, env.key, false, false, *env.publishing)
			if err != nil {
				env.errCh <- err
				continue
			}
		}
	}()
	go func() {
		for conf := range confirm {
			if v, ok := confirmWaiting.Load(conf.DeliveryTag); ok {
				errCh := v.(chan error)
				if conf.Ack {
					errCh <- nil
				} else {
					errCh <- ErrAckFailed
				}
				confirmWaiting.Delete(conf.DeliveryTag)
			}
		}
	}()
	return nil
}

func (p *Producer) Close() error {
	p.publishMux.Lock()
	defer p.publishMux.Unlock()

	if p.publish == nil {
		return nil
	}

	defer func() {
		close(p.handled)
	}()
	close(p.publish)
	p.publish = nil
	return <-p.handled
}

func (p *Producer) Publish(exchange, key string, publishing *amqp.Publishing) error {
	p.publishMux.RLock()
	defer p.publishMux.RUnlock()

	if p.publish == nil {
		return ErrAlreadyCloased
	}

	errCh := make(chan error, 1)
	defer close(errCh)

	p.publish <- &envelop{
		exchange:   exchange,
		key:        key,
		publishing: publishing,
		errCh:      errCh,
	}
	return <-errCh
}
