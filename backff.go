package longear

import "time"

type Backoff interface {
	Duration(reason error) time.Duration
	Reset()
}

type FixedBackoff struct {
	Interval time.Duration
}

func (bo FixedBackoff) Duration(_ error) time.Duration {
	return bo.Interval
}

func (bo FixedBackoff) Reset() {
	// Nothing todo
}
