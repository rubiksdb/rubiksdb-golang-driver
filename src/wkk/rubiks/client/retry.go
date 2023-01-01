package client

import (
	"time"
	"wkk/rubiks/api"
)

var FavoredRetry = &ExpBackRetry{
	Low:   1  * time.Millisecond,
	High:  32 * time.Millisecond,
}

type Retry interface {
	Fn(fn func() error) error
}

type SimpleRetry struct {
}

func (r *SimpleRetry) Fn(fn func() error) error {
	var err error

	for i := 0; i < 3; i += 1 {
		if err = fn(); err == nil || !err.(api.Outcome).Retryable() {
			return err
		}
	}
	return err
}

type ExpBackRetry struct {
	Low  time.Duration
	High time.Duration
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (r *ExpBackRetry) Fn(fn func() error) error {
	var err error
	backoff := r.Low

	for i := 0; i < 5; i += 1 {
		if err = fn(); err == nil || !err.(api.Outcome).Retryable() {
			return err
		}

		time.Sleep(backoff)
		backoff = min(backoff * 2, r.High)
	}

	return err
}