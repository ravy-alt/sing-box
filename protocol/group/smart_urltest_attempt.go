package group

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var errSmartDialCapacity = errors.New("smart-urltest: dial capacity exhausted")

type smartAttemptResult struct {
	connection io.Closer
	release    func()
	err        error
}

func (r smartAttemptResult) close() {
	if r.connection != nil {
		r.connection.Close()
	}
	if r.release != nil {
		r.release()
	}
}

// smartDialAttempt transfers the connection and its cancellation ownership to
// the caller only after an unbuffered handoff. A dialer returning after the
// timeout owns and closes its result. Slots remain occupied until the actual
// dial function exits, bounding even implementations that ignore cancellation.
func smartDialAttempt(parent, lifetime context.Context, timeout time.Duration, slots chan struct{}, dial func(context.Context) (io.Closer, error)) smartAttemptResult {
	ctx, cancel := context.WithCancelCause(parent)
	stopLifetime := context.AfterFunc(lifetime, func() { cancel(context.Canceled) })
	if lifetime.Err() != nil {
		cancel(context.Canceled)
	}
	var timerMu sync.Mutex
	dialing := true
	timer := time.AfterFunc(timeout, func() {
		timerMu.Lock()
		defer timerMu.Unlock()
		if dialing {
			cancel(context.DeadlineExceeded)
		}
	})
	finishDial := func() {
		timerMu.Lock()
		dialing = false
		timer.Stop()
		timerMu.Unlock()
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			finishDial()
			stopLifetime()
			cancel(context.Canceled)
		})
	}
	if err := context.Cause(ctx); err != nil {
		release()
		return smartAttemptResult{err: err}
	}
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		err := context.Cause(ctx)
		release()
		if parent.Err() == nil && lifetime.Err() == nil {
			err = errors.Join(errSmartDialCapacity, err)
		}
		return smartAttemptResult{err: err}
	}
	result := make(chan smartAttemptResult)
	go func() {
		defer func() { <-slots }()
		connection, err := dial(ctx)
		if err != nil && connection != nil {
			connection.Close()
			connection = nil
		}
		if err == nil && connection == nil {
			err = errors.New("smart-urltest: dialer returned a nil connection")
		}
		r := smartAttemptResult{connection: connection, release: release, err: err}
		select {
		case result <- r:
		case <-ctx.Done():
			r.close()
		}
	}()
	select {
	case r := <-result:
		// Stop the private dial timer without cancelling the successful
		// connection's context. Its new owner releases it on Close.
		finishDial()
		if err := context.Cause(ctx); err != nil {
			r.close()
			return smartAttemptResult{err: err}
		}
		if r.err != nil {
			r.close()
			return smartAttemptResult{err: r.err}
		}
		return r
	case <-ctx.Done():
		err := context.Cause(ctx)
		release()
		return smartAttemptResult{err: err}
	}
}
