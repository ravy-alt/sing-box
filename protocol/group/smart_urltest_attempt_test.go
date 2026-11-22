package group

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type smartTestCloser struct{ closed atomic.Int32 }

func (c *smartTestCloser) Close() error { c.closed.Add(1); return nil }

func TestSmartAttemptTransfersLifetime(t *testing.T) {
	var captured context.Context
	connection := new(smartTestCloser)
	r := smartDialAttempt(context.Background(), context.Background(), 30*time.Millisecond, make(chan struct{}, 1), func(ctx context.Context) (io.Closer, error) {
		captured = ctx
		return connection, nil
	})
	require.NoError(t, r.err)
	select {
	case <-captured.Done():
		t.Fatal("successful connection context was cancelled")
	case <-time.After(60 * time.Millisecond):
	}
	r.close()
	require.ErrorIs(t, captured.Err(), context.Canceled)
	require.EqualValues(t, 1, connection.closed.Load())
}

func TestSmartAttemptLateResultAndBoundedWorkers(t *testing.T) {
	slots := make(chan struct{}, 1)
	unblock := make(chan struct{})
	defer close(unblock)
	connection := new(smartTestCloser)
	r := smartDialAttempt(context.Background(), context.Background(), 20*time.Millisecond, slots, func(context.Context) (io.Closer, error) {
		<-unblock
		return connection, nil
	})
	require.ErrorIs(t, r.err, context.DeadlineExceeded)
	var called atomic.Bool
	r = smartDialAttempt(context.Background(), context.Background(), 20*time.Millisecond, slots, func(context.Context) (io.Closer, error) {
		called.Store(true)
		return nil, errors.New("unexpected dial")
	})
	require.ErrorIs(t, r.err, errSmartDialCapacity)
	require.False(t, called.Load(), "cancelled workers retain their capacity until they exit")
	unblock <- struct{}{}
	require.Eventually(t, func() bool { return connection.closed.Load() == 1 && len(slots) == 0 }, time.Second, time.Millisecond)
}

func TestSmartAttemptCancellationAndErrConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var called atomic.Bool
	r := smartDialAttempt(ctx, context.Background(), time.Second, make(chan struct{}, 1), func(context.Context) (io.Closer, error) {
		called.Store(true)
		return nil, nil
	})
	require.ErrorIs(t, r.err, context.Canceled)
	require.False(t, called.Load())
	connection := new(smartTestCloser)
	dialErr := errors.New("dial failed")
	r = smartDialAttempt(context.Background(), context.Background(), time.Second, make(chan struct{}, 1), func(context.Context) (io.Closer, error) {
		return connection, dialErr
	})
	require.ErrorIs(t, r.err, dialErr)
	require.EqualValues(t, 1, connection.closed.Load())
}
