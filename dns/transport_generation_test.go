package dns

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mDNS "github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/stretchr/testify/require"
)

type generationTestTransport struct {
	TransportAdapter
	closed  atomic.Bool
	resets  atomic.Int32
	keep    atomic.Bool
	stages  []adapter.StartStage
	entered chan struct{}
	resume  chan struct{}
	fail    bool
}

func (t *generationTestTransport) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	t.stages = append(t.stages, stage)
	if stage == adapter.StartStateInitialize {
		scope.Add(func() error { t.closed.Store(true); return nil })
	}
	if t.fail && stage == adapter.StartStateStart {
		return errors.New("start failed")
	}
	return nil
}
func (t *generationTestTransport) Reset()                           { t.resets.Add(1) }
func (t *generationTestTransport) SetKeepIdleConnections(keep bool) { t.keep.Store(keep) }
func (t *generationTestTransport) CloseIdleConnections()            {}
func (t *generationTestTransport) Exchange(ctx context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
	if t.entered != nil {
		close(t.entered)
	}
	if t.resume != nil {
		select {
		case <-t.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if t.closed.Load() {
		return nil, net.ErrClosed
	}
	return new(mDNS.Msg).SetReply(q), nil
}
func (t *generationTestTransport) ExchangeAsync(ctx context.Context, q *mDNS.Msg, callback func(*mDNS.Msg, error)) {
	go func() { response, err := t.Exchange(ctx, q); callback(response, err) }()
}

func TestDNSGenerationDrainsActiveAndEvictsIdle(t *testing.T) {
	for _, asynchronous := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[asynchronous], func(t *testing.T) {
			ctx := context.Background()
			logger := log.NewNOPFactory().Logger()
			updates := adapter.NewGroupUpdates()
			first := &generationTestTransport{entered: make(chan struct{}), resume: make(chan struct{})}
			second := &generationTestTransport{}
			var created atomic.Int32
			transport := newGenerationTransport(ctx, logger, updates, first, func() (adapter.DNSTransport, error) { created.Add(1); return second, nil })
			scope := adapter.NewScope(ctx, logger)
			t.Cleanup(func() { scope.Close() })
			for _, stage := range adapter.ListStartStages {
				require.NoError(t, transport.Start(stage, scope))
			}
			query := new(mDNS.Msg).SetQuestion("example.com.", mDNS.TypeA)
			result := make(chan error, 1)
			if asynchronous {
				transport.ExchangeAsync(ctx, query, func(_ *mDNS.Msg, err error) { result <- err })
			} else {
				go func() { _, err := transport.Exchange(ctx, query); result <- err }()
			}
			select {
			case <-first.entered:
			case <-time.After(time.Second):
				t.Fatal("exchange did not start")
			}
			transport.SetKeepIdleConnections(false)
			updates.Notify()
			_, err := transport.Exchange(ctx, query)
			require.NoError(t, err)
			require.EqualValues(t, 1, created.Load())
			require.Equal(t, adapter.ListStartStages, second.stages, "new pools replay all lifecycle stages")
			require.False(t, second.keep.Load())
			require.False(t, first.closed.Load(), "retirement must preserve the in-flight query")
			close(first.resume)
			require.NoError(t, <-result)
			require.True(t, first.closed.Load(), "last query releases the retired pool")
			transport.Reset()
			require.EqualValues(t, 1, second.resets.Load())
			updates.Notify()
			require.Eventually(t, func() bool { return second.closed.Load() }, time.Second, time.Millisecond, "idle pools must close without another DNS query")
			require.NoError(t, scope.Close())
			_, err = transport.Exchange(ctx, query)
			require.ErrorIs(t, err, net.ErrClosed)
		})
	}
}

func TestDNSGenerationCreationFailureAndCancellation(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNOPFactory().Logger()
	updates := adapter.NewGroupUpdates()
	first := &generationTestTransport{}
	failed := &generationTestTransport{fail: true}
	last := &generationTestTransport{entered: make(chan struct{}), resume: make(chan struct{})}
	attempt := 0
	transport := newGenerationTransport(ctx, logger, updates, first, func() (adapter.DNSTransport, error) {
		attempt++
		if attempt == 1 {
			return failed, nil
		}
		return last, nil
	})
	scope := adapter.NewScope(ctx, logger)
	t.Cleanup(func() { scope.Close() })
	for _, stage := range adapter.ListStartStages {
		require.NoError(t, transport.Start(stage, scope))
	}
	updates.Notify()
	query := new(mDNS.Msg).SetQuestion("example.com.", mDNS.TypeA)
	_, err := transport.Exchange(ctx, query)
	require.ErrorContains(t, err, "start failed")
	require.True(t, failed.closed.Load(), "failed pool construction must clean up")
	request, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	transport.ExchangeAsync(request, query, func(_ *mDNS.Msg, err error) { result <- err })
	select {
	case <-last.entered:
	case <-time.After(time.Second):
		t.Fatal("exchange did not start")
	}
	transport.CloseIdleConnections()
	require.False(t, last.closed.Load())
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.True(t, last.closed.Load(), "cancellation must release the retired pool")
	require.Equal(t, 2, attempt, "failed replacement can be retried")
}
