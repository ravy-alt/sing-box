package adapter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type traceTestOutbound struct {
	Outbound
	tag string
}

func (o *traceTestOutbound) Tag() string { return o.tag }

func TestOutboundTraceOnlyPublishesWinnerAndHonorsEarlyClear(t *testing.T) {
	a, b, c := &traceTestOutbound{tag: "outer"}, &traceTestOutbound{tag: "winner"}, &traceTestOutbound{tag: "loser"}
	trace := NewOutboundTrace([]Outbound{a})
	winner, loser := trace.Fork(), trace.Fork()
	var good, bad atomic.Int32
	winner.Append(b)
	winner.BindCache(func() { good.Add(1) })
	loser.Append(c)
	loser.BindCache(func() { bad.Add(1) })
	require.Equal(t, []Outbound{a}, trace.Chain())
	trace.InvalidateCaches()
	trace.Commit(winner)
	require.Equal(t, []Outbound{a, b}, trace.Chain())
	require.EqualValues(t, 1, good.Load())
	require.Zero(t, bad.Load())
	winner.Append(c)
	require.Equal(t, []Outbound{a, b, c}, trace.Chain(), "late handshake detours from the winner remain visible")
	winner.BindCache(func() { good.Add(1) })
	require.EqualValues(t, 2, good.Load(), "a late binding inherits an earlier administrative close")
}

func TestOutboundTraceConcurrentReadsAndInvalidation(t *testing.T) {
	trace := NewOutboundTrace(nil)
	ctx := WithContext(context.Background(), &InboundContext{OutboundTrace: trace, NamedDNSResponses: nil, SnifferNames: []string{"original"}})
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if i%3 == 0 {
					trace.InvalidateCaches()
				} else if i%3 == 1 {
					fork, child := ForkOutboundContext(ctx)
					ContextFrom(fork).SnifferNames[0] = "child"
					child.Append(&traceTestOutbound{tag: "node"})
					trace.Commit(child)
				} else {
					trace.Chain()
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, "original", ContextFrom(ctx).SnifferNames[0])
}

func TestDNSDetourDoesNotPolluteApplicationChain(t *testing.T) {
	node := &traceTestOutbound{tag: "application-proxy"}
	trace := NewOutboundTrace([]Outbound{node})
	ctx := WithContext(context.Background(), &InboundContext{OutboundTrace: trace})
	dnsCtx := ContextWithDNSTransportTag(ctx, "resolver")
	TraceOutbound(dnsCtx, &traceTestOutbound{tag: "resolver-detour"})
	require.Equal(t, []Outbound{node}, trace.Chain())
	require.Same(t, trace, ContextFrom(ctx).OutboundTrace)
}

func TestSharedSessionRetainsProbeMarkerWithoutRequestLifetime(t *testing.T) {
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	request, cancel := context.WithCancel(ContextWithURLTest(context.Background()))
	sessionCtx, value := NewTracedSession(lifetime, request)
	cancel()
	require.True(t, IsURLTest(sessionCtx))
	require.NoError(t, sessionCtx.Err(), "caller cancellation must not close a shared transport")
	trace := value.(*OutboundTrace)
	trace.Append(&traceTestOutbound{tag: "transport-relay"})
	connectionTrace := NewOutboundTrace([]Outbound{&traceTestOutbound{tag: "connection"}})
	ctx := WithContext(context.Background(), &InboundContext{OutboundTrace: connectionTrace})
	TraceSessionSelection(ctx, value)
	require.Len(t, connectionTrace.Chain(), 2)
	stop()
	require.ErrorIs(t, sessionCtx.Err(), context.Canceled)
}

func TestTransportTraceRemainsLiveAndReplacesFailedSession(t *testing.T) {
	trace := NewOutboundTrace([]Outbound{&traceTestOutbound{tag: "application"}})
	old, next := NewOutboundTrace([]Outbound{&traceTestOutbound{tag: "old"}}), NewOutboundTrace([]Outbound{&traceTestOutbound{tag: "current"}})
	var oldCleared, newCleared atomic.Int32
	old.BindCache(func() { oldCleared.Add(1) })
	next.BindCache(func() { newCleared.Add(1) })
	trace.SetTransport("http-transport", old)
	trace.Append(&traceTestOutbound{tag: "after-transport"})
	trace.SetTransport("http-transport", next)
	next.Append(&traceTestOutbound{tag: "late-detour"})
	chain := trace.Chain()
	var tags []string
	for _, outbound := range chain {
		tags = append(tags, outbound.Tag())
	}
	require.Equal(t, []string{"application", "current", "late-detour", "after-transport"}, tags)
	trace.InvalidateCaches()
	require.Zero(t, oldCleared.Load())
	require.EqualValues(t, 1, newCleared.Load())
	next.BindCache(func() { newCleared.Add(1) })
	require.EqualValues(t, 2, newCleared.Load())
}
