package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const smartFallbackCount = 3

type smartDialResult struct {
	smartAttemptResult
	tag    string
	trace  *adapter.OutboundTrace
	key    smartCacheKey
	serial uint64
}

type smartDialFunc func(context.Context, adapter.Outbound) (io.Closer, error)

func (s *SmartURLTest) attempt(ctx context.Context, candidate adapter.Outbound, dial smartDialFunc) smartDialResult {
	attemptCtx, trace := adapter.ForkOutboundContext(ctx)
	trace.Append(s)
	trace.Append(candidate)
	r := smartDialAttempt(attemptCtx, s.ctx, time.Duration(s.options.DialTimeout), s.dialSlots, func(ctx context.Context) (io.Closer, error) { return dial(ctx, candidate) })
	if r.err != nil {
		r.err = fmt.Errorf("%s: %w", candidate.Tag(), r.err)
	}
	return smartDialResult{smartAttemptResult: r, tag: candidate.Tag(), trace: trace}
}

func (s *SmartURLTest) connect(ctx context.Context, network string, destination M.Socksaddr, dial smartDialFunc) smartDialResult {
	if ctx.Err() != nil {
		return smartDialResult{smartAttemptResult: smartAttemptResult{err: ctx.Err()}}
	}
	if s.ctx.Err() != nil {
		return smartDialResult{smartAttemptResult: smartAttemptResult{err: net.ErrClosed}}
	}
	snapshot, candidates, best := s.ranked(network)
	if len(candidates) == 0 {
		return smartDialResult{smartAttemptResult: smartAttemptResult{err: fmt.Errorf("smart-urltest: no %s candidates", network)}}
	}
	metadata := adapter.InboundContext{Destination: destination}
	if original := adapter.ContextFrom(ctx); original != nil {
		metadata = *original
		// A smart group used as a detour is choosing a route to the proxy
		// server, not to the original application destination.
		if original.Destination != destination {
			metadata.Domain = ""
			metadata.Destination = destination
		}
	}
	key := smartCacheKey{smartTarget(metadata), network}
	cached, generation, serial := s.cache.get(key, time.Now())
	if adapter.IsURLTest(ctx) {
		cached = ""
	}
	initial := candidates[0]
	for _, tag := range []string{best, cached} {
		for _, candidate := range candidates {
			if candidate.Tag() == tag {
				initial = candidate
				break
			}
		}
	}
	finish := func(r smartDialResult) smartDialResult {
		if ctx.Err() != nil || s.ctx.Err() != nil {
			r.close()
			return smartDialResult{smartAttemptResult: smartAttemptResult{err: context.Canceled}}
		}
		r.key = key
		// A refreshed member can retain its tag while changing credentials or
		// detours. Do not install bindings from an obsolete membership view.
		if !adapter.IsURLTest(ctx) {
			s.members.Updates().Commit(snapshot.Revision, func() { r.serial = s.cache.put(key, r.tag, generation, time.Now()) })
		}
		r.trace.BindCache(func() { s.cache.clearTarget(key.target) })
		if parent := adapter.ContextFrom(ctx); parent != nil {
			parent.OutboundTrace.Commit(r.trace)
		}
		return r
	}
	result := s.attempt(ctx, initial, dial)
	if result.err == nil {
		return finish(result)
	}
	if !adapter.IsURLTest(ctx) {
		s.cache.forget(key, initial.Tag(), serial)
	}
	s.recordFailure(ctx, result.err)
	if ctx.Err() != nil || s.ctx.Err() != nil {
		return result
	}
	var alternatives []adapter.Outbound
	for _, candidate := range candidates {
		if candidate.Tag() != initial.Tag() {
			alternatives = append(alternatives, candidate)
			if len(alternatives) == smartFallbackCount {
				break
			}
		}
	}
	if len(alternatives) == 0 {
		return result
	}
	type indexedResult struct {
		index  int
		result smartDialResult
	}
	results := make(chan indexedResult)
	done := make(chan struct{})
	defer close(done)
	cancels := make([]context.CancelFunc, len(alternatives))
	winner := -1
	defer func() {
		for i, cancel := range cancels {
			if i != winner {
				cancel()
			}
		}
	}()
	for i, candidate := range alternatives {
		attemptCtx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func() {
			r := s.attempt(attemptCtx, candidate, dial)
			release := r.release
			r.release = func() {
				if release != nil {
					release()
				}
				cancel()
			}
			select {
			case results <- indexedResult{i, r}:
			case <-done:
				r.close()
			}
		}()
	}
	failures := []error{result.err}
	for range alternatives {
		select {
		case <-ctx.Done():
			return smartDialResult{smartAttemptResult: smartAttemptResult{err: ctx.Err()}}
		case <-s.ctx.Done():
			return smartDialResult{smartAttemptResult: smartAttemptResult{err: net.ErrClosed}}
		case next := <-results:
			if next.result.err == nil {
				winner = next.index
				return finish(next.result)
			}
			next.result.close()
			s.recordFailure(ctx, next.result.err)
			failures = append(failures, next.result.err)
		}
	}
	return smartDialResult{smartAttemptResult: smartAttemptResult{err: errors.Join(failures...)}}
}

func (s *SmartURLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	name := N.NetworkName(network)
	if name != N.NetworkTCP && name != N.NetworkUDP {
		return nil, fmt.Errorf("smart-urltest: unsupported network: %s", network)
	}
	r := s.connect(ctx, name, destination, func(ctx context.Context, outbound adapter.Outbound) (io.Closer, error) {
		conn, err := outbound.DialContext(ctx, network, destination)
		if conn == nil {
			return nil, err
		}
		return conn, err
	})
	if r.err != nil {
		return nil, r.err
	}
	connection := newSmartConn(ctx, s, r, name == N.NetworkUDP)
	wrapped := s.interruptGroup.NewConn(connection, interrupt.IsExternalConnectionFromContext(ctx))
	if s.ctx.Err() != nil || ctx.Err() != nil {
		wrapped.Close()
		return nil, context.Canceled
	}
	return wrapped, nil
}

func (s *SmartURLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	r := s.connect(ctx, N.NetworkUDP, destination, func(ctx context.Context, outbound adapter.Outbound) (io.Closer, error) {
		conn, err := outbound.ListenPacket(ctx, destination)
		if conn == nil {
			return nil, err
		}
		return conn, err
	})
	if r.err != nil {
		return nil, r.err
	}
	connection := newSmartPacketConn(ctx, s, r)
	wrapped := s.interruptGroup.NewPacketConn(connection, interrupt.IsExternalConnectionFromContext(ctx))
	if s.ctx.Err() != nil || ctx.Err() != nil {
		wrapped.Close()
		return nil, context.Canceled
	}
	return wrapped, nil
}
