package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type smartTestManager struct {
	adapter.OutboundManager
	mu    sync.RWMutex
	nodes map[string]adapter.Outbound
}

func (m *smartTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[tag]
	return node, ok
}
func (m *smartTestManager) set(node adapter.Outbound) {
	m.mu.Lock()
	m.nodes[node.Tag()] = node
	m.mu.Unlock()
}

type smartTestOutbound struct {
	outbound.Adapter
	dial    func(context.Context, string, M.Socksaddr) (net.Conn, error)
	udpDial func(context.Context, string, M.Socksaddr) (net.Conn, error)
	calls   atomic.Int32
}

func smartNode(tag string, dial func(context.Context, string, M.Socksaddr) (net.Conn, error)) *smartTestOutbound {
	return &smartTestOutbound{Adapter: outbound.NewAdapter("test", tag, []string{"tcp", "udp"}, nil), dial: dial}
}
func (n *smartTestOutbound) DialContext(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
	n.calls.Add(1)
	if network == "udp" {
		if n.udpDial != nil {
			return n.udpDial(ctx, network, dst)
		}
		return nil, errors.New("UDP unavailable in test fixture")
	}
	return n.dial(ctx, network, dst)
}
func (n *smartTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}

type smartTrackedConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *smartTrackedConn) Close() error { c.closes.Add(1); return c.Conn.Close() }

func smartFixture(t *testing.T, options option.SmartURLTestOutboundOptions, nodes ...adapter.Outbound) (*SmartURLTest, *smartTestManager) {
	t.Helper()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	manager := &smartTestManager{nodes: make(map[string]adapter.Outbound)}
	for _, node := range nodes {
		manager.set(node)
		if len(options.Import) == 0 {
			options.Outbounds = append(options.Outbounds, node.Tag())
		}
	}
	service.MustRegister[adapter.OutboundManager](ctx, manager)
	service.MustRegisterPtr(ctx, urltest.NewHistoryStorage())
	service.MustRegisterPtr(ctx, adapter.NewGroupUpdates())
	service.MustRegisterPtr(ctx, new(adapter.OutboundCacheRegistry))
	raw, err := NewSmartURLTest(ctx, nil, logger.NOP(), "smart", options)
	require.NoError(t, err)
	smart := raw.(*SmartURLTest)
	t.Cleanup(func() { smart.Close() })
	smart.refresh()
	return smart, manager
}

func TestSmartURLTestProbesEveryMemberWithLimitedConcurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	for _, concurrency := range []int{1, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var nodes []adapter.Outbound
			for index := range 48 {
				nodes = append(nodes, smartNode(fmt.Sprint("node-", index), func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, dst.String())
				}))
			}
			s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{URL: server.URL, TestConcurrency: concurrency}, nodes...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := s.URLTest(ctx)
			require.NoError(t, err)
			require.Len(t, result, len(nodes))
			for _, node := range nodes {
				require.EqualValues(t, 2, node.(*smartTestOutbound).calls.Load(), node.Tag())
			}
		})
	}
}

func TestSmartFallbackWinnerCacheChainAndLateClose(t *testing.T) {
	failure := errors.New("initial failed")
	initial := smartNode("initial", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, failure })
	started := make(chan struct{}, 3)
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	t.Cleanup(func() { unblockOnce.Do(func() { close(unblock) }) })
	var clients []*smartTrackedConn
	var late []*smartTestOutbound
	for _, tag := range []string{"late1", "winner", "late2"} {
		client, peer := net.Pipe()
		t.Cleanup(func() { peer.Close() })
		connection := &smartTrackedConn{Conn: client}
		clients = append(clients, connection)
		late = append(late, smartNode(tag, func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			started <- struct{}{}
			if tag != "winner" {
				<-unblock
			}
			return connection, nil
		}))
	}
	extra := smartNode("extra", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("fourth fallback must not dial")
	})
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, initial, late[0], late[1], late[2], extra)
	s.mu.Lock()
	for tag, delay := range map[string]uint16{"initial": 1, "late1": 2, "winner": 3, "late2": 4, "extra": 5} {
		s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: s.options.URL, Time: time.Now(), Delay: delay})
	}
	s.best["tcp"] = "initial"
	s.mu.Unlock()
	trace := adapter.NewOutboundTrace([]adapter.Outbound{s})
	destination := M.ParseSocksaddr("example.com:443")
	ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{Destination: destination, OutboundTrace: trace})
	connection, err := s.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	defer connection.Close()
	// A winner can cancel an alternative before its dial starts. Started workers
	// that ignore cancellation must still have their late success closed.
	unblockOnce.Do(func() { close(unblock) })
	require.Eventually(t, func() bool { return len(s.dialSlots) == 0 }, time.Second, time.Millisecond)
	for i, node := range late {
		if node.Tag() != "winner" && node.calls.Load() > 0 {
			require.EqualValues(t, 1, clients[i].closes.Load())
		}
	}
	require.EqualValues(t, 0, extra.calls.Load())
	require.EqualValues(t, 0, clients[1].closes.Load())
	require.Equal(t, []string{"smart", "winner"}, []string{trace.Chain()[0].Tag(), trace.Chain()[1].Tag()})
	cached, _, _ := s.cache.get(smartCacheKey{"example.com", "tcp"}, time.Now())
	require.Equal(t, "winner", cached)
	trace.InvalidateCaches()
	cached, _, _ = s.cache.get(smartCacheKey{"example.com", "tcp"}, time.Now())
	require.Empty(t, cached)
}

func TestSmartRefreshAndAPIClearDuringDial(t *testing.T) {
	for _, mode := range []string{"refresh", "clear", "close-trace"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			client, peer := net.Pipe()
			defer peer.Close()
			node := smartNode("same-tag", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				close(entered)
				<-release
				return client, nil
			})
			s, manager := smartFixture(t, option.SmartURLTestOutboundOptions{}, node)
			trace := adapter.NewOutboundTrace([]adapter.Outbound{s})
			dst := M.ParseSocksaddr("target.test:443")
			ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{Destination: dst, OutboundTrace: trace})
			done := make(chan net.Conn, 1)
			go func() {
				conn, err := s.DialContext(ctx, "tcp", dst)
				if err != nil {
					done <- nil
				} else {
					done <- conn
				}
			}()
			<-entered
			switch mode {
			case "refresh":
				manager.set(smartNode("same-tag", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("replacement") }))
				s.members.Updates().Notify()
				s.refresh()
			case "clear":
				s.ClearCache()
			case "close-trace":
				trace.InvalidateCaches()
			}
			close(release)
			conn := <-done
			require.NotNil(t, conn)
			conn.Close()
			cached, _, _ := s.cache.get(smartCacheKey{"target.test", "tcp"}, time.Now())
			require.Empty(t, cached)
		})
	}
}

func TestSmartURLTestCoalescingCancellationAndStaleResults(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			w.WriteHeader(204)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	node := smartNode("node", func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		require.True(t, adapter.IsURLTest(ctx))
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	})
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{URL: server.URL}, node)
	firstCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { _, err := s.URLTest(firstCtx); done <- err }()
	<-entered
	go func() { _, err := s.URLTest(context.Background()); done <- err }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	s.members.Updates().Notify()
	close(release)
	require.NoError(t, <-done)
	require.EqualValues(t, 4, node.calls.Load())
	require.NotNil(t, s.history.LoadURLTestHistory("node", server.URL))
	result, err := s.URLTest(context.Background())
	require.NoError(t, err)
	require.Contains(t, result, "node")
	require.Equal(t, "node", s.Selected("tcp").Tag())
}

func TestSmartBusinessFailuresExcludeTestsAndCancellation(t *testing.T) {
	node := smartNode("bad", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("failed") })
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, node)
	for range 20 {
		s.recordFailure(adapter.ContextWithURLTest(context.Background()), errors.New("probe"))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	s.recordFailure(cancelled, errors.New("cancelled"))
	s.recordFailure(context.Background(), errSmartDialCapacity)
	require.Empty(t, s.health.failures)
	for range 5 {
		s.recordFailure(context.Background(), errors.New("business"))
	}
	require.Len(t, s.wake, 0)
	s.recordFailure(context.Background(), errors.New("sixth"))
	require.Len(t, s.wake, 1)
	require.Empty(t, s.health.failures)
	<-s.wake
	s.health.failures = []time.Time{time.Now().Add(-2 * time.Minute)}
	s.recordFailure(context.Background(), errors.New("new"))
	require.Len(t, s.health.failures, 1)
}

func TestSmartAutomaticCooldownAndProbeFailureIsolation(t *testing.T) {
	var timesMu sync.Mutex
	var times []time.Time
	node := smartNode("bad", func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		timesMu.Lock()
		times = append(times, time.Now())
		timesMu.Unlock()
		return nil, errors.New("probe failed")
	})
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{MinTestInterval: badoption.Duration(100 * time.Millisecond), Interval: badoption.Duration(time.Hour)}, node)
	go s.schedule()
	s.requestTest()
	require.Eventually(t, func() bool { return node.calls.Load() > 0 }, time.Second, time.Millisecond)
	for range 100 {
		s.requestTest()
	}
	require.Eventually(t, func() bool { return node.calls.Load() > 1 }, time.Second, time.Millisecond)
	s.stopProbes()
	timesMu.Lock()
	recorded := slices.Clone(times)
	timesMu.Unlock()
	require.LessOrEqual(t, len(recorded), 2)
	require.GreaterOrEqual(t, recorded[1].Sub(recorded[0]), 90*time.Millisecond)
	require.Empty(t, s.health.failures)
}

func TestSmartWinningContextSurvivesUntilClose(t *testing.T) {
	var captured context.Context
	client, peer := net.Pipe()
	defer peer.Close()
	node := smartNode("node", func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		captured = ctx
		return client, nil
	})
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{DialTimeout: badoption.Duration(20 * time.Millisecond)}, node)
	conn, err := s.DialContext(context.Background(), "tcp", M.ParseSocksaddr("target.test:443"))
	require.NoError(t, err)
	select {
	case <-captured.Done():
		t.Fatal("winner cancelled")
	case <-time.After(40 * time.Millisecond):
	}
	conn.Close()
	require.ErrorIs(t, captured.Err(), context.Canceled)
}

var _ io.Closer = (*smartTrackedConn)(nil)
