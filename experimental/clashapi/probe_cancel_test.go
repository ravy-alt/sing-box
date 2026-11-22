package clashapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestCancelledProbePreservesPreviousHistory(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "group"}[batch], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			node := &delayedProbe{&namedTestOutbound{name: "node"}, func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
				cancel()
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			s, _ := probeServer(t, []adapter.Outbound{node})
			link := "https://target.example/"
			s.urlTestHistory.StoreURLTestHistory("node", &adapter.URLTestHistory{Time: time.Now(), URL: link, Delay: 42})
			if batch {
				_, err := s.testGroup(ctx, s.outbound.Default().(adapter.OutboundGroup), link)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				r := httptest.NewRequest(http.MethodGet, "/node/delay?"+url.Values{"url": {link}, "timeout": {"1000"}}.Encode(), nil).WithContext(ctx)
				proxyRouter(s, nil).ServeHTTP(httptest.NewRecorder(), r)
			}
			h := s.urlTestHistory.LoadURLTestHistory("node", link)
			require.NotNil(t, h, "a cancelled client request is not a node failure")
			require.EqualValues(t, 42, h.Delay)
		})
	}
}

func TestSingleProbeDeadlineBoundsNonCooperativeDial(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	timer := time.AfterFunc(250*time.Millisecond, finish)
	defer timer.Stop()
	returned := make(chan struct{})
	late, peer := net.Pipe()
	defer peer.Close()
	node := &delayedProbe{&namedTestOutbound{name: "node"}, func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		<-release
		close(returned)
		return late, nil
	}}
	s, _ := probeServer(t, []adapter.Outbound{node})
	start := time.Now()
	w := httptest.NewRecorder()
	proxyRouter(s, nil).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/node/delay?url=https://example.com/&timeout=20", nil))
	require.Equal(t, http.StatusGatewayTimeout, w.Code)
	require.Less(t, time.Since(start), 150*time.Millisecond, "the HTTP deadline must not wait for a shared-session dial to finish")
	s.urlTestHistory.StoreURLTestHistory("node", &adapter.URLTestHistory{Time: time.Now(), URL: "https://example.com/", Delay: 42})
	finish()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("probe did not clean up")
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "a late connection must be closed")
	require.EqualValues(t, 42, s.urlTestHistory.LoadURLTestHistory("node", "https://example.com/").Delay, "a late worker must never overwrite newer history")
}

func TestProbeCapacityTimeoutPreservesUntestedNodes(t *testing.T) {
	require.Eventually(t, func() bool { return len(apiProbeSlots) == 0 }, time.Second, time.Millisecond)
	for range cap(apiProbeSlots) {
		apiProbeSlots <- struct{}{}
	}
	defer func() {
		for range cap(apiProbeSlots) {
			<-apiProbeSlots
		}
	}()
	node := &providerProbe{&namedTestOutbound{name: "node"}}
	s, _ := probeServer(t, []adapter.Outbound{node})
	link := "https://never-dialled.example/"
	s.urlTestHistory.StoreURLTestHistory("node", &adapter.URLTestHistory{Time: time.Now(), URL: link, Delay: 42})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.testGroup(ctx, s.outbound.Default().(adapter.OutboundGroup), link)
	require.Error(t, err)
	history := s.urlTestHistory.LoadURLTestHistory("node", link)
	require.NotNil(t, history)
	require.EqualValues(t, 42, history.Delay, "local saturation is not evidence of a node failure")
}
