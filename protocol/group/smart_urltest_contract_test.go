package group

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func smartTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	server, err := listener.AcceptTCP()
	require.NoError(t, err)
	for _, c := range []*net.TCPConn{client, server} {
		require.NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
		t.Cleanup(func() { c.Close() })
	}
	return client, server
}

func TestSmartConnHalfClose(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain TCP baseline", true: "SmartURLTest wrapper"}[wrapped], func(t *testing.T) {
			client, source := smartTCPPair(t)
			upstream, server := smartTCPPair(t)
			var destination net.Conn = upstream
			if wrapped {
				destination = newSmartConn(context.Background(), nil, smartDialResult{smartAttemptResult: smartAttemptResult{connection: upstream}}, false)
			}
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				io.ReadAll(server)
				io.WriteString(server, "response after FIN")
				server.CloseWrite()
			}()
			copied := make(chan error, 1)
			go func() { copied <- bufio.CopyConn(context.Background(), source, destination) }()
			_, err := io.WriteString(client, "request")
			require.NoError(t, err)
			require.NoError(t, client.CloseWrite())
			reply, err := io.ReadAll(client)
			<-copied
			<-serverDone
			require.NoError(t, err)
			require.Equal(t, "response after FIN", string(reply))
		})
	}
}

type smartCopyTestConn struct {
	net.Conn
	input   *strings.Reader
	failure error
}

func (c *smartCopyTestConn) Read(p []byte) (int, error) { return c.input.Read(p) }
func (c *smartCopyTestConn) Write(p []byte) (int, error) {
	if c.failure != nil {
		return 0, c.failure
	}
	return len(p), nil
}
func (c *smartCopyTestConn) Close() error { return nil }

func TestSmartConnIOCopyTracksHealth(t *testing.T) {
	node := smartNode("node", nil)
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, node)
	raw := &smartCopyTestConn{input: strings.NewReader("reply")}
	c := newSmartConn(context.Background(), s, smartDialResult{smartAttemptResult: smartAttemptResult{connection: raw}}, true)
	defer c.Close()
	_, underlyingFrom := c.ExtendedConn.(io.ReaderFrom)
	_, underlyingTo := c.ExtendedConn.(io.WriterTo)
	_, outerFrom := any(c).(io.ReaderFrom)
	_, outerTo := any(c).(io.WriterTo)
	t.Logf("underlying ReaderFrom/WriterTo=%v/%v, smartConn=%v/%v", underlyingFrom, underlyingTo, outerFrom, outerTo)
	require.True(t, underlyingFrom)
	require.True(t, underlyingTo)
	require.False(t, outerFrom)
	require.False(t, outerTo)
	_, err := io.Copy(c, io.LimitReader(strings.NewReader("request"), 7))
	require.NoError(t, err)
	require.True(t, c.health.firstWrite.Load())
	require.Len(t, s.health.pending, 1, "io.Copy writes pass through sent()")
	_, err = io.Copy(io.Discard, c)
	require.NoError(t, err)
	require.Empty(t, s.health.pending, "io.Copy reads pass through received()")
	raw.failure = errors.New("copy write failed")
	c2 := newSmartConn(context.Background(), s, smartDialResult{smartAttemptResult: smartAttemptResult{connection: raw}}, false)
	defer c2.Close()
	_, err = io.Copy(c2, io.LimitReader(strings.NewReader("request"), 7))
	require.ErrorIs(t, err, raw.failure)
	require.True(t, c2.health.firstWrite.Load())
	require.Len(t, s.health.failures, 1, "io.Copy first-write failure is recorded")
}

func TestSmartPublishReturnsFalseWhenSkipped(t *testing.T) {
	for _, mode := range []string{"paused", "finalized", "obsolete round"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, smartNode("node", nil))
			round := &smartTestRound{snapshot: s.refresh(), measurements: map[string]urltest.Result{"node": {URL: s.options.URL, Started: time.Now(), Outbound: "node", Delay: 25}}}
			s.round = round
			switch mode {
			case "paused":
				s.PauseUpdates()
			case "finalized":
				round.finalized = true
			case "obsolete round":
				s.round = nil
			}
			committed := s.publishTest(round, map[string]uint16{"node": 25}, false)
			t.Logf("publishTest returned %v; history=%v", committed, s.history.LoadURLTestHistory("node", s.options.URL))
			require.Nil(t, s.history.LoadURLTestHistory("node", s.options.URL))
			require.False(t, committed, "skipped callback must not report a committed measurement")
		})
	}
}

func TestSmartPartialAvailableBeforeUDPCompletes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	udpEntered := make(chan struct{}, 1)
	node := smartNode("node", func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	})
	node.udpDial = func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		udpEntered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	other := smartNode("other", node.dial)
	other.udpDial = node.udpDial
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{URL: server.URL, TestConcurrency: 1}, node, other)
	round, _, err := s.startTest(false)
	require.NoError(t, err)
	defer func() {
		s.stopProbes()
		select {
		case <-round.done:
		case <-time.After(time.Second):
			t.Error("round cleanup timed out")
		}
	}()
	select {
	case <-udpEntered:
	case <-time.After(time.Second):
		t.Fatal("HTTP probe did not finish")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	partial, err := s.URLTest(ctx)
	t.Logf("HTTP completed before wait; partial=%v err=%v", partial, err)
	require.NoError(t, err, "completed HTTP measurement should be returned independently of pending UDP")
	require.Contains(t, partial, "node")
	require.Contains(t, partial, "other", "a blocked UDP probe must not starve the next node's HTTP probe")
	require.NotNil(t, s.history.LoadURLTestHistory("node", server.URL))
	require.NotNil(t, s.history.LoadURLTestHistory("other", server.URL))
}

type networkRankingTestGroup struct {
	*smartTestOutbound
	tcp, udp adapter.Outbound
}

func (g *networkRankingTestGroup) All() []string { return []string{g.tcp.Tag(), g.udp.Tag()} }
func (g *networkRankingTestGroup) Selected(network string) adapter.Outbound {
	if network == "udp" {
		return g.udp
	}
	return g.tcp
}
func (g *networkRankingTestGroup) AttachConnection(io.Closer) func() { return func() {} }

func TestURLTestRanksUDPByUDPLeaf(t *testing.T) {
	tcp, udp, other := smartNode("tcp-leaf", nil), smartNode("udp-leaf", nil), smartNode("other", nil)
	inner := &networkRankingTestGroup{smartTestOutbound: smartNode("inner", nil), tcp: tcp, udp: udp}
	history := urltest.NewHistoryStorage()
	link := "http://example.com/probe"
	for tag, delay := range map[string]uint16{"tcp-leaf": 10, "udp-leaf": 500, "other": 100} {
		history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: link, Time: time.Now(), Delay: delay})
	}
	g := &URLTestGroup{outbounds: []adapter.Outbound{inner, other}, history: history, link: link}
	choice, _ := g.Select("udp")
	t.Logf("TCP leaf=10ms, UDP leaf=500ms, alternative=100ms; selected UDP route=%s", choice.Tag())
	require.Equal(t, "other", choice.Tag(), "rank the HTTP latency of the leaf actually used for UDP")
}
