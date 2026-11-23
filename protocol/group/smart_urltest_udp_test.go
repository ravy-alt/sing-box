package group

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestSmartHealthPriorityToleranceAndTies(t *testing.T) {
	for _, tc := range []struct {
		name          string
		a, b          smartProbeHealth
		current, want string
		tolerance     uint16
	}{
		{"UDP failure overrides tolerance", smartProbeHealth{tcp: 100}, smartProbeHealth{tcp: 180, udp: 50}, "a", "b", 100},
		{"healthy UDP outranks much faster TCP", smartProbeHealth{tcp: 1}, smartProbeHealth{tcp: 1000, udp: 50}, "a", "b", 100},
		{"TCP failure is unavailable", smartProbeHealth{udp: 1, tcpFailed: true}, smartProbeHealth{tcp: 100}, "a", "b", 100},
		{"TCP latency first", smartProbeHealth{tcp: 100, udp: 200}, smartProbeHealth{tcp: 110, udp: 1}, "", "a", 0},
		{"UDP latency breaks TCP tie", smartProbeHealth{tcp: 100, udp: 200}, smartProbeHealth{tcp: 100, udp: 1}, "a", "b", 100},
		{"healthy tolerance retained", smartProbeHealth{tcp: 150, udp: 100}, smartProbeHealth{tcp: 100, udp: 1}, "a", "a", 50},
		{"healthy outside tolerance", smartProbeHealth{tcp: 150, udp: 100}, smartProbeHealth{tcp: 100, udp: 1}, "a", "b", 49},
		{"UDP failure cannot use tolerance", smartProbeHealth{tcp: 150}, smartProbeHealth{tcp: 100}, "a", "b", 100},
		{"no successful TCP", smartProbeHealth{tcpFailed: true}, smartProbeHealth{tcpFailed: true}, "a", "", 100},
		{"startup", smartProbeHealth{}, smartProbeHealth{}, "b", "b", 100},
		{"tolerance cannot overflow", smartProbeHealth{tcp: 65535, udp: 1}, smartProbeHealth{tcp: 65534, udp: 1}, "a", "a", 65535},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, smartChooseHealthy([]string{"a", "b"}, map[string]smartProbeHealth{"a": tc.a, "b": tc.b}, tc.current, tc.tolerance))
		})
	}
}

func recordSmartProbe(s *SmartURLTest, tag string, tcp, udp uint16) {
	s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: s.options.URL, Time: time.Now(), Delay: tcp})
	r := urltest.UDPResult{Options: *s.options.UDPTest, Started: time.Now(), Outbound: tag, Delay: udp}
	if udp == 0 {
		r.Err = errors.New("no UDP response")
	}
	r.Store(s.history)
}

func TestSmartUnhealthyUDPDoesNotSurviveDestinationAffinity(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		t.Run(network, func(t *testing.T) {
			dial := func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				client, peer := net.Pipe()
				t.Cleanup(func() { peer.Close() })
				return client, nil
			}
			bad, good := smartNode("bad", dial), smartNode("good", dial)
			bad.udpDial = dial
			good.udpDial = dial
			tolerance := uint16(100)
			s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{Tolerance: &tolerance}, bad, good)
			recordSmartProbe(s, "bad", 100, 0)
			recordSmartProbe(s, "good", 180, 70)
			s.best[network] = "bad"
			key := smartCacheKey{"example.com", network}
			_, generation, _ := s.cache.get(key, time.Now())
			s.cache.put(key, "bad", generation, time.Now())
			conn, err := s.DialContext(context.Background(), network, M.ParseSocksaddr("example.com:443"))
			require.NoError(t, err)
			conn.Close()
			require.Zero(t, bad.calls.Load())
			require.EqualValues(t, 1, good.calls.Load())
			require.Equal(t, "good", s.Selected(network).Tag())
		})
	}
}

func TestSmartFailedTCPExcludedAndNewMembersRemainProbeable(t *testing.T) {
	node := smartNode("dead", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("dead") })
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, node)
	recordSmartProbe(s, "dead", 0, 20)
	require.Nil(t, s.Selected("tcp"))
	require.Equal(t, node, s.Selected("udp"), "a failed HTTP probe must not discard a successful UDP probe")
	_, err := s.DialContext(context.Background(), "tcp", M.ParseSocksaddr("example.com:443"))
	require.Error(t, err)
	require.Zero(t, node.calls.Load())
	// Periodic probes iterate membership, not the filtered live candidates.
	_, err = s.URLTest(context.Background())
	require.Error(t, err)
	require.EqualValues(t, 2, node.calls.Load(), "both TCP and UDP must be probed after a TCP failure")
}

func TestSmartFallbackKeepsHealthPriority(t *testing.T) {
	for _, healthyFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy wins", true: "TCP-only remains a fallback"}[healthyFails], func(t *testing.T) {
			initial := smartNode("initial", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("dial failed") })
			connected := func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				client, peer := net.Pipe()
				t.Cleanup(func() { peer.Close() })
				return client, nil
			}
			healthy := smartNode("healthy", func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
				time.Sleep(20 * time.Millisecond)
				if healthyFails {
					return nil, errors.New("destination unavailable")
				}
				return connected(ctx, network, dst)
			})
			tcpOnly := smartNode("tcp-only", connected)
			s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, initial, healthy, tcpOnly)
			recordSmartProbe(s, "initial", 10, 10)
			recordSmartProbe(s, "healthy", 20, 10)
			recordSmartProbe(s, "tcp-only", 1, 0)
			conn, err := s.DialContext(context.Background(), "tcp", M.ParseSocksaddr("example.com:443"))
			require.NoError(t, err)
			conn.Close()
			require.EqualValues(t, 1, healthy.calls.Load())
			if healthyFails {
				require.EqualValues(t, 1, tcpOnly.calls.Load())
			} else {
				require.Zero(t, tcpOnly.calls.Load())
			}
		})
	}
}

func TestSmartAutomaticRoundProbesNestedLeafAndOwnUDPProfile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer packet.Close()
	queries := make(chan string, 4)
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, peer, err := packet.ReadFrom(buffer)
			if err != nil {
				return
			}
			var query dns.Msg
			if query.Unpack(buffer[:n]) != nil || len(query.Question) != 1 {
				continue
			}
			queries <- query.Question[0].Name
			reply := new(dns.Msg).SetReply(&query)
			reply.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.IPv4(192, 0, 2, 1)}}
			data, _ := reply.Pack()
			packet.WriteTo(data, peer)
		}
	}()
	dial := func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	}
	leaf := smartNode("leaf", dial)
	leaf.udpDial = dial
	c := &historyTestGroup{smartNode("c", nil), leaf, server.URL + "/c"}
	b := &historyTestGroup{smartNode("b", nil), c, server.URL + "/b"}
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{URL: server.URL + "/a", UDPTest: &option.UDPTestOptions{Server: "127.0.0.1", ServerPort: uint16(packet.LocalAddr().(*net.UDPAddr).Port), Domain: "parent.example", Timeout: badoption.Duration(time.Second)}}, b, c, leaf)
	result, err := s.URLTest(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, "parent.example.", <-queries)
	require.Empty(t, queries, "aliases share one actual UDP exchange")
	require.EqualValues(t, 2, leaf.calls.Load(), "one TCP and one UDP dial")
	require.True(t, s.history.LoadUDPHistory("leaf", *s.options.UDPTest).Alive)
	for _, tag := range []string{"smart", "b", "c"} {
		require.Empty(t, s.history.UDPHistories(tag))
	}
	require.Equal(t, 0, s.probeHealth(b, "tcp").tier())
	otherProfile := *s.options.UDPTest
	otherProfile.Domain = "child.example."
	urltest.UDPResult{Options: otherProfile, Started: time.Now(), Outbound: "leaf", Err: errors.New("child failed")}.Store(s.history)
	require.Equal(t, 0, s.probeHealth(b, "tcp").tier(), "another group's profile cannot poison this group")
	other := smartNode("replacement", nil)
	s.history.StoreURLTestHistory(other.Tag(), &adapter.URLTestHistory{URL: s.TestURL(), Time: time.Now(), Delay: 1})
	c.selected = other
	require.Equal(t, 1, s.probeHealth(b, "tcp").tier(), "nested selection must not inherit the old leaf's UDP success")
}
