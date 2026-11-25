package clashapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/uot"
	"github.com/stretchr/testify/require"
)

type configuredUDPProbeGroup struct {
	*configuredProbeGroup
	options option.UDPTestOptions
}

func (g *configuredUDPProbeGroup) UDPTestOptions() option.UDPTestOptions { return g.options }

func udpRequest(s *Server, name string, query url.Values) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	proxyRouter(s, nil).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(name)+"/udp-delay?"+query.Encode(), nil))
	return w
}

type tcpOnlyUDPProbe struct{ *delayedProbe }

func (*tcpOnlyUDPProbe) Network() []string { return []string{"tcp"} }

func TestUDPAPISkipsDisabledLeafThroughNestedGroups(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		t.Run(map[bool]string{false: "no history", true: "existing history"}[seeded], func(t *testing.T) {
			var calls atomic.Int32
			leaf := &tcpOnlyUDPProbe{&delayedProbe{&namedTestOutbound{name: "leaf"}, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				calls.Add(1)
				return nil, errors.New("must not dial")
			}}}
			b := &configuredProbeGroup{&testProvider{name: "b", all: []string{"leaf"}}, leaf, ""}
			a := &configuredProbeGroup{&testProvider{name: "a", all: []string{"b"}}, b, ""}
			s, _ := probeServer(t, []adapter.Outbound{a, b, leaf})
			options, _ := urltest.NormalizeUDPTest(option.UDPTestOptions{})
			if seeded {
				urltest.UDPResult{Options: options, Started: time.Now().Add(-time.Minute), Outbound: "leaf", Delay: 12}.Store(s.urlTestHistory)
			}
			before := s.urlTestHistory.UDPHistories("leaf")
			for _, name := range []string{"leaf", "b", "a"} {
				w := udpRequest(s, name, nil)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.JSONEq(t, `{"skipped":true,"reason":"udp_not_supported","outbound":"leaf"}`, w.Body.String())
				require.Zero(t, calls.Load())
				require.Equal(t, before, s.urlTestHistory.UDPHistories("leaf"))
				require.Empty(t, s.urlTestHistory.UDPHistories("a"))
				require.Empty(t, s.urlTestHistory.UDPHistories("b"))
			}
		})
	}
}

func TestUDPAPISkipsTCPOnlyShadowsocksSubscription(t *testing.T) {
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { packet.Close() })
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"shadowsocks","tag":"香港","server":"127.0.0.1","server_port":%d,"method":"none","network":"tcp"},{"type":"selector","tag":"inner","outbounds":["香港"]}]}`, packet.LocalAddr().(*net.UDPAddr).Port)
	}))
	t.Cleanup(source.Close)
	_, s, _ := startNamedSubscriptionBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"source","url":%q,"http_client":{},"cache_path":%q}],"outbounds":[{"type":"selector","tag":"outer","import":[{"group":"source","include":"^inner$"}]}]}`, source.URL, filepath.Join(t.TempDir(), "source.json")))
	for _, name := range []string{"香港", "inner", "outer"} {
		w := udpRequest(s, name, url.Values{"timeout": {"20"}})
		require.Equal(t, http.StatusOK, w.Code, "%s: %s", name, w.Body.String())
		require.JSONEq(t, `{"skipped":true,"reason":"udp_not_supported","outbound":"香港"}`, w.Body.String())
	}
	for _, node := range s.outbound.Outbounds() {
		require.Empty(t, s.urlTestHistory.UDPHistories(node.Tag()), node.Tag())
	}
	require.NoError(t, packet.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	_, _, err = packet.ReadFrom(make([]byte, 2048))
	var timeout net.Error
	require.ErrorAs(t, err, &timeout)
	require.True(t, timeout.Timeout(), "a TCP-only Shadowsocks node must never receive a UDP probe packet")
}

func TestUDPAPISkipsDisabledShadowsocksDetour(t *testing.T) {
	for _, subscription := range []bool{false, true} {
		t.Run(fmt.Sprintf("subscription=%t", subscription), func(t *testing.T) {
			packet, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { packet.Close() })
			nodes := fmt.Sprintf(`[{"type":"shadowsocks","tag":"exit","server":"127.0.0.1","server_port":12345,"method":"none","detour":"hop"},{"type":"selector","tag":"hop","outbounds":["relay"]},{"type":"shadowsocks","tag":"relay","server":"127.0.0.1","server_port":%d,"method":"none","network":"tcp"}]`, packet.LocalAddr().(*net.UDPAddr).Port)
			config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":%s}`, nodes)
			if subscription {
				source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintf(w, `{"outbounds":%s}`, nodes) }))
				t.Cleanup(source.Close)
				// Colliding host tags must not mask the private disabled relay.
				config = fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"source","url":%q,"http_client":{},"cache_path":%q}],"outbounds":[{"type":"direct","tag":"relay"},{"type":"direct","tag":"hop"}]}`, source.URL, filepath.Join(t.TempDir(), "source.json"))
			}
			_, s, _ := startNamedSubscriptionBox(t, config)
			w := udpRequest(s, "exit", url.Values{"timeout": {"50"}})
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.JSONEq(t, `{"skipped":true,"reason":"udp_not_supported","outbound":"exit"}`, w.Body.String())
			for _, node := range s.outbound.Outbounds() {
				require.Empty(t, s.urlTestHistory.UDPHistories(node.Tag()), node.Tag())
			}
			require.NoError(t, packet.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
			n, _, err := packet.ReadFrom(make([]byte, 2048))
			require.Zero(t, n, "a disabled relay must not receive probe packets")
			var timeout net.Error
			require.ErrorAs(t, err, &timeout)
			require.True(t, timeout.Timeout())
		})
	}
}

func TestUDPAPIUoTThroughTCPOnlyShadowsocksDetour(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	done := make(chan error, 1)
	go func() {
		done <- func() error {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				return err
			}
			// Two unencrypted SS headers: relay -> exit, then exit -> UoT.
			for range 2 {
				if _, err = M.SocksaddrSerializer.ReadAddrPort(conn); err != nil {
					return err
				}
			}
			request, err := uot.ReadRequest(conn)
			if err != nil {
				return err
			}
			packet := uot.NewConn(conn, *request)
			buffer := make([]byte, 4096)
			n, address, err := packet.ReadFrom(buffer)
			if err != nil {
				return err
			}
			var query dns.Msg
			if err = query.Unpack(buffer[:n]); err != nil {
				return err
			}
			reply := new(dns.Msg).SetReply(&query)
			reply.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET}, A: net.IPv4(192, 0, 2, 1)}}
			data, err := reply.Pack()
			if err != nil {
				return err
			}
			_, err = packet.WriteTo(data, address)
			return err
		}()
	}()
	_, s, _ := startNamedSubscriptionBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"shadowsocks","tag":"exit","server":"127.0.0.1","server_port":12345,"method":"none","udp_over_tcp":true,"detour":"hop"},{"type":"selector","tag":"hop","outbounds":["relay"]},{"type":"shadowsocks","tag":"relay","server":"127.0.0.1","server_port":%d,"method":"none","network":"tcp"}]}`, listener.Addr().(*net.TCPAddr).Port))
	w := udpRequest(s, "exit", url.Values{"timeout": {"1000"}})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var result adapter.UDPTestHistory
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.True(t, result.Alive, w.Body.String())
	require.Positive(t, result.Delay)
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("UoT peer did not finish")
	}
}

func TestUDPAPIUsesRequestedTargetAndStoresOnlyPinnedLeaf(t *testing.T) {
	var destination M.Socksaddr
	questions := make(chan string, 2)
	leaf := &delayedProbe{&namedTestOutbound{name: "leaf"}, func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		require.Equal(t, "udp", network)
		require.True(t, adapter.IsURLTest(ctx))
		destination = dst
		client, peer := net.Pipe()
		go func() {
			defer peer.Close()
			buf := make([]byte, 65535)
			n, err := peer.Read(buf)
			if err != nil {
				return
			}
			var q dns.Msg
			if q.Unpack(buf[:n]) != nil {
				return
			}
			questions <- q.Question[0].Name
			r := new(dns.Msg).SetReply(&q)
			r.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: q.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET}, A: net.IPv4(192, 0, 2, 1)}}
			packet, _ := r.Pack()
			peer.Write(packet)
		}()
		return client, nil
	}}
	options, _ := urltest.NormalizeUDPTest(option.UDPTestOptions{Domain: "group.example", Server: "192.0.2.53", ServerPort: 5353, Timeout: badoption.Duration(time.Second)})
	tcp := &tcpOnlyUDPProbe{&delayedProbe{&namedTestOutbound{name: "tcp-only"}, func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		t.Error("UDP API must not dial the TCP-selected node")
		return nil, errors.New("unexpected TCP leaf")
	}}}
	b := &networkAPIProbeGroup{&configuredProbeGroup{&testProvider{name: "b", all: []string{"tcp-only", "leaf"}}, tcp, "https://b.example/"}, leaf}
	a := &configuredUDPProbeGroup{&configuredProbeGroup{&testProvider{name: "a", all: []string{"b"}}, b, "https://a.example/"}, options}
	s, _ := probeServer(t, []adapter.Outbound{a, b, leaf, tcp})
	w := udpRequest(s, "a", nil)
	require.Equal(t, 200, w.Code, w.Body.String())
	var result adapter.UDPTestHistory
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.True(t, result.Alive)
	require.Equal(t, "leaf", result.Outbound)
	require.Equal(t, "group.example.", <-questions)
	require.Equal(t, "192.0.2.53:5353", destination.String())
	require.NotNil(t, s.urlTestHistory.LoadUDPHistory("leaf", options))
	require.Empty(t, s.urlTestHistory.UDPHistories("a"))
	require.Empty(t, s.urlTestHistory.UDPHistories("b"))
	w = udpRequest(s, "a", url.Values{"server": {"192.0.2.54"}, "port": {"54"}, "domain": {"override.example"}, "timeout": {"1234"}})
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "override.example.", <-questions)
	require.Equal(t, "192.0.2.54:54", destination.String())
	require.Len(t, s.urlTestHistory.UDPHistories("leaf"), 2)
	for _, query := range []url.Values{{"port": {"0"}}, {"port": {"65536"}}, {"timeout": {"0"}}, {"timeout": {"60001"}}, {"domain": {"a b"}}, {"server": {"udp://1.1.1.1"}}} {
		require.Equal(t, 400, udpRequest(s, "a", query).Code)
	}
}

func TestUDPAPITimeoutAndMembershipChange(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "refresh"}[changed], func(t *testing.T) {
			var updates *adapter.GroupUpdates
			leaf := &delayedProbe{&namedTestOutbound{name: "leaf"}, func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
				if changed {
					updates.Notify()
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			s, u := probeServer(t, []adapter.Outbound{leaf})
			updates = u
			w := udpRequest(s, "leaf", url.Values{"timeout": {"20"}})
			if changed {
				require.Equal(t, 409, w.Code)
				require.Empty(t, s.urlTestHistory.UDPHistories("leaf"))
			} else {
				require.Equal(t, 200, w.Code, w.Body.String())
				var result adapter.UDPTestHistory
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
				require.False(t, result.Alive)
				require.Zero(t, result.Delay)
			}
		})
	}
}
