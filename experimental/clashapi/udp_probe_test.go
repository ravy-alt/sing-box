package clashapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	b := &configuredProbeGroup{&testProvider{name: "b", all: []string{"leaf"}}, leaf, "https://b.example/"}
	a := &configuredUDPProbeGroup{&configuredProbeGroup{&testProvider{name: "a", all: []string{"b"}}, b, "https://a.example/"}, options}
	s, _ := probeServer(t, []adapter.Outbound{a, b, leaf})
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
