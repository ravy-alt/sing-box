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
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type splitNetworkGroup struct {
	*historyTestGroup
	udp adapter.Outbound
}

func (g *splitNetworkGroup) Selected(network string) adapter.Outbound {
	if network == "udp" {
		return g.udp
	}
	return g.selected
}
func TestSmartNestedUDPSelection(t *testing.T) {
	tcp, dead, healthy := smartNode("tcp-leaf", nil), smartNode("udp-dead", nil), smartNode("healthy", nil)
	child := &splitNetworkGroup{&historyTestGroup{smartNode("child", nil), tcp, ""}, dead}
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{}, child, healthy)
	recordSmartProbe(s, "tcp-leaf", 10, 10)
	recordSmartProbe(s, "udp-dead", 20, 0)
	recordSmartProbe(s, "healthy", 100, 100)
	require.Equal(t, "healthy", s.Selected("udp").Tag(), "UDP ranking must assess the actual UDP-selected leaf instead of the TCP leaf")
}

// All advertises candidates beyond the two currently selected leaves.
type candidateTestGroup struct {
	*splitNetworkGroup
	members []string
}

func (g *candidateTestGroup) All() []string { return g.members }

func TestSmartProbesAllNestedUDPCandidatesAfterTCPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	t.Cleanup(server.Close)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	t.Cleanup(func() { packet.Close(); <-done })
	go func() {
		defer close(done)
		buffer := make([]byte, 4096)
		for {
			n, peer, e := packet.ReadFrom(buffer)
			if e != nil {
				return
			}
			var query dns.Msg
			if query.Unpack(buffer[:n]) != nil {
				continue
			}
			reply := new(dns.Msg).SetReply(&query)
			reply.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.IPv4(192, 0, 2, 1)}}
			raw, _ := reply.Pack()
			packet.WriteTo(raw, peer)
		}
	}()
	dial := func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	}
	tcp := smartNode("tcp", dial)
	tcp.udpDial = dial
	failed := smartNode("failed-http", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("URL unavailable")
	})
	failed.udpDial = dial
	udpOnly := smartNode("udp-only", nil)
	udpOnly.Adapter = outbound.NewAdapter("test", "udp-only", []string{"udp"}, nil)
	udpOnly.udpDial = dial
	unselected := smartNode("unselected", dial)
	unselected.udpDial = dial
	inner := &candidateTestGroup{&splitNetworkGroup{&historyTestGroup{smartNode("inner", nil), tcp, server.URL}, failed}, []string{"tcp", "failed-http", "udp-only", "unselected"}}
	outer := &historyTestGroup{smartNode("outer", nil), inner, server.URL}
	s, manager := smartFixture(t, option.SmartURLTestOutboundOptions{URL: server.URL, UDPTest: &option.UDPTestOptions{Server: "127.0.0.1", ServerPort: uint16(packet.LocalAddr().(*net.UDPAddr).Port), Timeout: badoption.Duration(time.Second)}}, outer, inner)
	for _, leaf := range []adapter.Outbound{tcp, failed, udpOnly, unselected} {
		manager.set(leaf)
	}
	result, err := s.URLTest(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 2, "TCP aliases only")
	for _, leaf := range []*smartTestOutbound{tcp, failed, udpOnly, unselected} {
		observation := s.history.LoadUDPHistory(leaf.Tag(), *s.options.UDPTest)
		require.NotNil(t, observation, leaf.Tag())
		require.True(t, observation.Alive, leaf.Tag())
	}
	require.EqualValues(t, 2, tcp.calls.Load(), "deduplicate selected aliases")
	require.EqualValues(t, 2, failed.calls.Load(), "failed HTTP must not skip UDP")
	require.EqualValues(t, 1, udpOnly.calls.Load(), "UDP-only node must not be sent an HTTP probe")
	require.EqualValues(t, 2, unselected.calls.Load(), "unselected nested candidates also tested")
	require.Nil(t, s.history.LoadURLTestHistory("failed-http", s.options.URL))
	require.Nil(t, s.history.LoadURLTestObservation("udp-only", s.options.URL))
	require.Empty(t, s.history.UDPHistories("inner"))
	require.Empty(t, s.history.URLs("outer"))
	require.EqualValues(t, 0, s.probeHealth(outer, "udp").tcp)
	require.Positive(t, s.probeHealth(outer, "udp").udp)
}
