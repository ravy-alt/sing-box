package urltest

import (
	"context"
	"errors"
	"io"
	"net"
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

type udpProbeNode struct {
	outbound.Adapter
	dial func(context.Context, string, M.Socksaddr) (net.Conn, error)
}

func (n *udpProbeNode) DialContext(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
	return n.dial(ctx, network, dst)
}
func (n *udpProbeNode) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected packet dial")
}

func TestUDPProbeValidatesDNSAndNeverFallsBackToTCP(t *testing.T) {
	for _, mode := range []string{"answer", "wrong-id-then-answer", "wrong-question", "truncated", "nxdomain", "empty", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			go func() {
				buffer := make([]byte, 2048)
				count, peer, err := listener.ReadFrom(buffer)
				if err != nil {
					return
				}
				var query dns.Msg
				if query.Unpack(buffer[:count]) != nil {
					return
				}
				if mode == "timeout" {
					return
				}
				answer := new(dns.Msg).SetReply(&query)
				answer.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.IPv4(192, 0, 2, 1)}}
				switch mode {
				case "wrong-id-then-answer":
					answer.Id++
					packet, _ := answer.Pack()
					listener.WriteTo(packet, peer)
					answer.Id--
				case "wrong-question":
					answer.Question[0].Name = "other.example."
				case "truncated":
					answer.Truncated = true
				case "nxdomain":
					answer.Rcode = dns.RcodeNameError
				case "empty":
					answer.Answer = nil
				}
				packet, _ := answer.Pack()
				listener.WriteTo(packet, peer)
			}()
			node := &udpProbeNode{Adapter: outbound.NewAdapter("test", "leaf", []string{"tcp", "udp"}, nil), dial: func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
				require.Equal(t, "udp", network)
				require.True(t, adapter.IsURLTest(ctx))
				return (&net.Dialer{}).DialContext(ctx, network, dst.String())
			}}
			options := option.UDPTestOptions{Server: "127.0.0.1", ServerPort: uint16(listener.LocalAddr().(*net.UDPAddr).Port), Domain: "example.com", Timeout: badoption.Duration(80 * time.Millisecond)}
			result := UDPTestWithResult(context.Background(), options, node)
			if mode == "answer" || mode == "wrong-id-then-answer" {
				require.NoError(t, result.Err)
				require.Positive(t, result.Delay)
			} else {
				require.Error(t, result.Err)
				require.Zero(t, result.Delay)
			}
			require.Equal(t, "leaf", result.Outbound)
		})
	}
}

func TestUDPHistoryProfilesRetirementAndLateCompletions(t *testing.T) {
	options, err := NormalizeUDPTest(option.UDPTestOptions{})
	require.NoError(t, err)
	history := NewHistoryStorage()
	started := time.Now()
	profiles := []option.UDPTestOptions{options, options, options, options, options}
	profiles[1].Server = "1.1.1.1"
	profiles[2].ServerPort = 5353
	profiles[3].Domain = "other.example."
	profiles[4].Timeout = badoption.Duration(time.Second)
	for _, profile := range profiles {
		(UDPResult{Options: profile, Started: started, Outbound: "leaf", Delay: 10}).Store(history)
	}
	require.Len(t, history.UDPHistories("leaf"), 5)
	(UDPResult{Options: options, Started: started.Add(time.Second), Outbound: "leaf", Err: context.DeadlineExceeded}).Store(history)
	(UDPResult{Options: options, Started: started, Outbound: "leaf", Delay: 1}).Store(history)
	require.False(t, history.LoadUDPHistory("leaf", options).Alive)
	require.True(t, history.LoadUDPHistory("leaf", profiles[1]).Alive)
	(UDPResult{Options: profiles[1], Started: started.Add(time.Second), Outbound: "leaf", Err: context.Canceled}).Store(history)
	require.True(t, history.LoadUDPHistory("leaf", profiles[1]).Alive)
	history.DeleteURLTestHistory("leaf")
	require.Empty(t, history.UDPHistories("leaf"))
}

func TestUDPProbeClosesLateDialsAndBoundsWait(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client, peer := net.Pipe()
	defer peer.Close()
	node := &udpProbeNode{Adapter: outbound.NewAdapter("test", "leaf", []string{"udp"}, nil), dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) { <-release; return client, nil }}
	start := time.Now()
	result := UDPTestWithResult(context.Background(), option.UDPTestOptions{Timeout: badoption.Duration(20 * time.Millisecond)}, node)
	require.ErrorIs(t, result.Err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 200*time.Millisecond)
	// The shared worker must close a late connection instead of publishing it.
	go func() { release <- struct{}{} }()
	peer.SetReadDeadline(time.Now().Add(time.Second))
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.Eventually(t, func() bool { return len(udpProbeSlots) == 0 }, time.Second, time.Millisecond)
}

func TestUDPProbeOptions(t *testing.T) {
	options, err := NormalizeUDPTest(option.UDPTestOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 53, options.ServerPort)
	require.Equal(t, "example.com.", options.Domain)
	for _, options := range []option.UDPTestOptions{{Server: "udp://1.1.1.1"}, {Server: "1.1.1.1:53"}, {Domain: "not a domain"}, {Timeout: -1}, {Timeout: badoption.Duration(2 * time.Minute)}} {
		_, err := NormalizeUDPTest(options)
		require.Error(t, err)
	}
}
