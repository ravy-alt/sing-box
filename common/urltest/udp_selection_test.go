package urltest

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type networkUDPProbeGroup struct {
	*udpProbeNode
	tcp, udp adapter.Outbound
}

func (g *networkUDPProbeGroup) All() []string { return []string{g.tcp.Tag(), g.udp.Tag()} }
func (g *networkUDPProbeGroup) Selected(network string) adapter.Outbound {
	if network == "udp" {
		return g.udp
	}
	return g.tcp
}
func (g *networkUDPProbeGroup) AttachConnection(io.Closer) func() { return func() {} }

func TestUDPProbeUsesUDPSelection(t *testing.T) {
	for _, tcpUDP := range []bool{false, true} {
		t.Run(map[bool]string{false: "TCP leaf disables UDP", true: "different UDP-capable leaves"}[tcpUDP], func(t *testing.T) {
			networks := []string{"tcp"}
			if tcpUDP {
				networks = append(networks, "udp")
			}
			tcpCalls, udpCalls := 0, 0
			tcp := &udpProbeNode{Adapter: outbound.NewAdapter("test", "tcp-leaf", networks, nil), dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				tcpCalls++
				return nil, errors.New("wrong leaf")
			}}
			udp := &udpProbeNode{Adapter: outbound.NewAdapter("test", "udp-leaf", []string{"tcp", "udp"}, nil), dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) {
				udpCalls++
				return nil, errors.New("UDP leaf dialed")
			}}
			inner := &networkUDPProbeGroup{udpProbeNode: &udpProbeNode{Adapter: outbound.NewAdapter("urltest", "inner", []string{"tcp", "udp"}, nil)}, tcp: tcp, udp: udp}
			outer := &networkUDPProbeGroup{udpProbeNode: &udpProbeNode{Adapter: outbound.NewAdapter("selector", "outer", []string{"tcp", "udp"}, nil)}, tcp: inner, udp: inner}
			result := UDPTestWithResult(context.Background(), option.UDPTestOptions{}, outer)
			t.Logf("result=%s err=%v TCP-leaf dials=%d UDP-leaf dials=%d", result.Outbound, result.Err, tcpCalls, udpCalls)
			require.Equal(t, "udp-leaf", result.Outbound)
			require.Equal(t, 0, tcpCalls)
			require.Equal(t, 1, udpCalls)
		})
	}
}
