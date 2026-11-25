package openvpn

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type transportTestRelay struct {
	adapter.Outbound
	networks []string
}

func (r transportTestRelay) Type() string           { return "direct" }
func (r transportTestRelay) Tag() string            { return "relay" }
func (r transportTestRelay) Network() []string      { return r.networks }
func (r transportTestRelay) Dependencies() []string { return nil }

type transportTestManager struct {
	adapter.OutboundManager
	relay adapter.Outbound
}

func (m transportTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	return m.relay, tag == "relay"
}

func TestOpenVPNProbeUsesTunnelTransport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		options     option.OpenVPNClientEndpointOptions
		relay       []string
		unsupported bool
	}{
		{"TCP tunnel carries UDP", option.OpenVPNClientEndpointOptions{Network: "tcp"}, []string{"tcp"}, false},
		{"TCP6 tunnel carries UDP", option.OpenVPNClientEndpointOptions{Network: "tcp6"}, []string{"tcp"}, false},
		{"TCP tunnel cannot use UDP-only relay", option.OpenVPNClientEndpointOptions{Network: "tcp"}, []string{"udp"}, true},
		{"default tunnel needs UDP", option.OpenVPNClientEndpointOptions{}, []string{"tcp"}, true},
		{"UDP4 tunnel", option.OpenVPNClientEndpointOptions{Network: "udp4"}, []string{"udp"}, false},
		{"mixed remote fallback", option.OpenVPNClientEndpointOptions{Servers: []option.OpenVPNRemoteOptions{{Network: "udp"}, {Network: "tcp"}}}, []string{"tcp"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := tc.options
			if len(options.Servers) == 0 {
				options.Server = "127.0.0.1"
			}
			options.Detour = "relay"
			_, remotes := buildClientRemoteOptions(options)
			c := &ClientEndpoint{endpointBase: endpointBase{Adapter: endpoint.NewAdapterWithDialerOptions(C.TypeOpenVPNClient, "vpn", []string{"tcp", "udp"}, options.DialerOptions)}}
			for _, remote := range remotes {
				c.dialerNetworks = append(c.dialerNetworks, N.NetworkName(remote.Protocol))
			}
			manager := transportTestManager{relay: transportTestRelay{networks: tc.relay}}
			for _, payload := range []string{"tcp", "udp"} {
				err := adapter.CheckOutboundDialerNetwork(context.Background(), c, manager, payload)
				if tc.unsupported {
					require.ErrorIs(t, err, adapter.ErrOutboundNetworkUnsupported)
				} else {
					require.NoError(t, err)
				}
			}
			require.Equal(t, N.NetworkName(remotes[0].Protocol), adapter.DialerNetworkFor(c, "udp"))
			c.state.Store(&clientState{tunnelInfo: adapter.OpenVPNTunnelInfo{Network: "tcp"}})
			require.Equal(t, "tcp", adapter.DialerNetworkFor(c, "udp"), "diagnostics follow the established transport")
		})
	}
}
