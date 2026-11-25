package openconnect

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-openconnect"
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

func TestOpenConnectProbeAllowsTCPFallbackAndRequiresControl(t *testing.T) {
	for _, transport := range []string{"", openconnect.TransportCSTP, openconnect.TransportDTLS, openconnect.TransportESP, openconnect.TransportGPST} {
		t.Run(transport, func(t *testing.T) {
			e := &Endpoint{endpointBase: endpointBase{Adapter: endpoint.NewAdapter(C.TypeOpenConnect, "vpn", []string{"tcp", "udp"}, []string{"relay"})}}
			e.state.Store(&clientState{tunnelInfo: adapter.OpenConnectTunnelInfo{Transport: transport}})
			for _, payload := range []string{"tcp", "udp"} {
				require.NoError(t, adapter.CheckOutboundDialerNetwork(context.Background(), e, transportTestManager{relay: transportTestRelay{networks: []string{"tcp"}}}, payload))
				require.ErrorIs(t, adapter.CheckOutboundDialerNetwork(context.Background(), e, transportTestManager{relay: transportTestRelay{networks: []string{"udp"}}}, payload), adapter.ErrOutboundNetworkUnsupported)
			}
			want := "tcp"
			if transport == openconnect.TransportDTLS || transport == openconnect.TransportESP {
				want = "udp"
			}
			require.Equal(t, want, adapter.DialerNetworkFor(e, "tcp"))
			require.Equal(t, want, adapter.DialerNetworkFor(e, "udp"))
		})
	}
}
