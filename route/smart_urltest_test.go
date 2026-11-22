package route

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
	"testing"
)

type dynamicTestGroup struct{ adapter.OutboundGroup }

func (*dynamicTestGroup) Tag() string       { return "smart" }
func (*dynamicTestGroup) Network() []string { return []string{"tcp", "udp"} }
func (*dynamicTestGroup) DynamicSelection() {}
func (*dynamicTestGroup) Selected(string) adapter.Outbound {
	panic("dynamic group selection must occur during dialing")
}

func TestRouteRetainsDynamicGroup(t *testing.T) {
	smart := new(dynamicTestGroup)
	for _, network := range []string{"tcp", "udp"} {
		chain, err := resolveOutbound(smart, network)
		require.NoError(t, err)
		require.Equal(t, []adapter.Outbound{smart}, chain)
	}
}
