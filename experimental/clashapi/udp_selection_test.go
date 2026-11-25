package clashapi

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

type networkAPIProbeGroup struct {
	*configuredProbeGroup
	udp adapter.Outbound
}

func (g *networkAPIProbeGroup) Selected(network string) adapter.Outbound {
	if network == "udp" {
		return g.udp
	}
	return g.selected
}

func TestProxyUDPHistoryUsesUDPSelection(t *testing.T) {
	tcp, udp := &providerProbe{&namedTestOutbound{name: "tcp-leaf"}}, &providerProbe{&namedTestOutbound{name: "udp-leaf"}}
	inner := &networkAPIProbeGroup{configuredProbeGroup: &configuredProbeGroup{&testProvider{name: "inner", all: []string{"tcp-leaf", "udp-leaf"}}, tcp, ""}, udp: udp}
	outer := &configuredProbeGroup{&testProvider{name: "outer", all: []string{"inner"}}, inner, ""}
	s, _ := probeServer(t, []adapter.Outbound{outer, inner, tcp, udp})
	options, _ := urltest.NormalizeUDPTest(option.UDPTestOptions{})
	urltest.UDPResult{Options: options, Started: time.Now(), Outbound: tcp.Tag(), Delay: 11}.Store(s.urlTestHistory)
	urltest.UDPResult{Options: options, Started: time.Now(), Outbound: udp.Tag(), Delay: 22}.Store(s.urlTestHistory)
	s.urlTestHistory.StoreURLTestHistory(tcp.Tag(), &adapter.URLTestHistory{URL: urltest.DefaultURL, Time: time.Now(), Outbound: tcp.Tag(), Delay: 33})
	info := readProbeInfo(t, s, "outer", "")
	require.Equal(t, "tcp-leaf", info["history"].([]any)[0].(map[string]any)["outbound"], "TCP delay must keep its original selection")
	records := info["udpHistory"].(map[string]any)
	require.Len(t, records, 1)
	for _, record := range records {
		h := record.(map[string]any)
		t.Logf("nested group UDP history=%v", h)
		require.Equal(t, "udp-leaf", h["outbound"])
	}
	inner.udp = tcp
	info = readProbeInfo(t, s, "outer", "")
	for _, record := range info["udpHistory"].(map[string]any) {
		require.Equal(t, "tcp-leaf", record.(map[string]any)["outbound"], "history follows a changed UDP selection")
	}
	inner.udp = nil
	require.Empty(t, readProbeInfo(t, s, "outer", "")["udpHistory"], "no UDP selection must not borrow TCP history")
}
