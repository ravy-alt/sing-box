//go:build with_quic

package subscription

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	"github.com/sagernet/sing-box/protocol/trojan"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/protocol/vmess"
	_ "github.com/sagernet/sing-box/transport/v2rayquic"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestDialerChainV2RayTransportNetwork(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	for _, tc := range []struct{ kind, credentials string }{
		{"vmess", `"uuid":"00000000-0000-0000-0000-000000000001"`},
		{"vless", `"uuid":"00000000-0000-0000-0000-000000000001"`},
		{"trojan", `"password":"test"`},
		{"shadowsocks", `"method":"aes-128-gcm","password":"test"`},
	} {
		for _, transport := range []string{"ws", "quic"} {
			t.Run(tc.kind+"/"+transport, func(t *testing.T) {
				settings := fmt.Sprintf(`"transport":{"type":%q},"tls":{"enabled":true}`, transport)
				if tc.kind == "shadowsocks" {
					mode := transport
					if mode == "ws" {
						mode = "websocket"
					}
					// SIP003's mux wrapper must preserve the transport's network.
					settings = fmt.Sprintf(`"plugin":"v2ray-plugin","plugin_opts":"tls;mode=%s","udp_over_tcp":true`, mode)
				}
				remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					fmt.Fprintf(w, `{"outbounds":[{"type":%q,"tag":"proxy","server":"192.0.2.1","server_port":443,%s,%s,"detour":"hop"},
					{"type":"smart-urltest","tag":"hop","outbounds":["tcp-hop","udp-hop"],"url":%q},
					{"type":"diagnostic-network","tag":"tcp-hop","networks":["tcp"]},
					{"type":"diagnostic-network","tag":"udp-hop","networks":["udp"]}]}`, tc.kind, tc.credentials, settings, probe.URL)
				}))
				defer remote.Close()
				ctx := diagnosticSubscriptionContext()
				registry := service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry)
				vmess.RegisterOutbound(registry)
				vless.RegisterOutbound(registry)
				trojan.RegisterOutbound(registry)
				shadowsocks.RegisterOutbound(registry)
				_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, ""))
				manager := service.FromContext[adapter.OutboundManager](ctx)
				hop := "tcp-hop"
				if transport == "quic" {
					hop = "udp-hop"
				}
				for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
					require.Equal(t, []string{"proxy", "hop", hop}, adapter.InspectOutboundDialerChain(ctx, manager, memberTag("sub", "proxy"), network))
				}
			})
		}
	}
}
