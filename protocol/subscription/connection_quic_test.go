//go:build with_quic

package subscription

import (
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/protocol/hysteria2"
	"github.com/sagernet/sing-box/protocol/tuic"
)

func registerConnectionQUIC(i *inbound.Registry, o *outbound.Registry) {
	hysteria2.RegisterInbound(i)
	hysteria2.RegisterOutbound(o)
	tuic.RegisterInbound(i)
	tuic.RegisterOutbound(o)
}

func connectionQUICCases(serverTLS, clientTLS string) []connectionProtocolCase {
	const uuid = `"uuid":"a40c76d3-7c47-46da-b187-1598c7c8c596"`
	return []connectionProtocolCase{
		{"hysteria2", "hysteria2", `"users":[{"password":"test"}]` + serverTLS, `"password":"test"` + clientTLS, false},
		{"tuic", "tuic", `"users":[{` + uuid + `,"password":"test"}]` + serverTLS, uuid + `,"password":"test"` + clientTLS, false},
		{"tuic-uot", "tuic", `"users":[{` + uuid + `,"password":"test"}]` + serverTLS, uuid + `,"password":"test","udp_over_stream":true` + clientTLS, false},
	}
}
