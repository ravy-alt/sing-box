//go:build !with_quic

package subscription

import (
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
)

func registerConnectionQUIC(*inbound.Registry, *outbound.Registry) {}
func connectionQUICCases(string, string) []connectionProtocolCase  { return nil }
