package route

import (
	"context"
	"testing"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type diagnosticDNSRouter struct {
	adapter.DNSRouter
	info        adapter.DNSQueryInfo
	destination M.Socksaddr
}

func (r *diagnosticDNSRouter) ExchangeAsync(ctx context.Context, _ *dns.Msg, _ adapter.DNSQueryOptions, callback func(*dns.Msg, error)) {
	r.info = adapter.DNSQueryInfoFromContext(ctx)
	r.destination = adapter.ContextFrom(ctx).Destination
	callback(nil, context.Canceled)
}

func TestHijackedDNSPreservesOriginalEndpoint(t *testing.T) {
	resolver := &diagnosticDNSRouter{}
	router := &Router{dns: resolver}
	message, err := new(dns.Msg).SetQuestion("example.com.", dns.TypeA).Pack()
	require.NoError(t, err)
	router.HijackDNSPacket(context.Background(), message, nil, adapter.InboundContext{
		Source: M.ParseSocksaddr("192.0.2.10:12345"), Destination: M.ParseSocksaddr("8.8.8.8:53"),
	})
	require.Equal(t, "8.8.8.8", resolver.info.Destination)
	require.Equal(t, uint16(53), resolver.info.DestinationPort)
	require.Equal(t, uint16(12345), resolver.info.SourcePort)
	require.False(t, resolver.destination.IsValid(), "diagnostics must not change DNS rule matching metadata")
}
