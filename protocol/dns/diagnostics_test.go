package dns

import (
	"context"
	"io"
	"testing"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type diagnosticPacketConn struct{ N.PacketConn }

func (c diagnosticPacketConn) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, io.EOF
}
func (c diagnosticPacketConn) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}
func (c diagnosticPacketConn) Close() error { return nil }

type diagnosticRouter struct {
	adapter.DNSRouter
	infos []adapter.DNSQueryInfo
}

func (r *diagnosticRouter) ExchangeAsync(ctx context.Context, message *dns.Msg, _ adapter.DNSQueryOptions, cb func(*dns.Msg, error)) {
	r.infos = append(r.infos, adapter.DNSQueryInfoFromContext(ctx))
	cb(new(dns.Msg).SetReply(message), nil)
}

func TestDNSPacketSessionTracksEachDestination(t *testing.T) {
	message, err := new(dns.Msg).SetQuestion("example.com.", dns.TypeA).Pack()
	require.NoError(t, err)
	packets := []*N.PacketBuffer{}
	for _, destination := range []string{"8.8.8.8:53", "192.0.2.53:5656"} {
		buffer := buf.NewSize(len(message))
		_, err := buffer.Write(message)
		require.NoError(t, err)
		packets = append(packets, &N.PacketBuffer{Buffer: buffer, Destination: M.ParseSocksaddr(destination)})
	}
	resolver := &diagnosticRouter{}
	_ = NewDNSPacketConnection(context.Background(), resolver, diagnosticPacketConn{}, packets, adapter.InboundContext{Source: M.ParseSocksaddr("192.0.2.1:12345")})
	require.Len(t, resolver.infos, 2)
	require.Equal(t, "8.8.8.8", resolver.infos[0].Destination)
	require.Equal(t, uint16(53), resolver.infos[0].DestinationPort)
	require.Equal(t, "192.0.2.53", resolver.infos[1].Destination)
	require.Equal(t, uint16(5656), resolver.infos[1].DestinationPort)
	require.Equal(t, uint16(12345), resolver.infos[1].SourcePort)
}
