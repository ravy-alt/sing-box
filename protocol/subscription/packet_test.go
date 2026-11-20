package subscription

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestUDPDNSAddressSurvivesRefresh(t *testing.T) {
	for _, member := range []bool{false, true} {
		t.Run(fmt.Sprintf("member=%t", member), func(t *testing.T) {
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{"dns":{"servers":[{"type":"hosts","tag":"private","predefined":{"echo.local":"127.0.0.1"}}]},"outbounds":[{"type":"direct","tag":"exit","domain_resolver":"private"}]}`)
			}))
			defer remote.Close()
			_, ctx := startBox(t, configFor(remote.URL, ""))
			s := getSubscription(t, ctx, "sub")
			var outbound adapter.Outbound = s
			if member {
				var found bool
				outbound, found = service.FromContext[adapter.OutboundManager](ctx).Outbound(memberTag("sub", "exit"))
				require.True(t, found)
			}
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			require.NoError(t, err)
			defer udp.Close()
			destination := M.Socksaddr{Fqdn: "echo.local", Port: M.SocksaddrFromNet(udp.LocalAddr()).Port}
			conn, err := outbound.ListenPacket(ctx, destination)
			require.NoError(t, err)
			defer conn.Close()
			packet := bufio.NewPacketConn(conn)
			old := s.current
			for _, refresh := range []bool{false, true} {
				if refresh {
					require.NoError(t, s.Refresh(ctx))
					require.False(t, old.closed)
				}
				require.NoError(t, packet.SetDeadline(time.Now().Add(5*time.Second)))
				require.NoError(t, udp.SetDeadline(time.Now().Add(5*time.Second)))
				require.NoError(t, packet.WritePacket(buf.As([]byte("echo")), destination))
				data := make([]byte, 64)
				n, sender, err := udp.ReadFrom(data)
				require.NoError(t, err)
				_, err = udp.WriteTo(data[:n], sender)
				require.NoError(t, err)
				response := buf.NewPacket()
				address, err := packet.ReadPacket(response)
				body := string(response.Bytes())
				response.Release()
				require.NoError(t, err)
				require.Equal(t, destination, address, "UDP reply must preserve the resolved domain")
				require.Equal(t, "echo", body)
			}
			require.NoError(t, conn.Close())
			require.True(t, old.closed)
		})
	}
}
