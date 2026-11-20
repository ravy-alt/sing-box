package subscription

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestDNSOverIPDirectPathIsNotResolutionCycle(t *testing.T) {
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &mDNS.Server{PacketConn: packet, Handler: mDNS.HandlerFunc(func(w mDNS.ResponseWriter, q *mDNS.Msg) {
		r := new(mDNS.Msg).SetReply(q)
		if q.Question[0].Qtype == mDNS.TypeA {
			r.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: q.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 10}, A: net.ParseIP("127.0.0.1")}}
		}
		_ = w.WriteMsg(r)
	})}
	go func() { _ = server.ActivateAndServe() }()
	defer server.Shutdown()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"direct","tag":"node","connect_timeout":"1s"}]}`)
	}))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	for _, subscribed := range []bool{false, true} {
		t.Run(fmt.Sprint(subscribed), func(t *testing.T) {
			var declarations string
			if subscribed {
				declarations = fmt.Sprintf(`"subscriptions":[{"tag":"s","url":%q,"http_client":{},"override_dialer":{"domain_resolver":{"server":"race","strategy":"ipv4_only"}}}],"outbounds":[{"type":"selector","tag":"g","import":[{"group":"s"}]}]`, source.URL)
			} else {
				declarations = `"outbounds":[{"type":"direct","tag":"node","connect_timeout":"1s","domain_resolver":{"server":"race","strategy":"ipv4_only"}},{"type":"selector","tag":"g","outbounds":["node"]}]`
			}
			config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"udp","tag":"udp","server":"127.0.0.1","server_port":%d,"detour":"g"},{"type":"hosts","tag":"negative"},{"type":"race","tag":"race","servers":["udp","negative"]}]},%s}`, packet.LocalAddr().(*net.UDPAddr).Port, declarations)
			_, ctx := startBox(t, config)
			outbound, _ := service.FromContext[adapter.OutboundManager](ctx).Outbound("g")
			destination := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
			conn, err := outbound.DialContext(ctx, "tcp", M.ParseSocksaddrHostPort("private.invalid", destination.Port))
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		})
	}
}
