package subscription

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	mDNS "github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestDNSPoolReleasesRetiredSubscription(t *testing.T) {
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	senders := make(chan string, 8)
	t.Cleanup(func() { packet.Close(); <-done })
	go func() {
		defer close(done)
		b := make([]byte, 4096)
		for {
			n, a, e := packet.ReadFrom(b)
			if e != nil {
				return
			}
			var q mDNS.Msg
			if q.Unpack(b[:n]) != nil {
				continue
			}
			r := new(mDNS.Msg).SetReply(&q)
			rr, _ := mDNS.NewRR(q.Question[0].Name + " 60 IN A 192.0.2.1")
			r.Answer = []mDNS.RR{rr}
			raw, _ := r.Pack()
			senders <- a.String()
			packet.WriteTo(raw, a)
		}
	}()
	var content atomic.Value
	content.Store(`{"outbounds":[{"type":"direct","tag":"old"}]}`)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, content.Load().(string)) }))
	t.Cleanup(source.Close)
	port := packet.LocalAddr().(*net.UDPAddr).Port
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"bootstrap"},{"type":"udp","tag":"d","server":"127.0.0.1","server_port":%d,"detour":"sub"}],"final":"d","disable_cache":true},"route":{"default_domain_resolver":"bootstrap"},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, port, source.URL))
	s := getSubscription(t, ctx, "sub")
	router := service.FromContext[adapter.DNSRouter](ctx)
	query := func(name string) string {
		c, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := router.Exchange(c, new(mDNS.Msg).SetQuestion(name, mDNS.TypeA), adapter.DNSQueryOptions{})
		require.NoError(t, err)
		return <-senders
	}
	sender1 := query("first.example.")
	s.mu.Lock()
	old := s.current
	s.mu.Unlock()
	content.Store(`{"outbounds":[{"type":"direct","tag":"new"}]}`)
	require.NoError(t, s.Refresh(ctx))
	require.Eventually(t, func() bool { old.mu.Lock(); defer old.mu.Unlock(); return old.closed && old.refs == 0 }, time.Second, time.Millisecond, "idle pool must retire without another DNS query")

	sender2 := query("second.example.")
	servers, e := router.(adapter.DNSInspector).DNSServers()
	require.NoError(t, e)
	for _, server := range servers {
		if server.Tag == "d" {
			t.Logf("reported chain after refresh: %v", server.DialerChain)
		}
	}
	old.mu.Lock()
	refs, closed := old.refs, old.closed
	old.mu.Unlock()
	t.Logf("old generation after refresh: refs=%d closed=%v; UDP peer same=%v", refs, closed, sender1 == sender2)

	require.Eventually(t, func() bool { old.mu.Lock(); defer old.mu.Unlock(); return old.closed && old.refs == 0 }, time.Second, time.Millisecond, "membership update releases the old generation without explicit cleanup")
	require.NotEqual(t, sender1, sender2, "a new DNS query after removal should not reuse the deleted node's idle socket")
}
