package subscription

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestOverridePrivateDoHRaceAndChain(t *testing.T) {
	var fastCalls, slowCalls atomic.Int32
	slowStarted := make(chan struct{}, 1)
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowCalls.Add(1)
		select {
		case slowStarted <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
			w.WriteHeader(503)
		}
	}))
	defer slow.Close()
	fast := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fastCalls.Add(1)
		select {
		case <-slowStarted:
		case <-r.Context().Done():
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		var query mDNS.Msg
		if query.Unpack(body) != nil {
			w.WriteHeader(400)
			return
		}
		response := new(mDNS.Msg).SetReply(&query)
		if query.Question[0].Qtype == mDNS.TypeA {
			response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: query.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60}, A: net.ParseIP("127.0.0.1").To4()}}
		}
		data, _ := response.Pack()
		w.Header().Set("Content-Type", "application/dns-message")
		_, _ = w.Write(data)
	}))
	defer fast.Close()
	proxy, hits := auditHTTPProxy(t)
	relay, relayHits := auditHTTPProxy(t)
	proxyAddr := M.ParseSocksaddr(strings.TrimPrefix(proxy.URL, "http://"))
	relayAddr := M.ParseSocksaddr(strings.TrimPrefix(relay.URL, "http://"))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"hosts","tag":"private-race","predefined":{"node.private.invalid":"192.0.2.99"}}]},"outbounds":[{"type":"http","tag":"节点","server":"node.private.invalid","server_port":%d,"detour":"relay","domain_resolver":"lost-by-converter"},{"type":"http","tag":"relay","server":"127.0.0.1","server_port":%d}]}`, proxyAddr.Port, relayAddr.Port)
	}))
	defer source.Close()
	doh := func(tag, address string) string {
		addr := M.ParseSocksaddr(strings.TrimPrefix(address, "https://"))
		return fmt.Sprintf(`{"type":"https","tag":%q,"server":"127.0.0.1","server_port":%d,"tls":{"enabled":true,"insecure":true}}`, tag, addr.Port)
	}
	config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[%s,%s,{"type":"race","tag":"private-race","servers":["slow","fast"],"timeout":"1s"}]},"subscriptions":[{"tag":"airport","url":%q,"http_client":{},"override_dialer":{"domain_resolver":{"server":"private-race","strategy":"ipv4_only","disable_cache":true}}}],"outbounds":[{"type":"selector","tag":"proxy","import":[{"group":"airport","include":"^节点$"}]}]}`, doh("slow", slow.URL), doh("fast", fast.URL), source.URL)
	_, ctx := startBox(t, config)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "private DNS and two-hop chain") }))
	defer target.Close()
	selected, _ := service.FromContext[adapter.OutboundManager](ctx).Outbound("proxy")
	httpTransport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return selected.DialContext(ctx, network, M.ParseSocksaddr(address))
	}}
	defer httpTransport.CloseIdleConnections()
	client := &http.Client{Transport: httpTransport, Timeout: 3 * time.Second}
	response, err := client.Get(target.URL)
	require.NoError(t, err)
	content, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "private DNS and two-hop chain", string(content))
	require.Positive(t, fastCalls.Load())
	require.Positive(t, slowCalls.Load())
	require.EqualValues(t, 1, hits.Load())
	require.EqualValues(t, 1, relayHits.Load())
	s := getSubscription(t, ctx, "airport")
	require.NoError(t, s.Refresh(ctx))
	httpTransport.CloseIdleConnections()
	response, err = client.Get(target.URL)
	require.NoError(t, err)
	response.Body.Close()
	require.EqualValues(t, 2, hits.Load())
	require.EqualValues(t, 2, relayHits.Load())
}

func TestOverridePreservesUnsetFieldsAndIsScopeLocal(t *testing.T) {
	_, ctx := startBox(t, `{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"private"}]},"outbounds":[{"type":"direct","tag":"direct"}]}`)
	content := []byte(`{"outbounds":[{"type":"test","tag":"a","detour":"b","domain_resolver":"missing","bind_interface":"old","tcp_fast_open":true,"routing_mark":123,"connect_timeout":"9s"},{"type":"test","tag":"b"}],"endpoints":[{"type":"test","tag":"endpoint"}]}`)
	override, err := json.UnmarshalExtendedContext[option.SubscriptionDialerOverride](ctx, []byte(`{"domain_resolver":"private","bind_interface":"","tcp_fast_open":false,"routing_mark":0}`))
	require.NoError(t, err)
	doc, err := parseDocument(ctx, "a", content, nil, &override)
	require.NoError(t, err)
	a := doc.options.Outbounds[0].Options.(*testOptions)
	b := doc.options.Outbounds[1].Options.(*testOptions)
	require.Equal(t, memberTag("a", "b"), a.Detour)
	require.Equal(t, "", a.BindInterface)
	require.False(t, a.TCPFastOpen)
	require.Zero(t, a.RoutingMark)
	require.Equal(t, 9*time.Second, time.Duration(a.ConnectTimeout))
	require.Equal(t, "private", a.DomainResolver.Server)
	a.DomainResolver.Server = "mutated"
	require.Equal(t, "private", b.DomainResolver.Server)
	require.Equal(t, "private", override.DomainResolver.Server)
	require.Equal(t, "private", doc.options.Endpoints[0].Options.(*testOptions).DomainResolver.Server)
	_, err = parseDocument(ctx, "other", content, nil)
	require.ErrorContains(t, err, "missing dns_server reference", "override must not leak to another source")
}

func TestOverrideRaceCrossScopeCycleRejected(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"http","tag":"node","server":"private.invalid","server_port":443}]}`)
	}))
	defer source.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"race","tag":"race","servers":["a","b"]},{"type":"udp","tag":"a","server":"127.0.0.1","detour":"g"},{"type":"hosts","tag":"b"}]},"subscriptions":[{"tag":"source","url":%q,"http_client":{},"override_dialer":{"domain_resolver":"race"}}],"outbounds":[{"type":"selector","tag":"g","import":[{"group":"source"}]}]}`, source.URL)
	ctx := testContext()
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	t.Cleanup(func() { instance.Close() })
	require.ErrorContains(t, instance.Start(), "circular")
}

func TestHostRaceCyclesWithoutSubscriptions(t *testing.T) {
	for _, config := range []string{
		`{"dns":{"servers":[{"type":"race","tag":"race","servers":["a","b"]},{"type":"udp","tag":"a","server":"127.0.0.1","detour":"node"},{"type":"hosts","tag":"b"}]},"outbounds":[{"type":"http","tag":"node","server":"private.invalid","server_port":443,"domain_resolver":"race"}]}`,
		`{"dns":{"servers":[{"type":"race","tag":"a","servers":["b","c"]},{"type":"race","tag":"b","servers":["a","c"]},{"type":"hosts","tag":"c"}]}}`,
	} {
		ctx := testContext()
		opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
		require.NoError(t, err)
		instance, err := box.New(box.Options{Options: opts, Context: ctx})
		require.NoError(t, err)
		require.ErrorContains(t, instance.Start(), "circular")
		instance.Close()
	}
}
