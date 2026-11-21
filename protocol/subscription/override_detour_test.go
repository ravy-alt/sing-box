package subscription

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestOverrideDetourDeclaringScopeAndClear(t *testing.T) {
	_, ctx := startBox(t, `{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"upstream"}]}`)
	content := []byte(`{"outbounds":[{"type":"test","tag":"node","detour":"missing"},{"type":"test","tag":"upstream"}],"endpoints":[{"type":"test","tag":"endpoint","detour":"missing"}],"http_clients":[{"tag":"download","detour":"upstream"}]}`)
	for _, target := range []string{"upstream", ""} {
		t.Run("target="+target, func(t *testing.T) {
			override := &option.SubscriptionDialerOverride{Detour: &target}
			doc, err := parseDocument(ctx, "s", content, nil, override)
			require.NoError(t, err)
			for _, outbound := range doc.options.Outbounds {
				require.Equal(t, target, outbound.Options.(*testOptions).Detour)
			}
			require.Equal(t, target, doc.options.Endpoints[0].Options.(*testOptions).Detour)
			// Overrides affect imported nodes, never source HTTP clients.
			require.Equal(t, memberTag("s", "upstream"), doc.options.HTTPClients[0].Detour)
			if target != "" {
				require.Contains(t, doc.dependencies["outbound:"+memberTag("s", "node")], "outbound:upstream")
				require.NotContains(t, doc.dependencies["outbound:"+memberTag("s", "node")], "outbound:"+memberTag("s", "upstream"))
			}
		})
	}
	missing := "not-in-host"
	_, err := parseDocument(ctx, "s", content, nil, &option.SubscriptionDialerOverride{Detour: &missing})
	require.ErrorContains(t, err, "override_dialer.detour not found in the declaring scope")
}

func TestOverrideDetourChainRefreshAndCache(t *testing.T) {
	relay, relayHits := auditHTTPProxy(t)
	vpn, vpnHits := auditHTTPProxy(t)
	relayAddr := M.ParseSocksaddr(strings.TrimPrefix(relay.URL, "http://"))
	vpnAddr := M.ParseSocksaddr(strings.TrimPrefix(vpn.URL, "http://"))
	var body atomic.Value
	relayDocument := fmt.Sprintf(`{"outbounds":[{"type":"http","tag":"relay","server":"127.0.0.1","server_port":%d}]}`, relayAddr.Port)
	body.Store(relayDocument)
	transportSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer transportSource.Close()
	vpnSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"http","tag":"exit","server":"127.0.0.1","server_port":%d,"detour":"old-missing-chain"}]}`, vpnAddr.Port)
	}))
	defer vpnSource.Close()
	cache := filepath.Join(t.TempDir(), "vpn.json")
	config := fmt.Sprintf(`{"log":{"disabled":true},"http_clients":[{"tag":"unbound"}],"subscriptions":[{"tag":"transport","url":%q,"http_client":"unbound"},{"tag":"vpn","url":%q,"http_client":"unbound","cache_path":%q,"override_dialer":{"detour":"upstream"}}],"outbounds":[{"type":"test","tag":"direct"},{"type":"selector","tag":"upstream","outbounds":["direct"],"import":[{"group":"transport"}]},{"type":"selector","tag":"exit","import":[{"group":"vpn"}]}]}`, transportSource.URL, vpnSource.URL, cache)
	instance, ctx := startBox(t, config)
	upstream := importGroup(t, ctx, "upstream").(adapter.SelectableOutboundGroup)
	require.True(t, upstream.SelectOutbound(memberTag("transport", "relay")))
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "chained") }))
	defer target.Close()
	request := func(ctx context.Context) {
		t.Helper()
		exit := importGroup(t, ctx, "exit")
		transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return exit.DialContext(ctx, network, M.ParseSocksaddr(address))
		}}
		defer transport.CloseIdleConnections()
		response, err := (&http.Client{Transport: transport, Timeout: 3 * time.Second}).Get(target.URL)
		require.NoError(t, err)
		defer response.Body.Close()
		content, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, "chained", string(content))
	}
	request(ctx)
	require.EqualValues(t, 1, relayHits.Load())
	require.EqualValues(t, 1, vpnHits.Load())
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, getSubscription(t, ctx, "transport").Refresh(ctx))
	require.Equal(t, []string{"direct"}, upstream.All())
	request(ctx)
	require.EqualValues(t, 1, relayHits.Load())
	body.Store(relayDocument)
	require.NoError(t, getSubscription(t, ctx, "transport").Refresh(ctx))
	require.True(t, upstream.SelectOutbound(memberTag("transport", "relay")))
	request(ctx)
	require.EqualValues(t, 2, relayHits.Load())
	// A later upstream update must not close the cycle through the VPN group.
	body.Store(`{"outbounds":[{"type":"test","tag":"relay","detour":"exit"}]}`)
	generation := getSubscription(t, ctx, "transport").Status().Generation
	require.ErrorContains(t, getSubscription(t, ctx, "transport").Refresh(ctx), "circular")
	require.Equal(t, generation, getSubscription(t, ctx, "transport").Status().Generation)
	request(ctx)
	require.EqualValues(t, 3, relayHits.Load())
	require.NoError(t, instance.Close())
	vpnSource.Close()
	body.Store(relayDocument)
	config = strings.Replace(config, `"override_dialer":{"detour":"upstream"}`, `"override_dialer":{"detour":"direct"}`, 1)
	_, restoredCtx := startBox(t, config)
	require.Contains(t, getSubscription(t, restoredCtx, "vpn").current.dependencies["outbound:"+memberTag("vpn", "exit")], "outbound-ip:direct")
	request(restoredCtx)
	require.EqualValues(t, 3, relayHits.Load(), "cached source must use the new override")
	require.EqualValues(t, 5, vpnHits.Load())
}

func TestNestedOverrideDetourUsesParentScope(t *testing.T) {
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"},{"type":"test","tag":"upstream"}]}`)
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"subscriptions":[{"tag":"child","url":%q,"http_client":{},"override_dialer":{"detour":"upstream"}}],"outbounds":[{"type":"test","tag":"upstream"},{"type":"selector","tag":"g","import":[{"group":"child"}]}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"parent","url":%q,"http_client":{}}],"outbounds":[{"type":"test","tag":"upstream"}]}`, parent.URL))
	nested := getSubscription(t, ctx, "parent").current.children[0]
	require.Equal(t, memberTag("parent", "upstream"), *nested.options.OverrideDialer.Detour)
	require.Contains(t, nested.current.dependencies["outbound:"+memberTag(nested.Tag(), "node")], "outbound:"+memberTag("parent", "upstream"))
}

func TestOverrideDetourRejectsOwnImportAndSource(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer source.Close()
	for _, target := range []string{"upstream", "s"} {
		t.Run(target, func(t *testing.T) {
			ctx := testContext()
			config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"override_dialer":{"detour":%q}}],"outbounds":[{"type":"test","tag":"direct"},{"type":"selector","tag":"upstream","outbounds":["direct"],"import":[{"group":"s"}]}]}`, source.URL, target)
			opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
			require.NoError(t, err)
			instance, err := box.New(box.Options{Options: opts, Context: ctx})
			if target == "s" {
				require.ErrorContains(t, err, "is a source, not an outbound")
				return
			}
			require.NoError(t, err)
			defer instance.Close()
			require.ErrorContains(t, instance.Start(), "circular")
		})
	}
}

func TestStandaloneColdStartFailureWithEmptyFallback(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer source.Close()
	ctx := testContext()
	config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{}}],"outbounds":[{"type":"direct","tag":"direct"},{"type":"selector","tag":"g","import":[{"group":"s"}],"empty_fallback":"direct"}]}`, source.URL)
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	require.NoError(t, instance.Start())
	require.False(t, getSubscription(t, ctx, "s").Status().Ready)
	require.Contains(t, getSubscription(t, ctx, "s").Status().LastError, "subscription HTTP status: 503")
	require.Equal(t, []string{"direct"}, importGroup(t, ctx, "g").All())
}
