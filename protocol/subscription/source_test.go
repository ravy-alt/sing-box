package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestStandaloneSubscriptionImportsAndNoDirectRouting(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"香港"}]}`)
	}))
	defer remote.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"机场","url":%q,"http_client":{}}],"outbounds":[{"type":"selector","tag":"代理","import":[{"group":"机场"}]}]}`, remote.URL)
	_, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "机场")
	require.True(t, adapter.IsSubscriptionSource(s))
	manager := service.FromContext[adapter.OutboundManager](ctx)
	group, found := manager.Outbound("代理")
	require.True(t, found)
	require.Equal(t, []string{memberTag("机场", "香港")}, group.(adapter.OutboundGroup).All())
	require.Equal(t, "代理", manager.Default().Tag())
	_, err := s.DialContext(ctx, "tcp", M.ParseSocksaddr("127.0.0.1:9"))
	require.ErrorContains(t, err, "not dialable")
	_, err = s.ListenPacket(ctx, M.ParseSocksaddr("127.0.0.1:9"))
	require.ErrorContains(t, err, "not dialable")
	for _, extra := range []string{
		`"outbounds":[{"type":"selector","tag":"g","outbounds":["机场"]}]`,
		`"outbounds":[{"type":"test","tag":"n","detour":"机场"}]`,
		`"route":{"final":"机场"}`,
		`"dns":{"servers":[{"type":"udp","tag":"d","server":"127.0.0.1","detour":"机场"}]}`,
		`"outbounds":[{"type":"selector","tag":"g","empty_fallback":"机场"}]`,
		`"http_clients":[{"tag":"h","detour":"机场"}]`,
	} {
		ctx := testContext()
		options, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(fmt.Sprintf(`{"subscriptions":[{"tag":"机场","url":%q}],%s}`, remote.URL, extra)))
		require.NoError(t, err)
		_, err = box.New(box.Options{Options: options, Context: ctx})
		require.ErrorContains(t, err, "is a source")
	}
	// A downloaded document cannot detour through a host source either.
	_, err = parseDocument(ctx, "x", []byte(`{"outbounds":[{"type":"test","tag":"n","detour":"机场"}]}`), nil)
	require.ErrorContains(t, err, "missing outbound reference")
}

func TestNestedStandaloneSourceAndImports(t *testing.T) {
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"节点"}]}`)
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"subscriptions":[{"tag":"child","url":%q,"http_client":{}}],"outbounds":[{"type":"selector","tag":"choice","import":[{"group":"child"}]}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"parent","url":%q,"http_client":{}}],"outbounds":[{"type":"selector","tag":"host","import":[{"group":"parent"}]}]}`, parent.URL))
	s := getSubscription(t, ctx, "parent")
	require.Equal(t, []string{memberTag("parent", "choice")}, s.All())
	member, found := s.ProviderOutbound(memberTag("parent", "child"))
	require.True(t, found)
	require.True(t, adapter.IsSubscriptionSource(member))
	provider, ok := adapter.SubscriptionFor(member)
	require.True(t, ok)
	require.Equal(t, []string{memberTag(memberTag("parent", "child"), "节点")}, provider.All())
	require.NoError(t, provider.Refresh(context.Background()))
}

func TestStandaloneSourceConfigurationRoundTrip(t *testing.T) {
	ctx := testContext()
	input := []byte(`{"subscriptions":[{"tag":"s","url":"https://example.invalid","http_client":{},"bootstrap_http_client":{},"override_dialer":{"domain_resolver":{"server":"private","strategy":"ipv4_only"},"tcp_fast_open":false,"routing_mark":0,"bind_interface":""}}]}`)
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, input)
	require.NoError(t, err)
	encoded, err := json.MarshalContext(ctx, opts)
	require.NoError(t, err)
	roundtrip, err := json.UnmarshalExtendedContext[option.Options](ctx, encoded)
	require.NoError(t, err)
	require.Equal(t, opts.Subscriptions, roundtrip.Subscriptions)
	require.NotNil(t, roundtrip.Subscriptions[0].HTTPClient)
	_, err = option.ExpandSubscriptions(option.Options{Subscriptions: []option.SubscriptionOptions{{Tag: "s"}, {Tag: "s"}}})
	require.ErrorContains(t, err, "duplicate")
}
