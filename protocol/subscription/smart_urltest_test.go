package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestSmartURLTestImportsRefreshFallbackAndNestedControls(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	var document atomic.Value
	document.Store(`{"outbounds":[]}`)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, document.Load()) }))
	defer source.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"source","url":%q,"http_client":{}}],"outbounds":[{"type":"direct","tag":"fallback"},{"type":"smart-urltest","tag":"smart","url":%q,"import":[{"group":"source","include":"^node$"}],"empty_fallback":"fallback"}]}`, source.URL, target.URL)
	_, ctx := startBox(t, config)
	manager := service.FromContext[adapter.OutboundManager](ctx)
	raw, _ := manager.Outbound("smart")
	smart := raw.(adapter.SmartURLTestGroup)
	require.Equal(t, []string{"fallback"}, smart.All())
	update := func(name string) {
		document.Store(fmt.Sprintf(`{"outbounds":[{"type":"direct","tag":%q},{"type":"smart-urltest","tag":"inner","outbounds":[%q],"url":%q}]}`, name, name, target.URL))
		require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
	}
	update("node")
	require.Equal(t, []string{memberTag("source", "node")}, smart.All())
	node, found := manager.Outbound(memberTag("source", "inner"))
	require.True(t, found)
	control, found := adapter.OutboundGroupFor(node)
	require.True(t, found)
	inner, ok := control.(adapter.SmartURLTestGroup)
	require.True(t, ok)
	require.Equal(t, target.URL, inner.TestURL())
	result, err := inner.URLTest(context.Background())
	require.NoError(t, err)
	require.Contains(t, result, memberTag("source", "node"))
	trace := adapter.NewOutboundTrace([]adapter.Outbound{raw})
	dst := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
	dialCtx := adapter.WithContext(ctx, &adapter.InboundContext{Destination: dst, OutboundTrace: trace})
	conn, err := raw.DialContext(dialCtx, "tcp", dst)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, memberTag("source", "node"), trace.Chain()[len(trace.Chain())-1].Tag())
	document.Store(`{"outbounds":[]}`)
	require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
	require.Equal(t, []string{"fallback"}, smart.All())
	update("node")
	require.Equal(t, []string{memberTag("source", "node")}, smart.All())
	// A retained control handle resolves the fresh generation, including the
	// new smart group's own rewritten member tags and configured probe URL.
	inner.ClearCache()
	result, err = inner.URLTest(context.Background())
	require.NoError(t, err)
	require.Contains(t, result, memberTag("source", "node"))
}
