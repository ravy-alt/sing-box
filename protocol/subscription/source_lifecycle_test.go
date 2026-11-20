package subscription

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestStandaloneSourceDrainAndOverrideCache(t *testing.T) {
	var body atomic.Value
	body.Store(goodDocument)
	var modified atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Subscription-Userinfo", "upload=11; download=22; total=1000; expire=2000000000")
		if modified.Load() {
			w.WriteHeader(304)
			return
		}
		fmt.Fprint(w, body.Load())
	}))
	defer remote.Close()
	cache := filepath.Join(t.TempDir(), "source.json")
	config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"private"}]},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"cache_path":%q,"override_dialer":{"domain_resolver":"private"}}],"outbounds":[{"type":"selector","tag":"g","import":[{"group":"s","include":"^exit$"}],"empty_fallback":"direct"},{"type":"direct","tag":"direct"}]}`, remote.URL, cache)
	instance, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "s")
	require.EqualValues(t, 22, s.Status().SubscriptionInfo.Download)
	status := s.Status()
	status.SubscriptionInfo.Download = 9000
	require.EqualValues(t, 22, s.Status().SubscriptionInfo.Download, "status must not expose mutable provider state")
	old := s.current
	oldNode, _ := old.outbounds.Outbound(memberTag("s", "exit"))
	oldImplementation := oldNode.(*testOutbound)
	group, _ := service.FromContext[adapter.OutboundManager](ctx).Outbound("g")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "alive") }))
	defer target.Close()
	destination := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
	first, err := group.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	defer first.Close()
	second, err := group.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	defer second.Close()
	body.Store(`{"outbounds":[{"type":"test","tag":"exit"},{"type":"test","tag":"new"}]}`)
	require.NoError(t, s.Refresh(ctx))
	require.NotSame(t, old, s.current)
	require.False(t, oldImplementation.closed.Load())
	newConnection, err := group.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	defer newConnection.Close()
	s.current.mu.Lock()
	refs := s.current.refs
	s.current.mu.Unlock()
	require.Positive(t, refs, "new connection must lease the current generation")
	require.NoError(t, first.Close())
	require.False(t, oldImplementation.closed.Load(), "one remaining old connection still owns the generation")
	_, err = fmt.Fprintf(second, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, second.SetReadDeadline(time.Now().Add(time.Second)))
	response, err := io.ReadAll(second)
	require.NoError(t, err)
	require.Contains(t, string(response), "alive")
	require.NoError(t, second.Close())
	require.Eventually(t, oldImplementation.closed.Load, time.Second, time.Millisecond)
	require.NoError(t, newConnection.Close())
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, []string{"direct"}, group.(adapter.OutboundGroup).All())
	body.Store(goodDocument)
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, []string{memberTag("s", "exit")}, group.(adapter.OutboundGroup).All())
	modified.Store(true)
	generation := s.Status().Generation
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, generation, s.Status().Generation, "304 must not replace the generation")
	require.NoError(t, instance.Close())
	// The cache contains raw source data: apply this process's new resolver
	// rather than reviving the previous process's transformed node options.
	config = strings.ReplaceAll(config, `"private"`, `"new-private"`)
	_, restoredCtx := startBox(t, config)
	restored := getSubscription(t, restoredCtx, "s")
	require.EqualValues(t, 22, restored.Status().SubscriptionInfo.Download)
	require.Contains(t, restored.current.dependencies["outbound:"+memberTag("s", "exit")], "dns_server:new-private")
}
