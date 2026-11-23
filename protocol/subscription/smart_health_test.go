package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func nestedSmartFixture(t *testing.T) (context.Context, adapter.Outbound, string) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"direct","tag":"leaf"},{"type":"smart-urltest","tag":"inner","outbounds":["leaf"],"url":%q,"interval":"24h","udp_test":{"server":"127.0.0.1","server_port":1,"domain":"custom.example","timeout":"1ms"}}]}`, target.URL)
	}))
	t.Cleanup(source.Close)
	_, ctx := startBox(t, configFor(source.URL, ""))
	ob, found := service.FromContext[adapter.OutboundManager](ctx).Outbound(memberTag("sub", "inner"))
	require.True(t, found)
	return ctx, ob, target.URL
}
func TestNestedSmartUDPSettings(t *testing.T) {
	_, ob, _ := nestedSmartFixture(t)
	got := urltest.OutboundUDPTestOptions(ob)
	require.Equal(t, "127.0.0.1", got.Server, "panel must receive subscription Smart group's configured UDP server")
	require.EqualValues(t, 1, got.ServerPort)
	require.Equal(t, "custom.example.", got.Domain)
	require.Equal(t, time.Millisecond, time.Duration(got.Timeout))
}
func TestNestedSmartHistoryVisible(t *testing.T) {
	ctx, ob, link := nestedSmartFixture(t)
	control, _ := adapter.OutboundGroupFor(ob)
	result, err := control.(adapter.URLTestGroup).URLTest(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, result)
	leaf := urltest.SelectedLeaf(ob)
	require.NotNil(t, leaf)
	public := service.PtrFromContext[urltest.HistoryStorage](ctx)
	require.NotNil(t, public.LoadURLTestHistory(leaf.Tag(), link), "automatic nested group measurement must be visible in common API history")
	settings := urltest.OutboundUDPTestOptions(ob)
	require.NotNil(t, public.LoadUDPHistory(leaf.Tag(), settings), "internal UDP observations must also reach the API")
	public.StoreURLTestHistory(leaf.Tag(), &adapter.URLTestHistory{URL: link, Time: time.Now().Add(time.Hour), Delay: 123})
	s := getSubscription(t, ctx, "sub")
	s.mu.Lock()
	current := s.current
	s.mu.Unlock()
	require.EqualValues(t, 123, current.history.LoadURLTestHistory(leaf.Tag(), link).Delay, "API observations must also reach the inner group")
}
