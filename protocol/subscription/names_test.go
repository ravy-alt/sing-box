package subscription

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

func TestPublicNamesPreserveIdentityAcrossRefreshAndRestore(t *testing.T) {
	var first, second atomic.Value
	original := `{"outbounds":[{"type":"test","tag":"香港"},{"type":"test","tag":"香港-1"},{"type":"test","tag":"東京 / 100%+?#"}]}`
	first.Store(original)
	second.Store(`{"outbounds":[{"type":"test","tag":"香港"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			fmt.Fprint(w, first.Load())
		} else {
			fmt.Fprint(w, second.Load())
		}
	}))
	defer remote.Close()
	cache := t.TempDir()
	a := fmt.Sprintf(`{"type":"subscription","tag":"a","url":%q,"http_client":{},"cache_path":%q}`, remote.URL+"/a", filepath.Join(cache, "a.json"))
	b := fmt.Sprintf(`{"type":"subscription","tag":"b","url":%q,"http_client":{},"cache_path":%q}`, remote.URL+"/b", filepath.Join(cache, "b.json"))
	configuration := func(providers string) string {
		return fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"香港"},%s,{"type":"selector","tag":"pick","import":[{"group":"a"},{"group":"b"}]}]}`, providers)
	}
	instance, ctx := startBox(t, configuration(a+","+b))
	names := adapter.OutboundNamesFromContext(ctx)
	want := map[string]string{
		memberTag("a", "香港"):           "香港-2",
		memberTag("a", "香港-1"):         "香港-1",
		memberTag("a", "東京 / 100%+?#"): "東京 / 100%+?#",
		memberTag("b", "香港"):           "香港-3",
	}
	for tag, name := range want {
		require.Equal(t, name, names.Name(tag))
		require.Equal(t, tag, names.Tag(name))
	}
	require.Equal(t, "香港", names.Name("香港"))
	pick := importGroup(t, ctx, "pick").(adapter.SelectableOutboundGroup)
	require.True(t, pick.SelectOutbound(names.Tag("香港-3")))
	first.Store(`{"outbounds":[]}`)
	require.NoError(t, getSubscription(t, ctx, "a").Refresh(ctx))
	require.Equal(t, memberTag("b", "香港"), pick.Selected("tcp").Tag())
	require.Equal(t, "香港-3", names.Name(memberTag("b", "香港")))
	second.Store(`{"outbounds":[{"type":"test","tag":"香港"},{"type":"test","tag":"香港-2"}]}`)
	require.NoError(t, getSubscription(t, ctx, "b").Refresh(ctx))
	require.Equal(t, "香港-2-1", names.Name(memberTag("b", "香港-2")))
	first.Store(original)
	require.NoError(t, getSubscription(t, ctx, "a").Refresh(ctx))
	for tag, name := range want {
		require.Equal(t, name, names.Name(tag))
	}
	require.NoError(t, instance.Close())
	// Reverse provider startup order. Cached names retain their ownership.
	first.Store(`invalid`)
	second.Store(`invalid`)
	_, restored := startBox(t, configuration(b+","+a))
	for tag, name := range want {
		require.Equal(t, name, adapter.OutboundDisplayName(restored, tag))
	}
	require.Equal(t, "香港-2-1", adapter.OutboundDisplayName(restored, memberTag("b", "香港-2")))
}

func TestFailedParentDoesNotReserveChildPublicNames(t *testing.T) {
	var childCalls atomic.Int32
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		childCalls.Add(1)
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"新节点"}]}`)
	}))
	defer child.Close()
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"old"}]}`)
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, parent.URL))
	s := getSubscription(t, ctx, "sub")
	before := adapter.OutboundNamesFromContext(ctx).Snapshot()
	// The child loads before the parent's missing imported group is rejected.
	body.Store(fmt.Sprintf(`{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}},{"type":"selector","tag":"bad","import":[{"group":"subscription/child/missing"}]}]}`, child.URL))
	require.Error(t, s.Refresh(ctx))
	require.Positive(t, childCalls.Load())
	require.Equal(t, before, adapter.OutboundNamesFromContext(ctx).Snapshot())
	body.Store(fmt.Sprintf(`{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL))
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, "child", adapter.OutboundDisplayName(ctx, memberTag("sub", "child")))
	require.Equal(t, "新节点", adapter.OutboundDisplayName(ctx, memberTag(memberTag("sub", "child"), "新节点")))
}
