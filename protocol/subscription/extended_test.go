package subscription

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
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

	"github.com/stretchr/testify/require"
)

func TestEndpointOnlySubscription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"endpoints":[{"type":"test","tag":"ep"}]}`)
	}))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	require.Equal(t, []string{memberTag("sub", "ep")}, s.All())
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "alive") }))
	defer target.Close()
	conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	require.NoError(t, err)
	defer conn.Close()
	old := s.current
	require.NoError(t, s.Refresh(ctx))
	require.False(t, old.closed)
	_, err = fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	body, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Contains(t, string(body), "alive")
	require.NoError(t, conn.Close())
	require.True(t, old.closed)
	member, found := service.FromContext[adapter.OutboundManager](ctx).Outbound(memberTag("sub", "ep"))
	require.True(t, found)
	require.True(t, member.(adapter.ProvidedOutbound).Available())
}

func TestNestedSubscriptionRefreshAndImport(t *testing.T) {
	var childBody atomic.Value
	childBody.Store(goodDocument)
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, childBody.Load()) }))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"selector","tag":"inner","import":[{"group":"child","include":"^exit$"}]},{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
	}))
	defer parent.Close()
	memberName := memberTag(memberTag("sub", "child"), "exit")
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"g","import":[{"group":%q,"include":"^exit$"}]},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, memberTag("sub", "child"), parent.URL)
	_, ctx := startBox(t, config)
	require.Equal(t, []string{memberName}, importGroup(t, ctx, "g").All())
	manager := service.FromContext[adapter.OutboundManager](ctx)
	group, _ := manager.Outbound("g")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	dest := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
	conn, err := group.DialContext(ctx, "tcp", dest)
	require.NoError(t, err)
	defer conn.Close()
	parentSubscription := getSubscription(t, ctx, "sub")
	innerConn, err := parentSubscription.DialContext(ctx, "tcp", dest)
	require.NoError(t, err)
	require.NoError(t, innerConn.Close())
	childRaw, _ := parentSubscription.current.outbounds.Outbound(memberTag("sub", "child"))
	childSubscription := childRaw.(*Subscription)
	childBody.Store(`{"outbounds":[{"type":"test","tag":"exit"}]}`)
	require.NoError(t, childSubscription.Refresh(context.Background()))
	require.NoError(t, parentSubscription.Refresh(ctx))
	newConn, err := group.DialContext(ctx, "tcp", dest)
	require.NoError(t, err)
	require.NoError(t, newConn.Close())
}

func TestNestedSubscriptionCycle(t *testing.T) {
	var url string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"again","url":%q,"http_client":{}}]}`, url+"#ignored-fragment")
	}))
	defer server.Close()
	url = server.URL
	ctx := testContext()
	options, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(configFor(url, "")))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: options, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	require.ErrorContains(t, instance.Start(), "circular nested subscription")
}

func TestSubscriptionDNSRuleSets(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"version":4,"rules":[{"domain":"remote.invalid"}]}`)
	}))
	defer remote.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"hosts","tag":"empty"},{"type":"hosts","tag":"private","predefined":{"inline.invalid":"127.0.0.1","remote.invalid":"127.0.0.1","host.invalid":"127.0.0.1"}}],"rules":[{"rule_set":["inline","remote","host-rules"],"server":"private"}],"final":"empty"},"route":{"rule_set":[{"tag":"inline","rules":[{"domain":"inline.invalid"}]},{"type":"remote","tag":"remote","format":"source","url":%q,"http_client":{}}]},"outbounds":[{"type":"test","tag":"a"}]}`, remote.URL)
	}))
	defer server.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"route":{"rule_set":[{"tag":"host-rules","rules":[{"domain":"host.invalid"}]}]},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, server.URL)
	_, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "sub")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	port := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")).Port
	for _, host := range []string{"inline.invalid", "remote.invalid", "host.invalid"} {
		conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddrHostPort(host, port))
		require.NoError(t, err, host)
		require.NoError(t, conn.Close())
	}
	_, found := service.FromContext[adapter.Router](ctx).RuleSet("inline")
	require.False(t, found, "subscription rule sets must not mutate the host")
	require.NoError(t, s.Refresh(ctx))
	validator := service.FromContext[adapter.DNSRuleSetUpdateValidator](ctx)
	// The host has no DNS rules of its own: only the imported DNS scope rejects
	// this metadata transition, which would require switching legacy DNS modes.
	require.Error(t, validator.ValidateRuleSetMetadataUpdate("host-rules", adapter.RuleSetMetadata{ContainsIPCIDRRule: true}))
	require.NoError(t, s.close())
	require.NoError(t, validator.ValidateRuleSetMetadataUpdate("host-rules", adapter.RuleSetMetadata{ContainsIPCIDRRule: true}))
}

func TestNestedResourcesOfflineCache(t *testing.T) {
	rules := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"version":4,"rules":[{"domain":"cached.invalid"}]}`)
	}))
	defer rules.Close()
	var tag atomic.Value
	tag.Store("first")
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"route":{"rule_set":[{"type":"remote","tag":"remote","format":"source","url":%q,"http_client":{}}]},"dns":{"servers":[{"type":"hosts","tag":"hosts","predefined":{"cached.invalid":"127.0.0.1"}}],"rules":[{"rule_set":"remote","server":"hosts"}]},"outbounds":[{"type":"test","tag":%q}]}`, rules.URL, tag.Load())
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
	}))
	defer parent.Close()
	config := configFor(parent.URL, fmt.Sprintf(`"cache_path":%q,`, filepath.Join(t.TempDir(), "cache.json")))
	instance, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "sub")
	raw, _ := s.current.outbounds.Outbound(memberTag("sub", "child"))
	tag.Store("second")
	require.NoError(t, raw.(*Subscription).Refresh(ctx))
	require.NoError(t, instance.Close())
	parent.Close()
	child.Close()
	rules.Close()
	_, restoredContext := startBox(t, config)
	restored := getSubscription(t, restoredContext, "sub")
	raw, _ = restored.current.outbounds.Outbound(memberTag("sub", "child"))
	require.Equal(t, []string{memberTag(memberTag("sub", "child"), "second")}, raw.(*Subscription).All())
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	conn, err := restored.DialContext(restoredContext, "tcp", M.ParseSocksaddrHostPort("cached.invalid", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")).Port))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestSubscriptionCertificateStoreForRemoteRuleSet(t *testing.T) {
	rules := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"version":4,"rules":[{"domain":"private.invalid"}]}`)
	}))
	defer rules.Close()
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rules.Certificate().Raw})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"certificate":{"store":"none","certificate":[%q]},"route":{"rule_set":[{"type":"remote","tag":"r","format":"source","url":%q,"http_client":{}}]},"outbounds":[{"type":"test","tag":"node"}]}`, certificate, rules.URL)
	}))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	require.True(t, s.Status().Ready)
	require.NoError(t, s.Refresh(ctx))
}

func TestSubscriptionNetworkNamespaceScope(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("network namespaces require Linux")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"network_namespaces":[{"tag":"local","path":"/proc/self/ns/net"}],"outbounds":[{"type":"test","tag":"node","netns":"local"}]}`)
	}))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	manager := service.FromContext[adapter.NetworkNamespaceManager](s.current.scope.Context())
	require.Equal(t, "/proc/self/ns/net", manager.ResolvePath("subscription-network_namespace/sub/local"))
	require.NoError(t, s.Refresh(ctx))
}

func TestImportedGroupControlAndSelectionCache(t *testing.T) {
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"selector","tag":"pick","outbounds":["a","b"]},{"type":"test","tag":"a"},{"type":"test","tag":"b"},{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
	}))
	defer parent.Close()
	config := configFor(parent.URL, fmt.Sprintf(`"cache_path":%q,`, filepath.Join(t.TempDir(), "cache")))
	instance, ctx := startBox(t, config)
	manager := service.FromContext[adapter.OutboundManager](ctx)
	member, found := manager.Outbound(memberTag("sub", "pick"))
	require.True(t, found)
	_, routingGroup := member.(adapter.OutboundGroup)
	require.False(t, routingGroup, "routing must retain the generation lease")
	control, found := adapter.OutboundGroupFor(member)
	require.True(t, found)
	require.True(t, control.(adapter.SelectableOutboundGroup).SelectOutbound(memberTag("sub", "b")))
	require.Equal(t, memberTag("sub", "b"), control.Selected("tcp").Tag())
	childMember, found := manager.Outbound(memberTag("sub", "child"))
	require.True(t, found)
	childControl, found := adapter.SubscriptionFor(childMember)
	require.True(t, found)
	generation := childControl.Status().Generation
	require.NoError(t, childControl.Refresh(ctx))
	require.Greater(t, childControl.Status().Generation, generation)
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.Equal(t, memberTag("sub", "b"), control.Selected("tcp").Tag())
	require.NoError(t, childControl.Refresh(ctx), "retained control resolves the new child")
	require.NoError(t, instance.Close())
	parent.Close()
	child.Close()
	_, restored := startBox(t, config)
	member, found = service.FromContext[adapter.OutboundManager](restored).Outbound(memberTag("sub", "pick"))
	require.True(t, found)
	control, found = adapter.OutboundGroupFor(member)
	require.True(t, found)
	require.Equal(t, memberTag("sub", "b"), control.Selected("tcp").Tag())
}

func TestHostDefaultAndURLTestImportedMember(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer target.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer remote.Close()
	name := memberTag("sub", "exit")
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"urltest","tag":"health","import":[{"group":"sub","include":"^exit$"}],"url":%q},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}],"route":{"final":%q}}`, target.URL, remote.URL, name)
	_, ctx := startBox(t, config)
	manager := service.FromContext[adapter.OutboundManager](ctx)
	require.Equal(t, name, manager.Default().Tag())
	conn, err := manager.Default().DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	group, found := manager.Outbound("health")
	require.True(t, found)
	// The built-in URLTest may already be running its initial background test.
	require.Eventually(t, func() bool {
		results, err := group.(adapter.URLTestGroup).URLTest(ctx)
		if err != nil {
			return false
		}
		_, found := results[name]
		return found
	}, time.Second, 10*time.Millisecond)
}
