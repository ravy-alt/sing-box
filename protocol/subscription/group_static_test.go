package subscription

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestStaticGroupOutboundsCheckedBeforeDownload(t *testing.T) {
	var downloads atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
	}))
	defer remote.Close()
	for _, kind := range []string{"selector", "urltest"} {
		for _, ref := range []string{"missing", "subscription/sub/node", "subscription/sub/inner", memberTag(memberTag("sub", "child"), "node")} {
			t.Run(kind+"/"+ref, func(t *testing.T) {
				ctx := testContext()
				config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":%q,"tag":"pick","outbounds":[%q],"import":[{"group":"sub"}]},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, kind, ref, remote.URL)
				opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
				require.NoError(t, err)
				instance, err := box.New(box.Options{Options: opts, Context: ctx})
				require.ErrorContains(t, err, "outbound[0] (pick).outbounds[0]: "+ref)
				require.ErrorContains(t, err, "use import")
				require.Nil(t, instance)
			})
		}
	}
	require.Zero(t, downloads.Load(), "configuration checks must not load subscriptions")
}

func TestStaticGroupOutboundsLocalDeclarations(t *testing.T) {
	// Validate by declaration, not spelling or construction order. Native implicit
	// endpoint/outbound tags and locally declared groups are valid static members.
	document := `{"log":{"disabled":true},"endpoints":[{"type":"test"}],"outbounds":[
 {"type":"selector","tag":"pick","outbounds":["0","2","subscription/example/node","other"]},
 {"type":"urltest","tag":"other","outbounds":["0","2"],"url":"http://127.0.0.1:1"},
 {"type":"test"},{"type":"test","tag":"subscription/example/node"}]}`
	_, ctx := startBox(t, document)
	require.Equal(t, []string{"0", "2", "subscription/example/node", "other"}, importGroup(t, ctx, "pick").All())
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, document) }))
	defer remote.Close()
	_, ctx = startBox(t, configFor(remote.URL, `"outbounds":["pick"],`))
	require.Equal(t, []string{memberTag("sub", "0"), memberTag("sub", "2"), memberTag("sub", "subscription/example/node"), memberTag("sub", "other")}, importGroup(t, ctx, memberTag("sub", "pick")).All())
}

func TestStaticGroupOutboundsRefreshRollback(t *testing.T) {
	for _, kind := range []string{"selector", "urltest"} {
		t.Run(kind, func(t *testing.T) {
			var body atomic.Value
			probeOptions := ""
			if kind == "urltest" {
				probeOptions = `,"url":"http://127.0.0.1:1"`
			}
			document := func(node string) string {
				return fmt.Sprintf(`{"outbounds":[{"type":%q,"tag":"pick","outbounds":[%q]%s},{"type":"test","tag":%q}]}`, kind, node, probeOptions, node)
			}
			body.Store(document("old"))
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/source" {
					fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
				} else {
					fmt.Fprint(w, body.Load())
				}
			}))
			defer remote.Close()
			_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"host-node"},{"type":"subscription","tag":"source","url":%q,"http_client":{}},{"type":"subscription","tag":"consumer","url":%q,"http_client":{},"outbounds":["pick"]}]}`, remote.URL+"/source", remote.URL+"/consumer"))
			s := getSubscription(t, ctx, "consumer")
			generation := s.Status().Generation
			old := s.current
			for _, ref := range []string{"old", "host-node", "subscription/source/node", "subscription/source/inner", "subscription/child/node"} {
				// Include a real sibling provider and a local child declaration to
				// ensure neither provider lookup nor parent fallback admits a member.
				body.Store(fmt.Sprintf(`{"outbounds":[{"type":%q,"tag":"pick","outbounds":[%q]},{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, kind, ref, remote.URL+"/source"))
				require.ErrorContains(t, s.Refresh(ctx), "is not declared in this configuration document", ref)
				require.Equal(t, generation, s.Status().Generation)
				require.Same(t, old, s.current)
				require.Equal(t, []string{memberTag("consumer", "old")}, importGroup(t, ctx, memberTag("consumer", "pick")).All())
			}
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
			defer target.Close()
			conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
			require.NoError(t, err, "rejected documents must leave the old group usable")
			require.NoError(t, conn.Close())
			body.Store(document("new"))
			require.NoError(t, s.Refresh(ctx), "a group and its declarations may change together")
			require.Equal(t, []string{memberTag("consumer", "new")}, importGroup(t, ctx, memberTag("consumer", "pick")).All())
		})
	}
}

func TestStaticSubscriptionWhitelistIsDocumentLocal(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, ""))
	for _, name := range []string{"subscription/sub/exit", "subscription/child/exit"} {
		_, err := parseDocument(ctx, "other", []byte(fmt.Sprintf(`{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, remote.URL)), []string{name})
		require.ErrorContains(t, err, "subscription member not found: "+name)
	}
	_, err := parseDocument(ctx, "other", []byte(fmt.Sprintf(`{"outbounds":[{"type":"selector","tag":"pick","outbounds":["child"]},{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, remote.URL)), []string{"pick"})
	require.NoError(t, err, "a declared child group itself is a valid static reference")
}
