package subscription

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestEmptyFallbackDynamicMembership(t *testing.T) {
	for _, kind := range []string{"selector", "urltest", "subscription", "subscription-urltest"} {
		t.Run(kind, func(t *testing.T) {
			var body atomic.Value
			body.Store(`{"outbounds":[]}`)
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/empty" {
					fmt.Fprint(w, `{"outbounds":[]}`)
				} else {
					fmt.Fprint(w, body.Load())
				}
			}))
			defer remote.Close()
			health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
			defer health.Close()
			extra := ""
			groupType := kind
			if strings.HasPrefix(kind, "subscription") {
				groupType = "subscription"
				extra = fmt.Sprintf(`,"url":%q,"http_client":{},"test_url":%q`, remote.URL+"/empty", health.URL)
				if kind == "subscription-urltest" {
					extra += `,"mode":"urltest"`
				}
			} else if kind == "urltest" {
				extra = fmt.Sprintf(`,"url":%q`, health.URL)
			}
			_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[
 {"type":"direct","tag":"fallback"},
 {"type":"subscription","tag":"source","url":%q,"http_client":{}},
 {"type":%q,"tag":"pick","import":[{"group":"source","include":"^HK-","exclude":"expired","types":"test"}],"empty_fallback":"fallback"%s},
 {"type":"selector","tag":"top","import":[{"group":"pick"}]}]}`, remote.URL, groupType, extra))
			pick := importGroup(t, ctx, "pick")
			assertMembers := func(want ...string) {
				t.Helper()
				for _, tag := range []string{"pick", "top"} {
					require.Equal(t, want, importGroup(t, ctx, tag).All())
				}
				if probe, ok := pick.(adapter.URLTestGroup); ok {
					var result map[string]uint16
					require.Eventually(t, func() bool {
						var err error
						result, err = probe.URLTest(ctx)
						return err == nil && len(result) == len(want)
					}, 5*time.Second, 10*time.Millisecond)
					for _, tag := range want {
						require.Contains(t, result, tag)
					}
				}
				require.Contains(t, want, pick.Selected("tcp").Tag())
				conn, err := pick.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(health.URL, "http://")))
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			}
			assertMembers("fallback")
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			require.NoError(t, err)
			defer udp.Close()
			packet, err := pick.ListenPacket(interrupt.ContextWithIsExternalConnection(ctx, true), M.SocksaddrFromNet(udp.LocalAddr()))
			require.NoError(t, err)
			defer packet.Close()
			for _, state := range []struct{ document, want string }{
				{`{"outbounds":[{"type":"test","tag":"HK-a"}]}`, "subscription/source/HK-a"},
				{`{"outbounds":[{"type":"test","tag":"JP-a"},{"type":"test","tag":"HK-expired"},{"type":"direct","tag":"HK-direct"}]}`, "fallback"},
				{`{"outbounds":[{"type":"test","tag":"HK-b"}]}`, "subscription/source/HK-b"},
				{`{"outbounds":[]}`, "fallback"},
			} {
				body.Store(state.document)
				require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
				assertMembers(state.want)
				_, err = packet.WriteTo([]byte("alive"), udp.LocalAddr())
				require.NoError(t, err)
				require.NoError(t, udp.SetReadDeadline(time.Now().Add(5*time.Second)))
				data := make([]byte, 16)
				n, _, err := udp.ReadFrom(data)
				require.NoError(t, err)
				require.Equal(t, "alive", string(data[:n]))
			}
		})
	}
}

func TestEmptyFallbackStaticValidation(t *testing.T) {
	for _, kind := range []string{"selector", "urltest", "subscription"} {
		for _, target := range []string{"missing", "subscription/source/node", "pick"} {
			t.Run(kind+"/"+target, func(t *testing.T) {
				ctx := testContext()
				extra := ""
				if kind == "subscription" {
					extra = `,"url":"http://127.0.0.1:1","http_client":{}`
				}
				opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(fmt.Sprintf(`{"outbounds":[{"type":%q,"tag":"pick","empty_fallback":%q%s}]}`, kind, target, extra)))
				require.NoError(t, err)
				instance, err := box.New(box.Options{Options: opts, Context: ctx})
				require.ErrorContains(t, err, "empty_fallback")
				require.Nil(t, instance)
			})
		}
	}
	_, ctx := startBox(t, `{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"direct"},{"type":"selector","tag":"only","empty_fallback":"direct"},{"type":"selector","tag":"nonempty","outbounds":["only"],"empty_fallback":"direct"}]}`)
	require.Equal(t, []string{"direct"}, importGroup(t, ctx, "only").All())
	require.Equal(t, []string{"only"}, importGroup(t, ctx, "nonempty").All())
}

func TestEmptyFallbackNestedNamespaceAndRollback(t *testing.T) {
	for _, kind := range []string{"selector", "urltest", "subscription"} {
		t.Run(kind, func(t *testing.T) {
			child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"outbounds":[]}`) }))
			defer child.Close()
			extra := ""
			if kind == "subscription" {
				extra = fmt.Sprintf(`,"url":%q,"http_client":{}`, child.URL)
			}
			var body atomic.Value
			document := func(node string) string {
				return fmt.Sprintf(`{"outbounds":[%s{"type":%q,"tag":"pick","empty_fallback":"fallback"%s}]}`, node, kind, extra)
			}
			body.Store(document(`{"type":"test","tag":"fallback"},`))
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
			defer remote.Close()
			_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"fallback"},{"type":"subscription","tag":"source","url":%q,"http_client":{}}]}`, remote.URL))
			g := importGroup(t, ctx, memberTag("source", "pick"))
			require.Equal(t, []string{memberTag("source", "fallback")}, g.All())
			s := getSubscription(t, ctx, "source")
			generation := s.Status().Generation
			body.Store(document(""))
			require.ErrorContains(t, s.Refresh(ctx), "empty_fallback")
			body.Store(document(`{"type":"test","tag":"fallback","detour":"pick"},`))
			require.ErrorContains(t, s.Refresh(ctx), "circular")
			require.Equal(t, generation, s.Status().Generation)
			require.Equal(t, []string{memberTag("source", "fallback")}, g.All())
		})
	}
}

func TestEmptyFallbackDNSCycleEvenWhenInactive(t *testing.T) {
	for _, kind := range []string{"selector", "urltest", "subscription"} {
		t.Run(kind, func(t *testing.T) {
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
			}))
			defer remote.Close()
			extra := `,"outbounds":["source"]`
			if kind == "subscription" {
				extra = fmt.Sprintf(`,"url":%q,"http_client":{}`, remote.URL)
			}
			ctx := testContext()
			opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"udp","tag":"dns","server":"1.1.1.1","detour":"pick"}]},"outbounds":[
 {"type":"subscription","tag":"source","url":%q,"http_client":{}},
 {"type":"http","tag":"fallback","server":"proxy.invalid","server_port":8080,"domain_resolver":"dns"},
 {"type":%q,"tag":"pick","empty_fallback":"fallback"%s}]}`, remote.URL, kind, extra)))
			require.NoError(t, err)
			instance, err := box.New(box.Options{Options: opts, Context: ctx})
			require.NoError(t, err)
			defer instance.Close()
			require.ErrorContains(t, instance.Start(), "circular")
		})
	}
}
