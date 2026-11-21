package subscription

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionBackgroundRecoveryRemovesFallbacks(t *testing.T) {
	var body atomic.Value
	body.Store("")
	var failures atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		content := body.Load().(string)
		if content == "" {
			failures.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, content)
	}))
	defer source.Close()
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer health.Close()
	cache := filepath.Join(t.TempDir(), "subscription.json")
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"cache_path":%q,"update_interval":"40ms"}],"outbounds":[
 {"type":"direct","tag":"direct"},
 {"type":"selector","tag":"filtered","import":[{"group":"s","include":"^HK-"}],"empty_fallback":"direct"},
 {"type":"urltest","tag":"auto","import":[{"group":"filtered"}],"url":%q,"interval":"1h","idle_timeout":"2h","empty_fallback":"direct"},
 {"type":"selector","tag":"top","import":[{"group":"auto"}],"empty_fallback":"direct"},
 {"type":"selector","tag":"no-match","import":[{"group":"s","include":"^JP-"}],"empty_fallback":"direct"},
 {"type":"selector","tag":"static","outbounds":["direct"],"import":[{"group":"s"}]},
 {"type":"selector","tag":"no-fallback","import":[{"group":"s"}]}]}`, source.URL, cache, health.URL))
	s := getSubscription(t, ctx, "s")
	require.False(t, s.Status().Ready)
	require.Zero(t, s.Status().Generation)
	require.True(t, s.Status().UpdatedAt.IsZero())
	_, err := os.Stat(cache)
	require.True(t, os.IsNotExist(err), "unavailable must not overwrite the cache with an empty document")
	for _, tag := range []string{"filtered", "auto", "top", "no-match"} {
		require.Equal(t, []string{"direct"}, importGroup(t, ctx, tag).All())
	}
	require.Empty(t, importGroup(t, ctx, "no-fallback").All())
	waitFor := func(want string) {
		t.Helper()
		require.Eventually(t, func() bool {
			if !s.Status().Ready || s.Status().LastError != "" {
				return false
			}
			for _, tag := range []string{"filtered", "auto", "top"} {
				g := importGroup(t, ctx, tag)
				members := g.All()
				if len(members) != 1 || members[0] != want || g.Selected("tcp") == nil || g.Selected("tcp").Tag() != want {
					return false
				}
			}
			return true
		}, 5*time.Second, 5*time.Millisecond)
	}
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-a","detour":"top"}]}`)
	require.Eventually(t, func() bool { return strings.Contains(s.Status().LastError, "circular") }, time.Second, 5*time.Millisecond)
	require.False(t, s.Status().Ready)
	require.Equal(t, []string{"direct"}, importGroup(t, ctx, "top").All(), "invalid background candidates must not replace fallbacks")
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-a"}]}`)
	waitFor(memberTag("s", "HK-a"))
	require.Equal(t, []string{"direct"}, importGroup(t, ctx, "no-match").All(), "filtered-empty groups still need their fallback")
	require.Equal(t, []string{"direct", memberTag("s", "HK-a")}, importGroup(t, ctx, "static").All(), "explicit static members must remain")
	_, found := service.FromContext[adapter.OutboundManager](ctx).Outbound("direct")
	require.True(t, found, "removing implicit group members must not delete the global outbound")
	conn, err := importGroup(t, ctx, "top").DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(health.URL, "http://")))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	body.Store("")
	require.Eventually(t, func() bool { return s.Status().LastError != "" }, time.Second, 5*time.Millisecond)
	generation := s.Status().Generation
	failed := failures.Load()
	require.Eventually(t, func() bool { return failures.Load() > failed+1 }, time.Second, 5*time.Millisecond)
	require.Equal(t, generation, s.Status().Generation)
	require.Equal(t, []string{memberTag("s", "HK-a")}, importGroup(t, ctx, "top").All(), "failed refresh retains real nodes, not fallbacks")
	body.Store(`{"outbounds":[]}`)
	waitFor("direct")
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-b"}]}`)
	waitFor(memberTag("s", "HK-b"))
}

func TestSubscriptionRetriesBeforeDailyUpdateAndResetsAfterRecovery(t *testing.T) {
	var online atomic.Bool
	var calls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if !online.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer source.Close()
	instance, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"update_interval":"24h"}],"outbounds":[{"type":"direct","tag":"direct"},{"type":"selector","tag":"g","import":[{"group":"s"}],"empty_fallback":"direct"}]}`, source.URL))
	s := getSubscription(t, ctx, "s")
	require.False(t, s.Status().Ready)
	online.Store(true)
	// No manual Refresh: recovery must happen on the failure retry schedule,
	// rather than waiting for the 24-hour normal update interval.
	require.Eventually(t, func() bool { return s.Status().Ready }, 8*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{memberTag("s", "node")}, importGroup(t, ctx, "g").All())
	require.Eventually(t, func() bool { return s.nextUpdateDelay() > 23*time.Hour }, time.Second, time.Millisecond)
	require.EqualValues(t, 2, calls.Load())
	started := time.Now()
	require.NoError(t, instance.Close())
	require.Less(t, time.Since(started), time.Second, "shutdown must cancel the pending timer")
}

func TestNestedSubscriptionRecoversInBackground(t *testing.T) {
	var online atomic.Bool
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !online.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"subscriptions":[{"tag":"child","url":%q,"http_client":{},"update_interval":"40ms"}],"outbounds":[{"type":"test","tag":"fallback"},{"type":"selector","tag":"choice","import":[{"group":"child"}],"empty_fallback":"fallback"}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"parent","url":%q,"http_client":{}}]}`, parent.URL))
	p := getSubscription(t, ctx, "parent")
	require.True(t, p.Status().Ready)
	childTag := memberTag("parent", "child")
	g := importGroup(t, ctx, memberTag("parent", "choice"))
	require.Equal(t, []string{memberTag("parent", "fallback")}, g.All())
	online.Store(true)
	require.Eventually(t, func() bool {
		members := g.All()
		return len(members) == 1 && members[0] == memberTag(childTag, "node")
	}, 5*time.Second, 5*time.Millisecond)
}

func TestInvalidSubscriptionIsNotHiddenByUnavailableBootstrap(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"a","detour":"b"},{"type":"test","tag":"b","detour":"a"}]}`)
	}))
	defer source.Close()
	// The primary download succeeds with an invalid chain. A broken fallback
	// HTTP transport must not turn that validation failure into startup success.
	ctx := testContext()
	config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"bootstrap_http_client":{"detour":"unavailable"}}],"outbounds":[{"type":"http","tag":"unavailable","server":"127.0.0.1","server_port":1}]}`, source.URL)
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	require.ErrorContains(t, instance.Start(), "circular")
}

func TestLegacySubscriptionRetryRemovesOwnFallback(t *testing.T) {
	for _, mode := range []string{"selector", "urltest"} {
		t.Run(mode, func(t *testing.T) {
			var online atomic.Bool
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(204)
				} else if !online.Load() {
					w.WriteHeader(503)
				} else {
					fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
				}
			}))
			defer source.Close()
			_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"direct"},{"type":"subscription","tag":"s","mode":%q,"url":%q,"http_client":{},"empty_fallback":"direct","update_interval":"40ms","test_url":%q},{"type":"selector","tag":"top","import":[{"group":"s"}]}]}`, mode, source.URL, source.URL+"/health"))
			s := getSubscription(t, ctx, "s")
			require.False(t, s.Status().Ready)
			require.Equal(t, []string{"direct"}, s.All())
			require.NotNil(t, s.Selected("tcp"))
			require.True(t, s.SelectOutbound("direct"))
			conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(source.URL, "http://")))
			require.NoError(t, err, "the fallback must be usable before the source recovers")
			defer conn.Close()
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			require.NoError(t, err)
			defer udp.Close()
			packet, err := s.ListenPacket(ctx, M.SocksaddrFromNet(udp.LocalAddr()))
			require.NoError(t, err)
			defer packet.Close()
			checkPacket := func() {
				t.Helper()
				_, err := packet.WriteTo([]byte("alive"), udp.LocalAddr())
				require.NoError(t, err)
				require.NoError(t, udp.SetReadDeadline(time.Now().Add(time.Second)))
				data := make([]byte, 16)
				n, _, err := udp.ReadFrom(data)
				require.NoError(t, err)
				require.Equal(t, "alive", string(data[:n]))
			}
			checkPacket()
			online.Store(true)
			require.Eventually(t, func() bool {
				members := importGroup(t, ctx, "top").All()
				return s.Status().Ready && len(members) == 1 && members[0] == memberTag("s", "node")
			}, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []string{memberTag("s", "node")}, s.All())
			checkPacket()
			_, err = fmt.Fprint(conn, "GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
			require.NoError(t, err)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			data := make([]byte, 128)
			n, err := conn.Read(data)
			require.NoError(t, err)
			require.Contains(t, string(data[:n]), "204", "recovery must retain existing fallback connections")
		})
	}
}

func TestSubscriptionDownloadTimeoutStartsEmptyAndRecovers(t *testing.T) {
	var online atomic.Bool
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !online.Load() {
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer source.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{},"download_timeout":"30ms","update_interval":"40ms"}],"outbounds":[{"type":"direct","tag":"direct"},{"type":"selector","tag":"g","import":[{"group":"s"}],"empty_fallback":"direct"}]}`, source.URL))
	s := getSubscription(t, ctx, "s")
	require.False(t, s.Status().Ready)
	require.Contains(t, s.Status().LastError, "deadline exceeded")
	online.Store(true)
	require.Eventually(t, func() bool { return s.Status().Ready }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{memberTag("s", "node")}, importGroup(t, ctx, "g").All())
}

func TestUnavailableSubscriptionDoesNotBlockHealthySource(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/failed" {
			w.WriteHeader(503)
		} else {
			fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
		}
	}))
	defer source.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"failed","url":%q,"http_client":{}},{"tag":"healthy","url":%q,"http_client":{}}],"outbounds":[{"type":"direct","tag":"direct"},{"type":"selector","tag":"g","import":[{"group":"failed"},{"group":"healthy"}],"empty_fallback":"direct"}]}`, source.URL+"/failed", source.URL+"/healthy"))
	require.False(t, getSubscription(t, ctx, "failed").Status().Ready)
	require.True(t, getSubscription(t, ctx, "healthy").Status().Ready)
	require.Equal(t, []string{memberTag("healthy", "node")}, importGroup(t, ctx, "g").All())
}

func TestSubscriptionStartupCancellationIsNotIgnored(t *testing.T) {
	requested := make(chan struct{}, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer source.Close()
	ctx, cancel := context.WithCancel(testContext())
	defer cancel()
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"s","url":%q,"http_client":{}}]}`, source.URL)))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	result := make(chan error, 1)
	go func() { result <- instance.Start() }()
	select {
	case <-requested:
	case <-time.After(3 * time.Second):
		t.Fatal("subscription request never started")
	}
	cancel()
	select {
	case err = <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not stop after cancellation")
	}
}
