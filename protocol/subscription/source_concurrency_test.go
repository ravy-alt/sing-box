package subscription

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandaloneConcurrentCrossProviderCycle(t *testing.T) {
	var first, second atomic.Value
	first.Store(`{"outbounds":[{"type":"test","tag":"node"}]}`)
	second.Store(first.Load())
	newSource := func(body *atomic.Value) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	}
	a, b := newSource(&first), newSource(&second)
	defer a.Close()
	defer b.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"a","url":%q,"http_client":{}},{"tag":"b","url":%q,"http_client":{}}],"outbounds":[{"type":"selector","tag":"ga","import":[{"group":"a"}]},{"type":"selector","tag":"gb","import":[{"group":"b"}]}]}`, a.URL, b.URL))
	first.Store(`{"outbounds":[{"type":"test","tag":"node","detour":"gb"}]}`)
	second.Store(`{"outbounds":[{"type":"test","tag":"node","detour":"ga"}]}`)
	results := make(chan error, 2)
	for _, tag := range []string{"a", "b"} {
		s := getSubscription(t, ctx, tag)
		go func() { results <- s.Refresh(ctx) }()
	}
	accepted := 0
	for range 2 {
		err := <-results
		if err == nil {
			accepted++
		} else {
			require.ErrorContains(t, err, "circular")
		}
	}
	require.Equal(t, 1, accepted)
}

func TestNestedOverrideUsesParentDNS(t *testing.T) {
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node","domain_resolver":"lost"}]}`)
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"hosts","tag":"private"}]},"subscriptions":[{"tag":"child","url":%q,"http_client":{},"override_dialer":{"domain_resolver":"private"}}],"outbounds":[{"type":"selector","tag":"g","import":[{"group":"child"}]}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"private"}]},"subscriptions":[{"tag":"s","url":%q,"http_client":{}}]}`, parent.URL))
	s := getSubscription(t, ctx, "s")
	nested := s.current.children[0]
	require.Equal(t, "subscription-dns/s/private", nested.options.OverrideDialer.DomainResolver.Server)
	require.Contains(t, nested.current.dependencies["outbound:"+memberTag(nested.Tag(), "node")], "dns_server:subscription-dns/s/private")
}
