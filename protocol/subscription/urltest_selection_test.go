package subscription

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

func TestDashboardHistoryUpdatesSubscriptionURLTestSelection(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, `"mode":"urltest","test_url":"http://127.0.0.1:1","tolerance":20,`))
	s := getSubscription(t, ctx, "sub")
	// Stop scheduled probes so only the completed dashboard measurement drives
	// this test; the current generation remains alive and routable.
	s.pauseUpdates()
	first, second := memberTag("sub", "exit"), memberTag("sub", "relay")
	store := func(tag string, delay uint16) {
		s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: s.options.TestURL, Time: time.Now(), Delay: delay})
	}
	store(first, 10)
	store(second, 100)
	s.PerformUpdateCheck()
	require.Equal(t, first, s.Selected("tcp").Tag())
	store(first, 100)
	store(second, 10)
	s.PerformUpdateCheck()
	require.Equal(t, second, s.Selected("tcp").Tag())
	require.Equal(t, second, s.Selected("udp").Tag())
	store(first, 5)
	s.PerformUpdateCheck()
	require.Equal(t, second, s.Selected("tcp").Tag(), "retain the current member within tolerance")
	s.history.DeleteURLTestHistory(second)
	s.PerformUpdateCheck()
	require.Equal(t, first, s.Selected("tcp").Tag(), "a failed member must not retain selection over a healthy one")
}

func TestSubscriptionAutomaticHistoryUsesConfiguredURLAndFinalLeaf(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/subscription?scope=a", r.URL.String())
		w.WriteHeader(204)
	}))
	defer target.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"leaf"},{"type":"selector","tag":"middle","outbounds":["leaf"]}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, fmt.Sprintf(`"mode":"urltest","test_url":%q,`, target.URL+"/subscription?scope=a")))
	s := getSubscription(t, ctx, "sub")
	result, err := s.URLTest(ctx)
	require.NoError(t, err)
	leaf, middle := memberTag("sub", "leaf"), memberTag("sub", "middle")
	require.Contains(t, result, leaf)
	require.Equal(t, result[leaf], result[middle])
	h := s.history.LoadURLTestHistory(leaf, s.options.TestURL)
	require.NotNil(t, h)
	require.Equal(t, s.options.TestURL, h.URL)
	require.Equal(t, leaf, h.Outbound)
	require.Empty(t, s.history.URLs(middle))
	require.Empty(t, s.history.URLs("sub"))
}
