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
		s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: delay})
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
