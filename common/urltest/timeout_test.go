package urltest

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type timeoutDialer struct{ N.Dialer }

func (timeoutDialer) DialContext(ctx context.Context, network string, address M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address.String())
}

func TestExplicitHTTPDeadlineCanExceedAutomaticProbeTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("exercises a deadline beyond the 15-second automatic probe timeout")
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(16 * time.Second):
			w.WriteHeader(204)
		case <-r.Context().Done():
		}
	}))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	delay, err := URLTest(ctx, target.URL, timeoutDialer{})
	require.NoError(t, err, "an explicit HTTP timeout must not be silently capped at 15 seconds")
	require.GreaterOrEqual(t, delay, uint16(16000))
}
