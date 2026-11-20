//go:build with_wireguard && with_gvisor

package subscription

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/protocol/wireguard"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/curve25519"
)

func TestRealWireGuardEndpointSubscription(t *testing.T) {
	serverKey := [32]byte{1, 2, 3, 4, 5}
	clientKey := [32]byte{6, 7, 8, 9, 10}
	serverPublic, err := curve25519.X25519(serverKey[:], curve25519.Basepoint)
	require.NoError(t, err)
	clientPublic, err := curve25519.X25519(clientKey[:], curve25519.Basepoint)
	require.NoError(t, err)
	encode := base64.StdEncoding.EncodeToString
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, listener.Close())
	serverContext := testContext()
	wireguard.RegisterEndpoint(service.FromContext[adapter.EndpointRegistry](serverContext).(*endpoint.Registry))
	startBoxWithContext(t, serverContext, fmt.Sprintf(`{"log":{"disabled":true},"endpoints":[{"type":"wireguard","tag":"server","address":["10.13.0.2/32"],"private_key":%q,"listen_port":%d,"peers":[{"public_key":%q,"allowed_ips":["10.13.0.1/32"]}]}],"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct"}}`, encode(serverKey[:]), port, encode(clientPublic)))
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "wireguard-ok") }))
	defer target.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"udp","tag":"d","server":"1.1.1.1","detour":"wg"}]},"endpoints":[{"type":"wireguard","tag":"wg","address":["10.13.0.1/32"],"private_key":%q,"peers":[{"address":"127.0.0.1","port":%d,"public_key":%q,"allowed_ips":["10.13.0.2/32"]}]}]}`, encode(clientKey[:]), port, encode(serverPublic))
	}))
	defer remote.Close()
	clientContext := testContext()
	wireguard.RegisterEndpoint(service.FromContext[adapter.EndpointRegistry](clientContext).(*endpoint.Registry))
	_, ctx := startBoxWithContext(t, clientContext, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	destination := M.ParseSocksaddrHostPort("10.13.0.2", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")).Port)
	conn, err := s.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	body, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Contains(t, string(body), "wireguard-ok")
	require.NoError(t, conn.Close())
	require.NoError(t, s.Refresh(ctx))
}
