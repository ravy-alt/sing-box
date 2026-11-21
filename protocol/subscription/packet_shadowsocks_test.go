package subscription

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func shadowsocksPacketTestContext() context.Context {
	ctx := testContext()
	shadowsocks.RegisterInbound(service.FromContext[adapter.InboundRegistry](ctx).(*inbound.Registry))
	shadowsocks.RegisterOutbound(service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry))
	return ctx
}

func startPacketShadowServer(t *testing.T, method, password string) int {
	t.Helper()
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, listener.Close())
	startBoxWithContext(t, shadowsocksPacketTestContext(), fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"local","predefined":{"echo.test":"127.0.0.1"}}]},"inbounds":[{"type":"shadowsocks","tag":"server","listen":"127.0.0.1","listen_port":%d,"network":"udp","method":%q,"password":%q}],"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct","default_domain_resolver":"local"}}`, port, method, password))
	return port
}

func TestShadowsocksSubscriptionUDPRefresh(t *testing.T) {
	for _, cipher := range []struct{ method, password string }{
		{"aes-128-gcm", "regression-only"},
		{"aes-256-gcm", "regression-only"},
		{"chacha20-ietf-poly1305", "regression-only"},
		{"2022-blake3-aes-128-gcm", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))},
		{"2022-blake3-aes-256-gcm", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))},
		{"2022-blake3-chacha20-poly1305", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))},
	} {
		t.Run(cipher.method, func(t *testing.T) {
			exitPort := startPacketShadowServer(t, cipher.method, cipher.password)
			relayPort := startPacketShadowServer(t, cipher.method, cipher.password)
			echo, err := net.ListenPacket("udp4", "127.0.0.1:0")
			require.NoError(t, err)
			done := make(chan struct{})
			go func() {
				defer close(done)
				data := make([]byte, 65535)
				for {
					n, sender, err := echo.ReadFrom(data)
					if err != nil {
						return
					}
					_, _ = echo.WriteTo(data[:n], sender)
				}
			}()
			t.Cleanup(func() { echo.Close(); <-done })
			for _, path := range []string{"subscription", "member", "selector", "urltest", "chain", "nested"} {
				t.Run(path, func(t *testing.T) {
					node := func(tag string, port int, detour string) string {
						return fmt.Sprintf(`{"type":"shadowsocks","tag":%q,"server":"127.0.0.1","server_port":%d,"method":%q,"password":%q%s}`, tag, port, cipher.method, cipher.password, detour)
					}
					document := `{"outbounds":[` + node("node", exitPort, "") + `]}`
					if path == "chain" {
						document = `{"outbounds":[` + node("node", exitPort, `,"detour":"relay"`) + `,` + node("relay", relayPort, "") + `]}`
					}
					var sourceURL string
					remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if path == "nested" && r.URL.Path != "/child" {
							fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, sourceURL+"/child")
						} else {
							fmt.Fprint(w, document)
						}
					}))
					sourceURL = remote.URL
					t.Cleanup(remote.Close)
					groups := ""
					if path == "selector" || path == "urltest" {
						extra := ""
						if path == "urltest" {
							extra = `,"url":"http://127.0.0.1:1"`
						}
						groups = fmt.Sprintf(`,{"type":%q,"tag":"pick","import":[{"group":"sub","include":"^node$"}]%s}`, path, extra)
					}
					_, ctx := startBoxWithContext(t, shadowsocksPacketTestContext(), fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}}%s]}`, sourceURL, groups))
					s := getSubscription(t, ctx, "sub")
					var target adapter.Outbound = s
					manager := service.FromContext[adapter.OutboundManager](ctx)
					if path == "member" {
						target, _ = manager.Outbound(memberTag("sub", "node"))
					} else if path == "selector" || path == "urltest" {
						target, _ = manager.Outbound("pick")
					}
					destination := M.SocksaddrFromNet(echo.LocalAddr())
					if path == "selector" {
						destination = M.Socksaddr{Fqdn: "echo.test", Port: destination.Port}
					}
					open := func() N.NetPacketConn {
						conn, err := target.ListenPacket(interrupt.ContextWithIsExternalConnection(ctx, true), destination)
						require.NoError(t, err)
						t.Cleanup(func() { conn.Close() })
						return bufio.NewPacketConn(conn)
					}
					packet := open()
					old := s.current
					exchange := func(packet N.NetPacketConn, cached bool, size int) {
						t.Helper()
						require.NoError(t, packet.SetDeadline(time.Now().Add(3*time.Second)))
						payload := bytes.Repeat([]byte{0x61}, size)
						n, err := bufio.CopyPacket(packet, &regressionPacketReader{payload: payload, target: destination, cached: cached})
						require.ErrorIs(t, err, io.EOF)
						require.EqualValues(t, len(payload), n)
						options := N.NewReadWaitOptions(packet, nil)
						response := options.NewPacketBuffer()
						address, err := packet.ReadPacket(response)
						body := append([]byte(nil), response.Bytes()...)
						response.Release()
						require.NoError(t, err)
						require.Equal(t, destination, address)
						require.Equal(t, payload, body)
					}
					exchange(packet, true, 64)
					exchange(packet, false, 1200)
					require.NoError(t, s.Refresh(ctx))
					old.mu.Lock()
					closed := old.closed
					old.mu.Unlock()
					require.False(t, closed, "live UDP connection must retain its generation")
					exchange(packet, true, 1200)
					exchange(packet, false, 64)
					exchange(open(), true, 1200)
					require.NoError(t, packet.Close())
					require.Eventually(t, func() bool {
						old.mu.Lock()
						defer old.mu.Unlock()
						return old.closed && old.refs == 0
					}, time.Second, time.Millisecond, "closing the old connection must release its generation")
				})
			}
		})
	}
}
