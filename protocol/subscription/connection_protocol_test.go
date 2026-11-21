package subscription

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/protocol/anytls"
	httpproxy "github.com/sagernet/sing-box/protocol/http"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	"github.com/sagernet/sing-box/protocol/socks"
	"github.com/sagernet/sing-box/protocol/trojan"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/protocol/vmess"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func connectionProtocolContext() context.Context {
	ctx := testContext()
	i := service.FromContext[adapter.InboundRegistry](ctx).(*inbound.Registry)
	o := service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry)
	for _, register := range []func(*inbound.Registry){anytls.RegisterInbound, httpproxy.RegisterInbound, shadowsocks.RegisterInbound, socks.RegisterInbound, trojan.RegisterInbound, vless.RegisterInbound, vmess.RegisterInbound} {
		register(i)
	}
	for _, register := range []func(*outbound.Registry){anytls.RegisterOutbound, shadowsocks.RegisterOutbound, socks.RegisterOutbound, trojan.RegisterOutbound, vless.RegisterOutbound, vmess.RegisterOutbound} {
		register(o)
	}
	registerConnectionQUIC(i, o)
	return ctx
}

type connectionProtocolCase struct {
	name, protocol, server, client string
	tcpOnly                        bool
}

func connectionProtocolCases() []connectionProtocolCase {
	const uuid = `"uuid":"a40c76d3-7c47-46da-b187-1598c7c8c596"`
	users := `"users":[{` + uuid + `}]`
	return []connectionProtocolCase{
		{"http", "http", `"users":[]`, `"username":""`, true},
		{"socks5", "socks", `"users":[]`, `"version":"5"`, false},
		{"socks-uot", "socks", `"users":[]`, `"version":"5","udp_over_tcp":{"enabled":true}`, false},
		{"shadowsocks", "shadowsocks", `"method":"aes-128-gcm","password":"regression-only"`, `"method":"aes-128-gcm","password":"regression-only"`, false},
		{"shadowsocks-uot", "shadowsocks", `"method":"aes-128-gcm","password":"regression-only"`, `"method":"aes-128-gcm","password":"regression-only","udp_over_tcp":{"enabled":true}`, false},
		{"trojan", "trojan", `"users":[{"password":"regression-only"}]`, `"password":"regression-only"`, false},
		{"vmess", "vmess", users, uuid + `,"security":"aes-128-gcm"`, false},
		{"vmess-xudp", "vmess", users, uuid + `,"security":"chacha20-poly1305","packet_encoding":"xudp"`, false},
		{"vmess-packetaddr", "vmess", users, uuid + `,"security":"auto","packet_encoding":"packetaddr"`, false},
		{"vless", "vless", users, uuid + `,"packet_encoding":""`, false},
		{"vless-xudp", "vless", users, uuid + `,"packet_encoding":"xudp"`, false},
		{"vless-packetaddr", "vless", users, uuid + `,"packet_encoding":"packetaddr"`, false},
		{"vless-ws", "vless", users + `,"transport":{"type":"ws","path":"/test"}`, uuid + `,"transport":{"type":"ws","path":"/test"}`, false},
		{"vless-smux", "vless", users + `,"multiplex":{"enabled":true}`, uuid + `,"multiplex":{"enabled":true,"protocol":"smux","max_connections":1}`, false},
		{"vless-yamux", "vless", users + `,"multiplex":{"enabled":true}`, uuid + `,"multiplex":{"enabled":true,"protocol":"yamux","max_connections":1}`, false},
		{"vless-h2mux", "vless", users + `,"multiplex":{"enabled":true}`, uuid + `,"multiplex":{"enabled":true,"protocol":"h2mux","max_connections":1}`, false},
	}
}

func startConnectionProxy(t *testing.T, protocol connectionProtocolCase) int {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	startBoxWithContext(t, connectionProtocolContext(), fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"local","predefined":{"echo.test":"127.0.0.1"}}]},"inbounds":[{"type":%q,"tag":"server","listen":"127.0.0.1","listen_port":%d,%s}],"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct","default_domain_resolver":"local"}}`, protocol.protocol, port, protocol.server))
	return port
}

func startConnectionEcho(t *testing.T) (M.Socksaddr, M.Socksaddr) {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	p, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(10 * time.Second))
				_, _ = io.Copy(c, c)
			})
		}
	})
	wg.Go(func() {
		b := make([]byte, 65535)
		for {
			n, a, err := p.ReadFrom(b)
			if err != nil {
				return
			}
			_, _ = p.WriteTo(b[:n], a)
		}
	})
	t.Cleanup(func() { l.Close(); p.Close(); wg.Wait() })
	return M.SocksaddrFromNet(l.Addr()), M.SocksaddrFromNet(p.LocalAddr())
}

// Exercise real encoders, servers, Copy/CopyPacket, and old/new generations.
// No public proxy service, privileged listener, or external DNS is involved.
func TestSubscriptionConnectionProtocols(t *testing.T) {
	testConnectionProtocols(t, connectionProtocolCases())
}

func testConnectionProtocols(t *testing.T, protocols []connectionProtocolCase) {
	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			port := startConnectionProxy(t, protocol)
			relayPort := startConnectionProxy(t, protocol)
			tcpDestination, udpDestination := startConnectionEcho(t)
			for _, path := range []string{"subscription", "selector", "chain", "nested", "external-chain"} {
				t.Run(path, func(t *testing.T) {
					node := func(tag string, port int, detour string) string {
						return fmt.Sprintf(`{"type":%q,"tag":%q,"server":"127.0.0.1","server_port":%d,%s%s}`, protocol.protocol, tag, port, protocol.client, detour)
					}
					document := `{"outbounds":[` + node("node", port, "") + `]}`
					if path == "chain" {
						document = `{"outbounds":[` + node("node", port, `,"detour":"relay"`) + `,` + node("relay", relayPort, "") + `]}`
					} else if path == "external-chain" {
						document = `{"outbounds":[` + node("node", port, `,"detour":"relay-group"`) + `]}`
					}
					var removed atomic.Bool
					var sourceURL string
					source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch {
						case r.URL.Path == "/relay":
							fmt.Fprint(w, `{"outbounds":[`+node("relay", relayPort, "")+`]}`)
						case path == "nested" && r.URL.Path != "/child":
							fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{},"empty_fallback":"direct"},{"type":"direct","tag":"direct"}]}`, sourceURL+"/child")
						case removed.Load():
							fmt.Fprint(w, `{"outbounds":[]}`)
						default:
							fmt.Fprint(w, document)
						}
					}))
					sourceURL = source.URL
					t.Cleanup(source.Close)
					groups := ""
					if path == "selector" {
						groups = `,{"type":"selector","tag":"pick","import":[{"group":"sub"}],"empty_fallback":"direct"}`
					} else if path == "external-chain" {
						groups = fmt.Sprintf(`,{"type":"subscription","tag":"relay-source","url":%q,"http_client":{}},{"type":"selector","tag":"relay-group","import":[{"group":"relay-source"}]}`, source.URL+"/relay")
					}
					_, ctx := startBoxWithContext(t, connectionProtocolContext(), fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"direct"},{"type":"subscription","tag":"sub","url":%q,"http_client":{},"empty_fallback":"direct"}%s]}`, source.URL, groups))
					ctx = interrupt.ContextWithIsExternalConnection(ctx, true)
					s := getSubscription(t, ctx, "sub")
					var target adapter.Outbound = s
					if path == "selector" {
						target, _ = service.FromContext[adapter.OutboundManager](ctx).Outbound("pick")
					}
					old := s.current
					var relayOld *snapshot
					if path == "external-chain" {
						relayOld = getSubscription(t, ctx, "relay-source").current
					}
					stream, err := target.DialContext(ctx, "tcp", tcpDestination)
					require.NoError(t, err)
					t.Cleanup(func() { stream.Close() })
					var packet N.NetPacketConn
					var bound net.Conn
					if !protocol.tcpOnly {
						raw, err := target.ListenPacket(ctx, udpDestination)
						require.NoError(t, err)
						packet = bufio.NewPacketConn(raw)
						t.Cleanup(func() { packet.Close() })
						bound, err = target.DialContext(ctx, "udp", udpDestination)
						require.NoError(t, err)
						t.Cleanup(func() { bound.Close() })
					}
					streamExchange := func(conn net.Conn, size int) {
						t.Helper()
						// WebSocket/HTTP2 deliberately lack native socket deadlines.
						// Keep a close timer as the test's hard bound for either direction.
						stop := time.AfterFunc(3*time.Second, func() { conn.Close() })
						defer stop.Stop()
						if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
							require.ErrorIs(t, err, os.ErrInvalid)
						}
						payload := bytes.Repeat([]byte{0x6b}, size)
						// Read concurrently so this also covers payloads larger than a
						// transport window and encrypted protocol frame.
						response := make([]byte, size)
						done := make(chan error, 1)
						go func() { _, err := io.ReadFull(conn, response); done <- err }()
						n, err := bufio.Copy(conn, bytes.NewReader(payload))
						readErr := <-done
						require.NoError(t, err)
						require.NoError(t, readErr)
						require.EqualValues(t, size, n)
						require.Equal(t, payload, response)
					}
					exchange := func() {
						t.Helper()
						streamExchange(stream, 70000)
						if packet == nil {
							return
						}
						for _, cached := range []bool{true, false} {
							stop := time.AfterFunc(3*time.Second, func() { packet.Close() })
							defer stop.Stop()
							if err := packet.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
								require.ErrorIs(t, err, os.ErrInvalid)
							}
							payload := bytes.Repeat([]byte{0x62}, 1200)
							n, err := bufio.CopyPacket(packet, &regressionPacketReader{payload: payload, target: udpDestination, cached: cached})
							require.ErrorIs(t, err, io.EOF)
							require.EqualValues(t, 1200, n)
							b := N.NewReadWaitOptions(packet, nil).NewPacketBuffer()
							address, err := packet.ReadPacket(b)
							body := append([]byte(nil), b.Bytes()...)
							b.Release()
							require.NoError(t, err)
							require.Equal(t, udpDestination, address)
							require.Equal(t, payload, body)
						}
						stop := time.AfterFunc(3*time.Second, func() { bound.Close() })
						defer stop.Stop()
						payload := bytes.Repeat([]byte{0x63}, 1200)
						n, err := bound.Write(payload)
						require.NoError(t, err)
						require.Equal(t, len(payload), n)
						// Datagram decoders may use the same slice for their wire
						// header/authentication overhead; do not size it to payload.
						response := make([]byte, 65535)
						n, err = bound.Read(response)
						require.NoError(t, err)
						require.Equal(t, payload, response[:n])
					}
					exchange()
					for _, deleted := range []bool{false, true, false} {
						removed.Store(deleted)
						require.NoError(t, s.Refresh(ctx))
						if path == "external-chain" {
							require.NoError(t, getSubscription(t, ctx, "relay-source").Refresh(ctx))
						}
						exchange()
						old.mu.Lock()
						closed := old.closed
						old.mu.Unlock()
						require.False(t, closed, "live streams must retain the original generation")
					}
					fresh, err := target.DialContext(ctx, "tcp", tcpDestination)
					require.NoError(t, err)
					t.Cleanup(func() { fresh.Close() })
					streamExchange(fresh, 128)
					_ = fresh.Close()
					_ = stream.Close()
					if packet != nil {
						_ = packet.Close()
						_ = bound.Close()
					}
					require.Eventually(t, func() bool {
						old.mu.Lock()
						defer old.mu.Unlock()
						return old.closed && old.refs == 0
					}, 2*time.Second, time.Millisecond, "all leases must drain after the last user connection closes")
					if relayOld != nil {
						require.Eventually(t, func() bool {
							relayOld.mu.Lock()
							defer relayOld.mu.Unlock()
							return relayOld.closed && relayOld.refs == 0
						}, 2*time.Second, time.Millisecond, "retired connection pools must release the other provider's leases")
					}
				})
			}
		})
	}
}
