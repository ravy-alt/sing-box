package subscription

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

// The caller supplies a plaintext buffer. The encrypted Shadowsocks UDP
// datagram needs additional space for its salt, destination and authentication
// tag; it must not be truncated before authentication and decoding.
func TestShadowsocksUDPReadBuffer(t *testing.T) {
	for _, cipher := range []struct{ method, password string }{
		{"aes-128-gcm", "read-buffer-test"},
		{"aes-256-gcm", "read-buffer-test"},
		{"chacha20-ietf-poly1305", "read-buffer-test"},
		{"2022-blake3-aes-128-gcm", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))},
		{"2022-blake3-aes-256-gcm", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))},
		{"2022-blake3-chacha20-poly1305", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))},
	} {
		t.Run(cipher.method, func(t *testing.T) { testShadowsocksUDPReadBuffer(t, cipher.method, cipher.password) })
	}
}

func testShadowsocksUDPReadBuffer(t *testing.T, method, password string) {
	port := startPacketShadowServer(t, method, password)
	_, destination := startConnectionEcho(t)
	node := fmt.Sprintf(`{"type":"shadowsocks","tag":"node","server":"127.0.0.1","server_port":%d,"method":%q,"password":%q}`, port, method, password)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[`+node+`]}`)
	}))
	t.Cleanup(source.Close)
	for _, path := range []string{"native", "subscription"} {
		t.Run(path, func(t *testing.T) {
			outbounds, tag := node, "node"
			if path == "subscription" {
				outbounds = fmt.Sprintf(`{"type":"subscription","tag":"sub","url":%q,"http_client":{}}`, source.URL)
				tag = "sub"
			}
			_, ctx := startBoxWithContext(t, shadowsocksPacketTestContext(), `{"log":{"disabled":true},"outbounds":[`+outbounds+`]}`)
			outbound, loaded := service.FromContext[adapter.OutboundManager](ctx).Outbound(tag)
			require.True(t, loaded)
			for _, bound := range []bool{false, true} {
				t.Run(fmt.Sprintf("bound=%t", bound), func(t *testing.T) {
					var write func([]byte) (int, error)
					var read func([]byte) (int, error)
					if bound {
						conn, err := outbound.DialContext(ctx, "udp", destination)
						require.NoError(t, err)
						t.Cleanup(func() { conn.Close() })
						require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
						write, read = conn.Write, conn.Read
					} else {
						conn, err := outbound.ListenPacket(ctx, destination)
						require.NoError(t, err)
						t.Cleanup(func() { conn.Close() })
						require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
						write = func(p []byte) (int, error) { return conn.WriteTo(p, destination) }
						read = func(p []byte) (int, error) {
							n, source, err := conn.ReadFrom(p)
							if err == nil {
								require.Equal(t, destination.UDPAddr(), source.(*net.UDPAddr))
							}
							return n, err
						}
					}
					for _, size := range []int{107, 474, 512, 1000, 107} {
						payload := bytes.Repeat([]byte{0x42}, size)
						n, err := write(payload)
						require.NoError(t, err)
						require.Equal(t, size, n)
						buffer := make([]byte, 512)
						n, err = read(buffer)
						require.NoError(t, err, "payload size %d", size)
						// Match net.PacketConn: truncate only the decoded datagram,
						// consume its remainder, then read the next packet normally.
						require.Equal(t, payload[:min(size, len(buffer))], buffer[:n])
					}
				})
			}
		})
	}
}
