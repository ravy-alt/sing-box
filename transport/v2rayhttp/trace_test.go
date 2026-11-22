package v2rayhttp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2raygrpc"
	"github.com/sagernet/sing-box/transport/v2raygrpclite"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type testTraceNode struct {
	adapter.Outbound
	tag string
}

func (n *testTraceNode) Tag() string { return n.tag }

type testTraceDialer struct {
	calls atomic.Int32
	probe atomic.Bool
}

func (d *testTraceDialer) DialContext(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
	d.calls.Add(1)
	d.probe.Store(adapter.IsURLTest(ctx))
	adapter.TraceOutbound(ctx, &testTraceNode{tag: "session-relay"})
	return (&net.Dialer{}).DialContext(ctx, network, dst.String())
}
func (*testTraceDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected packet dial")
}

func TestHTTP2AndGRPCReusePreserveActualTransportTrace(t *testing.T) {
	for _, protocol := range []string{"http2", "grpc", "grpc-full"} {
		t.Run(protocol, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/grpc")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				payload := make([]byte, 128)
				for {
					n, err := r.Body.Read(payload)
					if n > 0 {
						if _, e := w.Write(payload[:n]); e != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
					if err != nil {
						return
					}
				}
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			config, err := tls.NewClient(context.Background(), logger.NOP(), "127.0.0.1", option.OutboundTLSOptions{Enabled: true, Insecure: true})
			require.NoError(t, err)
			dialer := new(testTraceDialer)
			address := M.SocksaddrFromNet(server.Listener.Addr())
			var client adapter.V2RayClientTransport
			if protocol == "http2" {
				client, err = v2rayhttp.NewClient(context.Background(), dialer, address, option.V2RayHTTPOptions{}, config)
				require.NoError(t, err)
			} else if protocol == "grpc" {
				client = v2raygrpclite.NewClient(context.Background(), dialer, address, option.V2RayGRPCOptions{}, config)
			} else {
				client, err = v2raygrpc.NewClient(context.Background(), dialer, address, option.V2RayGRPCOptions{ServiceName: "trace"}, config)
				require.NoError(t, err)
			}
			defer client.Close()
			for _, tag := range []string{"first", "second"} {
				trace := adapter.NewOutboundTrace([]adapter.Outbound{&testTraceNode{tag: tag}})
				ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{OutboundTrace: trace})
				if tag == "first" {
					ctx = adapter.ContextWithURLTest(ctx)
				}
				conn, err := client.DialContext(ctx)
				require.NoError(t, err)
				_, err = conn.Write([]byte("ping"))
				require.NoError(t, err)
				reply := make([]byte, 4)
				_, err = io.ReadFull(conn, reply)
				require.NoError(t, err)
				require.Equal(t, "ping", string(reply))
				chain := trace.Chain()
				require.Len(t, chain, 2)
				require.Equal(t, tag, chain[0].Tag())
				require.Equal(t, "session-relay", chain[1].Tag())
				conn.Close()
			}
			require.EqualValues(t, 1, dialer.calls.Load())
			require.True(t, dialer.probe.Load(), "the cold session must retain its initiating probe classification")
		})
	}
}
