package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	smux "github.com/sagernet/sing-mux"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type traceNode struct {
	adapter.Outbound
	tag string
}

func (n *traceNode) Tag() string { return n.tag }

type traceEcho struct{}

func (*traceEcho) NewConnectionEx(_ context.Context, c net.Conn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	defer c.Close()
	io.Copy(c, c)
}
func (*traceEcho) NewPacketConnectionEx(_ context.Context, c N.PacketConn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	c.Close()
}

type traceDialer struct {
	selected atomic.Pointer[traceNode]
	calls    atomic.Int32
	cleared  atomic.Int32
	server   *smux.Service
	workers  sync.WaitGroup
}

func (d *traceDialer) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	d.calls.Add(1)
	adapter.TraceOutbound(ctx, d.selected.Load())
	adapter.ContextFrom(ctx).OutboundTrace.BindCache(func() { d.cleared.Add(1) })
	client, peer := net.Pipe()
	d.workers.Add(1)
	go func() {
		defer d.workers.Done()
		d.server.NewConnectionEx(context.Background(), peer, M.Socksaddr{}, M.Socksaddr{}, nil)
	}()
	return client, nil
}
func (*traceDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected packet dial")
}

func TestReusedMultiplexSessionRetainsItsActualRoute(t *testing.T) {
	for _, protocol := range []string{"h2mux", "smux", "yamux"} {
		t.Run(protocol, func(t *testing.T) {
			server, err := smux.NewService(smux.ServiceOptions{Logger: logger.NOP(), HandlerEx: new(traceEcho), NewStreamContext: func(ctx context.Context, _ net.Conn) context.Context { return ctx }})
			require.NoError(t, err)
			dialer := &traceDialer{server: server}
			dialer.selected.Store(&traceNode{tag: "relay-a"})
			client, err := NewClientWithOptions(dialer, logger.NOP(), option.OutboundMultiplexOptions{Enabled: true, Protocol: protocol, MaxConnections: 1})
			require.NoError(t, err)
			defer func() { client.Close(); dialer.workers.Wait() }()
			for _, root := range []string{"first-connection", "second-connection"} {
				trace := adapter.NewOutboundTrace([]adapter.Outbound{&traceNode{tag: root}})
				ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{OutboundTrace: trace})
				conn, err := client.DialContext(ctx, "tcp", M.ParseSocksaddr("example.test:443"))
				require.NoError(t, err)
				if protocol != "h2mux" {
					require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
				}
				_, err = conn.Write([]byte("ping"))
				require.NoError(t, err)
				reply := make([]byte, 4)
				_, err = io.ReadFull(conn, reply)
				require.NoError(t, err)
				require.Equal(t, "ping", string(reply))
				chain := trace.Chain()
				require.Len(t, chain, 2)
				require.Equal(t, root, chain[0].Tag())
				require.Equal(t, "relay-a", chain[1].Tag())
				trace.InvalidateCaches()
				conn.Close()
				dialer.selected.Store(&traceNode{tag: "relay-b"})
			}
			require.EqualValues(t, 1, dialer.calls.Load(), "the second stream reused the old transport")
			require.EqualValues(t, 2, dialer.cleared.Load(), "both application connections retain transport cache invalidation")
		})
	}
}
