package anytls

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anytls/sing-anytls/padding"
	"github.com/anytls/sing-anytls/session"
	"github.com/sagernet/sing-box/adapter"
	A "github.com/sagernet/sing/common/atomic"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type anyTraceNode struct {
	adapter.Outbound
	tag string
}

func (n *anyTraceNode) Tag() string { return n.tag }

func TestAnyTLSReusedSessionKeepsActualDetour(t *testing.T) {
	var factory A.TypedValue[*padding.PaddingFactory]
	require.True(t, padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &factory))
	var calls, cleared atomic.Int32
	var servers sync.WaitGroup
	client := session.NewClient(context.Background(), logger.NOP(), func(ctx context.Context) (net.Conn, error) {
		calls.Add(1)
		trace := adapter.NewOutboundTrace([]adapter.Outbound{&anyTraceNode{tag: "actual-relay"}})
		trace.BindCache(func() { cleared.Add(1) })
		local, remote := net.Pipe()
		server := session.NewServerSession(remote, func(stream *session.Stream) {
			go func() {
				defer stream.Close()
				_, err := M.SocksaddrSerializer.ReadAddrPort(stream)
				if err == nil {
					io.Copy(stream, stream)
				}
			}()
		}, &factory, logger.NOP())
		servers.Add(1)
		go func() { defer servers.Done(); server.Run() }()
		return adapter.WithTransportTrace(local, trace), nil
	}, &factory, time.Minute, time.Minute, 0)
	defer func() { client.Close(); servers.Wait() }()
	h := &Outbound{sessionClient: client}
	for _, root := range []string{"request-a", "request-b"} {
		trace := adapter.NewOutboundTrace([]adapter.Outbound{&anyTraceNode{tag: root}})
		ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{OutboundTrace: trace})
		conn, err := h.createProxy(ctx, M.ParseSocksaddr("example.test:443"))
		require.NoError(t, err)
		require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
		_, err = conn.Write([]byte("ping"))
		require.NoError(t, err)
		reply := make([]byte, 4)
		_, err = io.ReadFull(conn, reply)
		require.NoError(t, err)
		require.Equal(t, "ping", string(reply))
		chain := trace.Chain()
		require.Len(t, chain, 2)
		require.Equal(t, root, chain[0].Tag())
		require.Equal(t, "actual-relay", chain[1].Tag())
		trace.InvalidateCaches()
		conn.Close()
	}
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 2, cleared.Load())
}
