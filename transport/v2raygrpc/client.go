package v2raygrpc

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

type Client struct {
	ctx           context.Context
	dialer        N.Dialer
	serverAddr    string
	serviceName   string
	dialOptions   []grpc.DialOption
	conn          atomic.Pointer[grpc.ClientConn]
	connAccess    sync.Mutex
	businessDials atomic.Int32
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayGRPCOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	var dialOptions []grpc.DialOption
	var transportCredentials credentials.TransportCredentials
	if tlsConfig != nil {
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
		}
		transportCredentials = NewTLSTransportCredentials(tlsConfig)
		if tlsConfig.ServerName() != "" {
			dialOptions = append(dialOptions, grpc.WithAuthority(tlsConfig.ServerName()))
		}
	} else {
		transportCredentials = insecure.NewCredentials()
	}
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(&tracedCredentials{transportCredentials}))
	if options.IdleTimeout > 0 {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(options.IdleTimeout),
			Timeout:             time.Duration(options.PingTimeout),
			PermitWithoutStream: options.PermitWithoutStream,
		}))
	}
	dialOptions = append(dialOptions, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  500 * time.Millisecond,
			Multiplier: 1.5,
			Jitter:     0.2,
			MaxDelay:   19 * time.Second,
		},
		MinConnectTimeout: 5 * time.Second,
	}))
	//nolint:staticcheck
	dialOptions = append(dialOptions, grpc.WithReturnConnectionError())
	client := &Client{
		ctx:         ctx,
		dialer:      dialer,
		serverAddr:  serverAddr.String(),
		serviceName: options.ServiceName,
		dialOptions: dialOptions,
	}
	client.dialOptions = append(client.dialOptions, grpc.WithContextDialer(func(ctx context.Context, server string) (net.Conn, error) {
		ctx = adapter.ContextForMultiplexSession(ctx)
		// gRPC owns reconnects and discards RPC context values. Background and
		// probe-only connection attempts must not cause business failure loops.
		if client.businessDials.Load() == 0 {
			ctx = adapter.ContextWithURLTest(ctx)
		}
		conn, err := dialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr(server))
		if err != nil {
			return nil, err
		}
		return adapter.WithTransportTrace(conn, adapter.ContextFrom(ctx).OutboundTrace), nil
	}))
	return client, nil
}

func (c *Client) connect() (*grpc.ClientConn, error) {
	conn := c.conn.Load()
	if conn != nil && conn.GetState() != connectivity.Shutdown {
		return conn, nil
	}
	c.connAccess.Lock()
	defer c.connAccess.Unlock()
	conn = c.conn.Load()
	if conn != nil && conn.GetState() != connectivity.Shutdown {
		return conn, nil
	}
	//nolint:staticcheck
	conn, err := grpc.DialContext(c.ctx, c.serverAddr, c.dialOptions...)
	if err != nil {
		return nil, err
	}
	c.conn.Store(conn)
	return conn, nil
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	if !adapter.IsURLTest(ctx) {
		c.businessDials.Add(1)
		defer c.businessDials.Add(-1)
	}
	clientConn, err := c.connect()
	if err != nil {
		return nil, err
	}
	client := NewGunServiceClient(clientConn).(GunServiceCustomNameClient)
	streamCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	stopPropagation := context.AfterFunc(ctx, func() {
		cancel(context.Cause(ctx))
	})
	stream, err := client.TunCustomName(streamCtx, c.serviceName)
	if err != nil {
		stopPropagation()
		cancel(err)
		return nil, err
	}
	if !stopPropagation() {
		return nil, context.Cause(ctx)
	}
	if remote, ok := peer.FromContext(stream.Context()); ok {
		if info, ok := remote.AuthInfo.(*tracedAuthInfo); ok {
			adapter.TraceSessionSelection(ctx, info.trace)
		}
	}
	return NewGRPCConn(stream, cancel), nil
}

func (c *Client) Close() error {
	conn := c.conn.Swap(nil)
	if conn != nil {
		conn.Close()
	}
	return nil
}
