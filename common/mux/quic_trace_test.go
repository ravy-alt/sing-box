//go:build with_quic

package mux_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-quic/hysteria"
	"github.com/sagernet/sing-quic/hysteria2"
	"github.com/sagernet/sing-quic/tuic"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type quicTraceDialer struct{ entered chan context.Context }

func (d *quicTraceDialer) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	d.entered <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *quicTraceDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected packet dial")
}

type tracedQUICClient interface {
	DialConn(context.Context, M.Socksaddr) (net.Conn, error)
	CloseWithError(error) error
}

func TestQUICSharedOffersPreserveProbeClassification(t *testing.T) {
	for _, protocol := range []string{"hysteria", "hysteria2", "tuic"} {
		for _, probe := range []bool{false, true} {
			t.Run(protocol+map[bool]string{false: "/business", true: "/probe"}[probe], func(t *testing.T) {
				lifetime, stop := context.WithCancel(context.Background())
				defer stop()
				config, err := tls.NewClient(lifetime, logger.NOP(), "server.test", option.OutboundTLSOptions{Enabled: true, Insecure: true})
				require.NoError(t, err)
				dialer := &quicTraceDialer{entered: make(chan context.Context, 1)}
				var client tracedQUICClient
				server := M.ParseSocksaddr("127.0.0.1:443")
				switch protocol {
				case "hysteria":
					client, err = hysteria.NewClient(hysteria.ClientOptions{Context: lifetime, Dialer: dialer, TLSConfig: config, Logger: logger.NOP(), ServerAddress: server, SendBPS: 1000000, ReceiveBPS: 1000000, NewSession: adapter.NewTracedSession, SessionSelected: adapter.TraceSessionSelection, SessionTransportSelected: adapter.TraceSessionTransport})
				case "hysteria2":
					client, err = hysteria2.NewClient(hysteria2.ClientOptions{Context: lifetime, Dialer: dialer, TLSConfig: config, Logger: logger.NOP(), ServerAddress: server, NewSession: adapter.NewTracedSession, SessionSelected: adapter.TraceSessionSelection, SessionTransportSelected: adapter.TraceSessionTransport})
				case "tuic":
					client, err = tuic.NewClient(tuic.ClientOptions{Context: lifetime, Dialer: dialer, TLSConfig: config, ServerAddress: server, NewSession: adapter.NewTracedSession, SessionSelected: adapter.TraceSessionSelection})
				}
				require.NoError(t, err)
				defer client.CloseWithError(net.ErrClosed)
				request, cancel := context.WithCancel(context.Background())
				defer cancel()
				if probe {
					request = adapter.ContextWithURLTest(request)
				}
				done := make(chan error, 1)
				go func() { _, err := client.DialConn(request, M.ParseSocksaddr("target.test:443")); done <- err }()
				var transport context.Context
				select {
				case transport = <-dialer.entered:
				case <-time.After(time.Second):
					t.Fatal("shared transport did not start")
				}
				require.Equal(t, probe, adapter.IsURLTest(transport))
				require.NotNil(t, adapter.ContextFrom(transport).OutboundTrace)
				cancel()
				select {
				case err = <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("foreground cancellation did not return")
				}
				require.NoError(t, transport.Err(), "a foreground timeout must not tear down the shared offer")
				client.CloseWithError(net.ErrClosed)
				select {
				case <-transport.Done():
				case <-time.After(time.Second):
					t.Fatal("closing client did not cancel the shared offer")
				}
			})
		}
	}
}
