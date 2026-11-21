package subscription

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing/common/bufio"

	"github.com/stretchr/testify/require"
)

func TestSubscriptionEncryptedForcedDrain(t *testing.T) {
	for _, protocol := range connectionProtocolCases() {
		if protocol.name != "shadowsocks" && protocol.name != "vless-xudp" {
			continue
		}
		t.Run(protocol.name, func(t *testing.T) {
			port := startConnectionProxy(t, protocol)
			tcpDestination, udpDestination := startConnectionEcho(t)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"outbounds":[{"type":%q,"tag":"node","server":"127.0.0.1","server_port":%d,%s}]}`, protocol.protocol, port, protocol.client)
			}))
			t.Cleanup(source.Close)
			_, ctx := startBoxWithContext(t, connectionProtocolContext(), configFor(source.URL, `"drain_timeout":"20ms",`))
			s := getSubscription(t, ctx, "sub")
			stream, err := s.DialContext(ctx, "tcp", tcpDestination)
			require.NoError(t, err)
			t.Cleanup(func() { stream.Close() })
			packet, err := s.ListenPacket(ctx, udpDestination)
			require.NoError(t, err)
			t.Cleanup(func() { packet.Close() })
			require.NoError(t, stream.SetDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, packet.SetDeadline(time.Now().Add(3*time.Second)))
			_, err = stream.Write([]byte("ready"))
			require.NoError(t, err)
			_, err = io.ReadFull(stream, make([]byte, 5))
			require.NoError(t, err)
			_, err = packet.WriteTo([]byte("ready"), udpDestination)
			require.NoError(t, err)
			_, _, err = packet.ReadFrom(make([]byte, 65535))
			require.NoError(t, err)
			results := make(chan error, 2)
			go func() { _, err := stream.Read(make([]byte, 16)); results <- err }()
			go func() { _, _, err := packet.ReadFrom(make([]byte, 65535)); results <- err }()
			old := s.current
			require.NoError(t, s.Refresh(ctx))
			for range 2 {
				select {
				case err := <-results:
					require.Error(t, err, "retirement must interrupt pending reads")
				case <-time.After(time.Second):
					t.Fatal("retirement did not close an encrypted connection")
				}
			}
			// Copy goroutines and explicit shutdown may close the same handles
			// after the retirement timer has already closed them.
			var wg sync.WaitGroup
			for range 16 {
				wg.Go(func() { _ = stream.Close(); _ = packet.Close() })
			}
			wg.Wait()
			require.Eventually(t, func() bool {
				old.mu.Lock()
				defer old.mu.Unlock()
				return old.closed && old.refs == 0 && len(old.connections) == 0
			}, time.Second, time.Millisecond)
			fresh, err := s.DialContext(ctx, "tcp", tcpDestination)
			require.NoError(t, err)
			t.Cleanup(func() { fresh.Close() })
			require.NoError(t, fresh.SetDeadline(time.Now().Add(time.Second)))
			_, err = fresh.Write([]byte("new"))
			require.NoError(t, err)
			data := make([]byte, 3)
			_, err = io.ReadFull(bufio.NewExtendedConn(fresh), data)
			require.NoError(t, err)
			require.Equal(t, "new", string(data))
		})
	}
}
