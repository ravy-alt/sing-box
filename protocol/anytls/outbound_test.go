package anytls

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/bufio/deadline"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"

	"github.com/stretchr/testify/require"
)

// Some deployed AnyTLS peers accept connect-mode requests but always include
// an address in their replies. Reading those replies as connect-mode frames
// corrupts the first datagram and treats payload bytes as the next frame length.
func TestOutboundUDPAddressedReplies(t *testing.T) {
	for _, addressedOnly := range []bool{false, true} {
		for _, address := range []string{"8.8.8.8:53", "[2001:db8::1]:53", "dns.test:53"} {
			t.Run(fmt.Sprintf("addressed_only=%t/%s", addressedOnly, address), func(t *testing.T) {
				destination := M.ParseSocksaddr(address)
				client, server := net.Pipe()
				t.Cleanup(func() { client.Close(); server.Close() })
				require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
				peerDone := make(chan error, 1)
				go func() {
					peerDone <- echoUoTDatagrams(server, addressedOnly, destination, 3)
				}()
				h := &Outbound{
					logger: log.NewNOPFactory().Logger(),
					uotClient: &uot.Client{
						Version: uot.Version,
						Dialer: anytlsDialer(func(_ context.Context, target M.Socksaddr) (net.Conn, error) {
							require.Equal(t, uot.RequestDestination(uot.Version), target)
							return client, nil
						}),
					},
				}
				conn, err := h.DialContext(context.Background(), N.NetworkUDP, destination)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				require.Equal(t, destination, M.SocksaddrFromNet(conn.RemoteAddr()))
				require.True(t, deadline.NeedAdditionalReadDeadline(conn), "UoT frame reads need deadline protection")
				require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
				// Small DNS-sized datagrams, an exact-size 2048-byte read, then
				// another small packet must retain their boundaries on one stream.
				for _, size := range []int{107, 2048, 31} {
					payload := bytes.Repeat([]byte{0x34, 0x81}, (size+1)/2)[:size]
					n, err := conn.Write(payload)
					require.NoError(t, err)
					require.Equal(t, size, n)
					response := make([]byte, 2048)
					n, err = conn.Read(response)
					require.NoError(t, err)
					require.Equal(t, payload, response[:n])
				}
				require.NoError(t, <-peerDone)
			})
		}
	}
}

func echoUoTDatagrams(conn net.Conn, addressedOnly bool, destination M.Socksaddr, count int) error {
	defer conn.Close()
	request, err := uot.ReadRequest(conn)
	if err != nil {
		return err
	}
	if request.Destination != destination {
		return fmt.Errorf("unexpected request destination: %v", request.Destination)
	}
	reader := uot.NewConn(conn, *request)
	responseMode := *request
	if addressedOnly {
		responseMode.IsConnect = false
	}
	writer := uot.NewConn(conn, responseMode)
	buffer := make([]byte, 65535)
	for range count {
		n, source, err := reader.ReadFrom(buffer)
		if err != nil {
			return err
		}
		// A domain destination is represented by UDPAddr only after resolution;
		// use the request destination to exercise domain-addressed replies too.
		if destination.IsIP() && M.SocksaddrFromNet(source) != destination {
			return fmt.Errorf("unexpected packet destination: %v", source)
		}
		if _, err = writer.WriteTo(buffer[:n], destination); err != nil {
			return err
		}
	}
	return nil
}
