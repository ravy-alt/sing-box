package subscription

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"

	shadowsocks "github.com/sagernet/sing-shadowsocks2"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// Includes the cached first-packet path in route.ConnectionManager, which was
// the path in the reported buffer.ExtendHeader panic.
type regressionPacketReader struct {
	payload []byte
	target  M.Socksaddr
	cached  bool
}

func (r *regressionPacketReader) ReadCachedPacket() *N.PacketBuffer {
	if !r.cached || r.payload == nil {
		return nil
	}
	p := N.NewPacketBuffer()
	p.Buffer = buf.NewPacket()
	_, _ = p.Buffer.Write(r.payload)
	p.Destination = r.target
	r.payload = nil
	return p
}

func (r *regressionPacketReader) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	if r.payload == nil {
		return M.Socksaddr{}, io.EOF
	}
	_, err := b.Write(r.payload)
	r.payload = nil
	return r.target, err
}

// Capture full datagrams without relying on the host's UDP fragmentation or
// firewall policy. The real protocol encoder and CopyPacket paths still run.
type packetCaptureConn struct {
	net.Conn
	data []byte
}

func (c *packetCaptureConn) Write(p []byte) (int, error) {
	c.data = append([]byte(nil), p...)
	return len(p), nil
}
func (*packetCaptureConn) Close() error { return nil }

func TestLeasedShadowsocksPacketCopy(t *testing.T) {
	for _, cached := range []bool{true, false} {
		for _, size := range []int{64, 1400, 16000} {
			t.Run(fmt.Sprintf("cached=%t/size=%d", cached, size), func(t *testing.T) {
				conn := new(packetCaptureConn)
				method, err := shadowsocks.CreateMethod(context.Background(), "aes-128-gcm", shadowsocks.MethodOptions{Password: "regression-only"})
				require.NoError(t, err)
				var released atomic.Int32
				packet := &leasedPacketConn{NetPacketConn: method.DialPacketConn(conn), release: func() { released.Add(1) }}
				t.Cleanup(func() { packet.Close() })
				payload := bytes.Repeat([]byte{0x5a}, size)
				reader := &regressionPacketReader{payload: payload, target: M.ParseSocksaddr("192.0.2.1:443"), cached: cached}
				var copied int64
				require.NotPanics(t, func() { copied, err = bufio.CopyPacket(packet, reader) })
				require.ErrorIs(t, err, io.EOF)
				require.EqualValues(t, size, copied)
				require.Len(t, conn.data, size+16+7+16, "AES-128 salt, IPv4 destination and authentication tag")
				require.NotEqual(t, payload, conn.data[:size], "the packet must be encrypted")
				require.Zero(t, released.Load())
				require.NoError(t, packet.Close())
				_ = packet.Close()
				require.EqualValues(t, 1, released.Load(), "the lease still releases exactly once")
			})
		}
	}
}

type sizedPacketConn struct{ N.NetPacketConn }

func (*sizedPacketConn) FrontHeadroom() int  { return 31 }
func (*sizedPacketConn) RearHeadroom() int   { return 19 }
func (*sizedPacketConn) ReaderOverhead() int { return 23 }
func (*sizedPacketConn) ReaderMTU() int      { return 9000 }
func (*sizedPacketConn) WriterMTU() int      { return 1300 }

func TestLeasedPacketBufferRequirementsRemainVisible(t *testing.T) {
	var original N.NetPacketConn = &sizedPacketConn{}
	wrapped := original
	for range 3 {
		wrapped = &leasedPacketConn{NetPacketConn: wrapped}
		require.Equal(t, N.NewReadWaitOptions(original, original), N.NewReadWaitOptions(wrapped, wrapped))
		require.Equal(t, N.CalculateMTU(original, nil), N.CalculateMTU(wrapped, nil))
		require.Equal(t, N.CalculateMTU(nil, original), N.CalculateMTU(nil, wrapped))
		require.Same(t, wrapped, N.UnwrapPacketReader(wrapped), "reader must retain the generation lease")
		require.Same(t, wrapped, N.UnwrapPacketWriter(wrapped), "writer must retain the generation lease")
	}
}
