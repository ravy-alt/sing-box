package subscription

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/bufio/deadline"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"

	"github.com/stretchr/testify/require"
)

func TestLeasedConnectionReadDeadlineRequirement(t *testing.T) {
	// Framed UDP-over-TCP reads cannot safely resume after a partial frame has
	// timed out. DNS/STUN and chained transports must still see this marker.
	raw, peer := net.Pipe()
	t.Cleanup(func() { peer.Close() })
	framed := uot.NewConn(raw, uot.Request{})
	var stream net.Conn = framed
	var packet N.NetPacketConn = framed
	for range 3 {
		stream = &leasedConn{Conn: stream, release: func() {}}
		packet = &leasedPacketConn{NetPacketConn: packet, release: func() {}}
		require.True(t, deadline.NeedAdditionalReadDeadline(stream))
		require.True(t, deadline.NeedAdditionalReadDeadline(packet))
	}
	t.Cleanup(func() { stream.Close() })
	require.False(t, deadline.NeedAdditionalReadDeadline(&leasedConn{Conn: peer}))
}

// A handshake can change the allocation contract. CopyPacket must observe the
// transition and recalculate its options, including through nested leases.
type changingPacketConn struct {
	N.NetPacketConn
	written int
}

func (c *changingPacketConn) NeedHandshakeForWrite() bool { return c.written == 0 }
func (c *changingPacketConn) FrontHeadroom() int {
	if c.written == 0 {
		return 0
	}
	return 32
}
func (c *changingPacketConn) WritePacket(b *buf.Buffer, _ M.Socksaddr) error {
	defer b.Release()
	b.ExtendHeader(c.FrontHeadroom())
	c.written++
	return nil
}

type twoPacketReader struct{ count int }

func (r *twoPacketReader) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	if r.count == 2 {
		return M.Socksaddr{}, io.EOF
	}
	r.count++
	_, err := b.Write([]byte("payload"))
	return M.ParseSocksaddr("127.0.0.1:53"), err
}

func TestLeasedPacketHandshakeRecalculatesHeadroom(t *testing.T) {
	raw := &changingPacketConn{}
	packet := &leasedPacketConn{NetPacketConn: &leasedPacketConn{NetPacketConn: raw}}
	require.True(t, N.NeedHandshakeForWriteAny(packet))
	require.NotPanics(t, func() {
		n, err := bufio.CopyPacket(packet, &twoPacketReader{})
		require.ErrorIs(t, err, io.EOF)
		require.EqualValues(t, 14, n)
	})
	require.Equal(t, 2, raw.written)
	require.False(t, N.NeedHandshakeForWriteAny(packet))
}

type closeCountingConn struct {
	net.Conn
	count atomic.Int32
	err   error
}

func (c *closeCountingConn) Close() error { c.count.Add(1); return c.err }

func TestLeasedConnectionConcurrentClose(t *testing.T) {
	for _, packet := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "packet"}[packet], func(t *testing.T) {
			failure := errors.New("close failure")
			raw := &closeCountingConn{err: failure}
			var released atomic.Int32
			release := func() { released.Add(1) }
			var conn io.Closer = &leasedConn{Conn: raw, release: release}
			if packet {
				conn = &leasedPacketConn{NetPacketConn: bufio.NewUnbindPacketConnWithAddr(raw, M.Socksaddr{}), release: release}
			}
			var wg sync.WaitGroup
			results := make(chan error, 32)
			for range 32 {
				wg.Go(func() { results <- conn.Close() })
			}
			wg.Wait()
			close(results)
			for err := range results {
				require.ErrorIs(t, err, failure)
			}
			require.EqualValues(t, 1, released.Load())
			require.EqualValues(t, 1, raw.count.Load(), "shutdown, refresh and copy loops can all close the same connection")
		})
	}
}
