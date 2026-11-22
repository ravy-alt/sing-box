package group

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func healthFixture(t *testing.T) *SmartURLTest {
	node := smartNode("node", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("unused") })
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{UDPResponseTimeout: badoption.Duration(20 * time.Millisecond), UDPFailureWindow: badoption.Duration(time.Second)}, node)
	return s
}

func TestSmartUDPCountsSessionsAndBoundsMemory(t *testing.T) {
	s := healthFixture(t)
	key := smartCacheKey{"dns.test", "udp"}
	_, generation, _ := s.cache.get(key, time.Now())
	serial := s.cache.put(key, "node", generation, time.Now())
	sessions := make([]*smartUDPSession, 10)
	for i := range sessions {
		sessions[i] = &smartUDPSession{health: &s.health, key: key, tag: "node", serial: serial}
	}
	s.health.expire(time.Now().Add(time.Second))
	require.Empty(t, s.wake, "idle connections are not failures")
	for _, session := range sessions {
		for range 100 {
			session.sent()
		}
	}
	require.Len(t, s.health.pending, 10, "count sessions, not packets")
	sessions[0].received()
	sessions[1].received()
	s.health.expire(time.Now().Add(50 * time.Millisecond))
	require.Len(t, s.wake, 1, "8 failures out of 10 sessions triggers a test")
	require.Empty(t, s.health.pending)
	cached, _, _ := s.cache.get(key, time.Now())
	require.Empty(t, cached)
	// Frequent packets from one session cannot dominate a later sample window.
	for range 100 {
		sessions[0].sent()
		sessions[0].received()
	}
	require.Empty(t, s.health.pending)
	sessions[0].next = time.Now().Add(-time.Second)
	sessions[0].sent()
	require.Len(t, s.health.pending, 1, "long-lived sessions are sampled again")
	clear(s.health.pending)
	for range smartUDPPendingLimit + 50 {
		(&smartUDPSession{health: &s.health}).sent()
	}
	require.Len(t, s.health.pending, smartUDPPendingLimit)
}

func TestSmartUDPReplyAndWriteFailureAreNotTimeouts(t *testing.T) {
	s := healthFixture(t)
	for range 10 {
		session := &smartUDPSession{health: &s.health}
		session.sent()
		session.received()
		session.received()
	}
	s.health.expire(time.Now().Add(time.Second))
	require.Empty(t, s.wake)
	for range 10 {
		session := &smartUDPSession{health: &s.health}
		session.sent()
		session.writeFailed()
	}
	s.health.expire(time.Now().Add(time.Second))
	require.Empty(t, s.wake)
}

type smartRoomPacket struct {
	N.NetPacketConn
	writes int
}

func (c *smartRoomPacket) FrontHeadroom() int { return 23 }
func (c *smartRoomPacket) RearHeadroom() int  { return 16 }
func (c *smartRoomPacket) WritePacket(b *buf.Buffer, dst M.Socksaddr) error {
	c.writes++
	b.ExtendHeader(23)
	b.Extend(16)
	b.Release()
	return nil
}

func TestSmartPacketBufferedContractsAndRealUDP(t *testing.T) {
	s := healthFixture(t)
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()
	go func() {
		data := make([]byte, 2048)
		for {
			n, addr, err := server.ReadFrom(data)
			if err != nil {
				return
			}
			server.WriteTo(data[:n], addr)
		}
	}()
	for _, buffered := range []bool{false, true} {
		client, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		r := smartDialResult{smartAttemptResult: smartAttemptResult{connection: client}, tag: "node"}
		wrapper := newSmartPacketConn(context.Background(), s, r)
		require.NoError(t, wrapper.SetDeadline(time.Now().Add(time.Second)))
		if buffered {
			packet := buf.As([]byte("test"))
			require.NoError(t, wrapper.WritePacket(packet, M.SocksaddrFromNet(server.LocalAddr())))
			reply := buf.NewPacket()
			_, err = wrapper.ReadPacket(reply)
			require.NoError(t, err)
			require.Equal(t, "test", string(reply.Bytes()))
			reply.Release()
		} else {
			_, err = wrapper.WriteTo([]byte("test"), server.LocalAddr())
			require.NoError(t, err)
			data := make([]byte, 128)
			n, _, err := wrapper.ReadFrom(data)
			require.NoError(t, err)
			require.Equal(t, "test", string(data[:n]))
		}
		require.Empty(t, s.health.pending)
		require.False(t, wrapper.ReaderReplaceable())
		require.False(t, wrapper.WriterReplaceable())
		wrapper.Close()
	}
	raw, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	room := &smartRoomPacket{NetPacketConn: bufio.NewPacketConn(raw)}
	wrapper := newSmartPacketConn(context.Background(), s, smartDialResult{smartAttemptResult: smartAttemptResult{connection: room}, tag: "node"})
	defer wrapper.Close()
	require.Equal(t, 23, N.CalculateFrontHeadroom(wrapper))
	require.Equal(t, 16, N.CalculateRearHeadroom(wrapper))
	b := buf.NewSize(23 + 4 + 16)
	b.Resize(23, 0)
	_, err = b.Write([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, wrapper.WritePacket(b, M.ParseSocksaddr("127.0.0.1:53")))
	require.Equal(t, 1, room.writes)
	require.Len(t, s.health.pending, 1)
}

type smartFailWriteConn struct{ net.Conn }

func (c *smartFailWriteConn) Write([]byte) (int, error) { return 0, errors.New("handshake failed") }

func TestSmartFirstWriteFailureInvalidatesOnce(t *testing.T) {
	s := healthFixture(t)
	key := smartCacheKey{"target.test", "tcp"}
	_, generation, _ := s.cache.get(key, time.Now())
	serial := s.cache.put(key, "node", generation, time.Now())
	client, peer := net.Pipe()
	defer peer.Close()
	wrapper := newSmartConn(context.Background(), s, smartDialResult{smartAttemptResult: smartAttemptResult{connection: &smartFailWriteConn{client}}, key: key, tag: "node", serial: serial}, false)
	defer wrapper.Close()
	for range 10 {
		_, err := wrapper.Write([]byte("application data"))
		require.Error(t, err)
	}
	cached, _, _ := s.cache.get(key, time.Now())
	require.Empty(t, cached)
	require.Len(t, s.health.failures, 1)
}

func TestSmartClosedOrCancelledUDPDoesNotTriggerFailureProbe(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "cancelled"}[cancelled], func(t *testing.T) {
			s := healthFixture(t)
			key := smartCacheKey{"cancelled.example", "udp"}
			_, generation, _ := s.cache.get(key, time.Now())
			serial := s.cache.put(key, "node", generation, time.Now())
			for range 10 {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				health := newSmartConnectionHealth(ctx, s, smartDialResult{smartAttemptResult: smartAttemptResult{connection: new(smartTestCloser)}, key: key, tag: "node", serial: serial}, true)
				health.udp.sent()
				if cancelled {
					cancel()
				} else {
					require.NoError(t, health.Close())
					health.udp.sent()
				}
			}
			s.health.expire(time.Now().Add(time.Second))
			require.Empty(t, s.wake, "administrative closes and cancelled requests are not UDP failures")
			require.Empty(t, s.health.pending)
			cached, _, _ := s.cache.get(key, time.Now())
			require.Equal(t, "node", cached, "ordinary teardown must retain destination affinity")
		})
	}
}
