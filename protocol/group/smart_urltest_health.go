package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const smartUDPBuckets = 64
const smartUDPPendingLimit = 10000

type smartUDPBucket struct {
	epoch            int64
	success, failure int
}

type smartHealth struct {
	mu       sync.Mutex
	group    *SmartURLTest
	failures []time.Time
	pending  map[*smartUDPSession]time.Time
	buckets  [smartUDPBuckets]smartUDPBucket
}

func (h *smartHealth) init(group *SmartURLTest) {
	h.group = group
	h.pending = make(map[*smartUDPSession]time.Time)
}

func (s *SmartURLTest) recordFailure(ctx context.Context, err error) {
	if err == nil || ctx.Err() != nil || s.ctx.Err() != nil || adapter.IsURLTest(ctx) || errors.Is(err, context.Canceled) || errors.Is(err, errSmartDialCapacity) {
		return
	}
	h := &s.health
	now := time.Now()
	h.mu.Lock()
	cutoff := now.Add(-time.Duration(s.options.FailureWindow))
	first := 0
	for first < len(h.failures) && h.failures[first].Before(cutoff) {
		first++
	}
	h.failures = append(h.failures[:copy(h.failures, h.failures[first:])], now)
	trigger := len(h.failures) > s.options.FailureThreshold
	if trigger {
		h.failures = h.failures[:0]
	}
	h.mu.Unlock()
	if trigger {
		s.requestTest()
	}
}

// Buckets bound memory regardless of packet/session rate. The sliding UDP
// window has a resolution of window/64; each session contributes at most one
// result per window, so a busy stream cannot drown out many unanswered queries.
func (h *smartHealth) outcomeLocked(failure bool, now time.Time) bool {
	width := max(int64(time.Millisecond), int64(h.group.options.UDPFailureWindow)/smartUDPBuckets)
	epoch := now.UnixNano() / width
	bucket := &h.buckets[epoch%smartUDPBuckets]
	if bucket.epoch != epoch {
		*bucket = smartUDPBucket{epoch: epoch}
	}
	if failure {
		bucket.failure++
	} else {
		bucket.success++
	}
	success, failed := 0, 0
	for _, item := range h.buckets {
		if item.epoch > epoch-smartUDPBuckets && item.epoch <= epoch {
			success += item.success
			failed += item.failure
		}
	}
	if success+failed >= h.group.options.UDPMinSessions && float64(failed)/float64(success+failed) >= h.group.options.UDPFailureRatio {
		clear(h.buckets[:])
		return true
	}
	return false
}

func (h *smartHealth) run(ctx context.Context) {
	interval := max(time.Millisecond, min(time.Second, time.Duration(h.group.options.UDPResponseTimeout)/4))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer func() { h.mu.Lock(); clear(h.pending); h.mu.Unlock() }()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			h.expire(now)
		}
	}
}

func (h *smartHealth) expire(now time.Time) {
	var failed []*smartUDPSession
	trigger := false
	h.mu.Lock()
	for session, deadline := range h.pending {
		if session.cancelled() {
			delete(h.pending, session)
			continue
		}
		if !now.Before(deadline) {
			delete(h.pending, session)
			failed = append(failed, session)
			trigger = h.outcomeLocked(true, now) || trigger
		}
	}
	h.mu.Unlock()
	for _, session := range failed {
		session.forget()
	}
	if trigger {
		h.group.requestTest()
	}
}

type smartUDPSession struct {
	ctx    context.Context
	closed bool // protected by health.mu
	health *smartHealth
	key    smartCacheKey
	tag    string
	serial uint64
	next   time.Time // protected by health.mu
}

func (u *smartUDPSession) cancelled() bool {
	return u.ctx != nil && u.ctx.Err() != nil
}

func (u *smartUDPSession) stop() {
	if u == nil {
		return
	}
	u.health.mu.Lock()
	u.closed = true
	delete(u.health.pending, u)
	u.health.mu.Unlock()
}

func (u *smartUDPSession) sent() {
	if u == nil {
		return
	}
	now := time.Now()
	h := u.health
	h.mu.Lock()
	defer h.mu.Unlock()
	if u.closed || u.cancelled() || now.Before(u.next) || h.group.ctx.Err() != nil || len(h.pending) >= smartUDPPendingLimit {
		return
	}
	if _, exists := h.pending[u]; exists {
		return
	}
	h.pending[u] = now.Add(time.Duration(h.group.options.UDPResponseTimeout))
	u.next = now.Add(time.Duration(h.group.options.UDPFailureWindow))
}

func (u *smartUDPSession) received() {
	if u == nil {
		return
	}
	now := time.Now()
	h := u.health
	h.mu.Lock()
	deadline, found := h.pending[u]
	if u.cancelled() {
		delete(h.pending, u)
		h.mu.Unlock()
		return
	}
	trigger := false
	failed := found && !now.Before(deadline)
	if found {
		delete(h.pending, u)
		trigger = h.outcomeLocked(failed, now)
	}
	h.mu.Unlock()
	if failed {
		u.forget()
	}
	if trigger {
		h.group.requestTest()
	}
}

func (u *smartUDPSession) writeFailed() {
	if u == nil {
		return
	}
	u.health.mu.Lock()
	delete(u.health.pending, u)
	u.health.mu.Unlock()
}

func (u *smartUDPSession) forget() { u.health.group.cache.forget(u.key, u.tag, u.serial) }

type smartConnectionHealth struct {
	group      *SmartURLTest
	ctx        context.Context
	result     smartDialResult
	udp        *smartUDPSession
	firstWrite atomic.Bool
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
}

func newSmartConnectionHealth(ctx context.Context, s *SmartURLTest, r smartDialResult, udp bool) *smartConnectionHealth {
	h := &smartConnectionHealth{group: s, ctx: ctx, result: r}
	if udp && !adapter.IsURLTest(ctx) {
		h.udp = &smartUDPSession{ctx: ctx, health: &s.health, key: r.key, tag: r.tag, serial: r.serial}
	}
	return h
}

func (h *smartConnectionHealth) wrote(err error) {
	first := h.firstWrite.CompareAndSwap(false, true)
	if err == nil {
		return
	}
	h.udp.writeFailed()
	if h.closed.Load() {
		return
	}
	if !adapter.IsURLTest(h.ctx) {
		h.group.cache.forget(h.result.key, h.result.tag, h.result.serial)
		// Lazy proxy handshakes may fail on the first write. Never replay
		// application bytes, but invalidate affinity and count the failure.
		if first {
			h.group.recordFailure(h.ctx, err)
		}
	}
}

func (h *smartConnectionHealth) Close() error {
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		h.udp.stop()
		h.closeErr = h.result.connection.Close()
		if h.result.release != nil {
			h.result.release()
		}
	})
	return h.closeErr
}

type smartConn struct {
	N.ExtendedConn
	health *smartConnectionHealth
}

func newSmartConn(ctx context.Context, s *SmartURLTest, r smartDialResult, udp bool) *smartConn {
	return &smartConn{bufio.NewExtendedConn(r.connection.(net.Conn)), newSmartConnectionHealth(ctx, s, r, udp)}
}
func (c *smartConn) Read(p []byte) (int, error) {
	n, err := c.ExtendedConn.Read(p)
	if err == nil || n > 0 {
		c.health.udp.received()
	}
	return n, err
}
func (c *smartConn) ReadBuffer(b *buf.Buffer) error {
	err := c.ExtendedConn.ReadBuffer(b)
	if err == nil {
		c.health.udp.received()
	}
	return err
}
func (c *smartConn) Write(p []byte) (int, error) {
	c.health.udp.sent()
	n, err := c.ExtendedConn.Write(p)
	c.health.wrote(err)
	return n, err
}
func (c *smartConn) WriteBuffer(b *buf.Buffer) error {
	c.health.udp.sent()
	err := c.ExtendedConn.WriteBuffer(b)
	c.health.wrote(err)
	return err
}
func (c *smartConn) Close() error     { return c.health.Close() }
func (c *smartConn) CloseRead() error { return N.CloseRead(c.ExtendedConn) }
func (c *smartConn) CloseWrite() error {
	writer, _ := N.UnwrapCountWriter(c.ExtendedConn, nil)
	if duplex, ok := N.UnwrapWriter(writer).(N.WriteCloser); ok {
		return duplex.CloseWrite()
	}
	// Protocols without half-close still need the normal close path to release
	// the dial lease and stop passive health tracking exactly once.
	return c.Close()
}
func (c *smartConn) Upstream() any           { return c.ExtendedConn }
func (c *smartConn) ReaderReplaceable() bool { return false }
func (c *smartConn) WriterReplaceable() bool { return false }

type smartPacketConn struct {
	N.NetPacketConn
	health *smartConnectionHealth
}

func newSmartPacketConn(ctx context.Context, s *SmartURLTest, r smartDialResult) *smartPacketConn {
	return &smartPacketConn{bufio.NewPacketConn(r.connection.(net.PacketConn)), newSmartConnectionHealth(ctx, s, r, true)}
}
func (c *smartPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.NetPacketConn.ReadFrom(p)
	if err == nil || n > 0 {
		c.health.udp.received()
	}
	return n, addr, err
}
func (c *smartPacketConn) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	addr, err := c.NetPacketConn.ReadPacket(b)
	if err == nil {
		c.health.udp.received()
	}
	return addr, err
}
func (c *smartPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.health.udp.sent()
	n, err := c.NetPacketConn.WriteTo(p, addr)
	c.health.wrote(err)
	return n, err
}
func (c *smartPacketConn) WritePacket(b *buf.Buffer, addr M.Socksaddr) error {
	c.health.udp.sent()
	err := c.NetPacketConn.WritePacket(b, addr)
	c.health.wrote(err)
	return err
}
func (c *smartPacketConn) Close() error            { return c.health.Close() }
func (c *smartPacketConn) Upstream() any           { return c.NetPacketConn }
func (c *smartPacketConn) ReaderReplaceable() bool { return false }
func (c *smartPacketConn) WriterReplaceable() bool { return false }
