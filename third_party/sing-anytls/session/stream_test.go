package session

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/logger"
)

type discardConn struct{ net.Conn }

func (*discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (*discardConn) Close() error                     { return nil }
func (*discardConn) SetWriteDeadline(time.Time) error { return nil }

func TestStreamConcurrentReadWriteAndClose(t *testing.T) {
	for range 100 {
		session := NewServerSession(&discardConn{}, nil, nil, logger.NOP())
		stream := newStream(1, session)
		session.streams[1] = stream
		var calls atomic.Int32
		stream.dieHook = func() { calls.Add(1) }
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, operation := range []func(){
			func() {
				for range 100 {
					_, _ = stream.Write([]byte("test"))
				}
			},
			func() { _, _ = stream.Read(make([]byte, 16)) },
			func() { _ = stream.Close() },
			func() { stream.closeLocally() },
			func() { _ = session.Close() },
		} {
			wg.Add(1)
			go func(operation func()) { defer wg.Done(); <-start; operation() }(operation)
		}
		close(start)
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("close hook called %d times", calls.Load())
		}
		if _, err := stream.Write([]byte("late")); err == nil {
			t.Fatal("write succeeded after close")
		}
	}
}

func TestStreamRemovedBeforePoolReuse(t *testing.T) {
	session := NewServerSession(&discardConn{}, nil, nil, logger.NOP())
	var called bool
	stream, err := session.openStream(func() {
		called = true
		session.streamLock.RLock()
		defer session.streamLock.RUnlock()
		if len(session.streams) != 0 {
			t.Error("session returned to pool before stream was removed")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("close hook not installed before stream publication")
	}
	_ = session.Close()
}
