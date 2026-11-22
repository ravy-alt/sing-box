package urltest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

func TestHistoryIsolatesNodesURLsAndLateCompletions(t *testing.T) {
	h := NewHistoryStorage()
	start := time.Now()
	for _, node := range []string{"c", "d"} {
		for _, link := range []string{"https://a.example/", "https://b.example/"} {
			(Result{URL: link, Started: start, Outbound: node, Delay: 30}).Store(h)
		}
	}
	(Result{URL: "https://a.example/", Started: start.Add(time.Second), Outbound: "c", Err: errors.New("timeout")}).Store(h)
	require.Nil(t, h.LoadURLTestHistory("c", "https://a.example/"))
	require.NotNil(t, h.LoadURLTestHistory("c", "https://b.example/"))
	require.NotNil(t, h.LoadURLTestHistory("d", "https://a.example/"))
	(Result{URL: "https://a.example/", Started: start, Outbound: "c", Delay: 1}).Store(h)
	require.Nil(t, h.LoadURLTestHistory("c", "https://a.example/"), "an old completion cannot revive a newer failed result")
	copy := h.LoadURLTestHistory("c", "https://b.example/")
	copy.Delay = 0
	require.EqualValues(t, 30, h.LoadURLTestHistory("c", "https://b.example/").Delay)
	require.Nil(t, h.LoadURLTestHistory("c"), "default queries cannot borrow another URL")
	h.DeleteURLTestHistory("c", "https://a.example/")
	require.NotNil(t, h.LoadURLTestHistory("c", "https://b.example/"))
	h.DeleteURLTestHistory("c")
	require.Empty(t, h.URLs("c"))
}

func TestHistoryBoundsURLsAndHandlesConcurrentSnapshots(t *testing.T) {
	h := NewHistoryStorage()
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range 100 {
				link := fmt.Sprintf("https://example.com/%d/%d", worker, i)
				h.StoreURLTestHistory("node", &adapter.URLTestHistory{URL: link, Time: time.Now(), Delay: 1})
				h.LoadURLTestHistory("node", link)
				h.URLs("node")
			}
		}()
	}
	workers.Wait()
	require.Len(t, h.URLs("node"), maxURLsPerNode)
}
