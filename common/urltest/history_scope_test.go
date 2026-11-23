package urltest

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestHistoryScopePublicationAndRetirement(t *testing.T) {
	root := NewHistoryStorage()
	options, err := NormalizeUDPTest(option.UDPTestOptions{})
	require.NoError(t, err)
	owns := func(tag string) bool { return strings.HasPrefix(tag, "sub/") }
	store := func(h *HistoryStorage, tag string, delay uint16) {
		(Result{URL: "https://a.example/", Started: time.Now(), Outbound: tag, Delay: delay}).Store(h)
		(UDPResult{Options: options, Started: time.Now(), Outbound: tag, Delay: delay}).Store(h)
	}
	first := root.NewScope("sub", owns)
	store(first, "sub/node", 10)
	require.Nil(t, root.LoadURLTestHistory("sub/node", "https://a.example/"))
	first.Publish()
	require.EqualValues(t, 10, root.LoadURLTestHistory("sub/node", "https://a.example/").Delay)
	require.EqualValues(t, 10, root.LoadUDPHistory("sub/node", options).Delay)
	store(root, "sub/node", 20)
	require.EqualValues(t, 20, first.LoadUDPHistory("sub/node", options).Delay, "API updates reach the inner group")
	store(root, "host", 30)
	second := root.NewScope("sub", owns)
	store(second, "sub/node", 40)
	store(second, "host", 999)
	require.EqualValues(t, 30, root.LoadUDPHistory("host", options).Delay, "pending probes cannot affect the running host")
	second.Publish()
	store(first, "sub/node", 999)
	store(first, "host", 999)
	require.EqualValues(t, 40, root.LoadUDPHistory("sub/node", options).Delay, "old completion cannot overwrite the replacement")
	require.EqualValues(t, 30, root.LoadUDPHistory("host", options).Delay)
	require.NoError(t, first.Close())
	require.EqualValues(t, 40, root.LoadURLTestHistory("sub/node", "https://a.example/").Delay)
	root.DeleteURLTestHistory("sub/node")
	require.Nil(t, second.LoadUDPHistory("sub/node", options))
	require.Empty(t, second.URLs("sub/node"))
	store(second, "sub/node", 50)
	require.NoError(t, second.Close())
	require.Nil(t, root.LoadUDPHistory("sub/node", options))
	third := root.NewScope("sub", owns)
	third.Publish()
	require.Nil(t, root.LoadURLTestHistory("sub/node", "https://a.example/"), "re-added names must not inherit removed node measurements")
}

func TestHistoryNestedScopesAndConcurrentPublication(t *testing.T) {
	root := NewHistoryStorage()
	owns := func(tag string) bool { return strings.HasPrefix(tag, "sub/") }
	parent := root.NewScope("sub", owns)
	child := parent.NewScope("inner", func(tag string) bool { return strings.HasPrefix(tag, "sub/inner/") })
	child.Publish()
	(Result{Started: time.Now(), Outbound: "sub/inner/node", Delay: 10}).Store(child)
	require.Nil(t, root.LoadURLTestHistory("sub/inner/node"))
	parent.Publish()
	require.EqualValues(t, 10, root.LoadURLTestHistory("sub/inner/node").Delay)
	var workers sync.WaitGroup
	for range 3 {
		workers.Go(func() {
			for range 100 {
				(Result{Started: time.Now(), Outbound: "sub/inner/node", Delay: 20}).Store(child)
				root.LoadURLTestHistory("sub/inner/node")
			}
		})
	}
	for range 30 {
		next := root.NewScope("sub", owns)
		next.Publish()
	}
	workers.Wait()
	require.Nil(t, root.LoadURLTestHistory("sub/inner/node"))
	require.NoError(t, parent.Close())
	(Result{Started: time.Now(), Outbound: "host", Delay: 1}).Store(child)
	require.Nil(t, root.LoadURLTestHistory("host"), "closing an ancestor must also isolate descendant writes")
}
