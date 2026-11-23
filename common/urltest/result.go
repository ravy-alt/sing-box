package urltest

import (
	"context"
	"errors"
	"time"

	"github.com/sagernet/sing-box/adapter"
	N "github.com/sagernet/sing/common/network"
)

type Result struct {
	URL      string
	Started  time.Time
	Via      string
	Outbound string
	Delay    uint16
	Err      error
}

func (r Result) Store(history *HistoryStorage) {
	// Groups have no history of their own. A failed dynamic selection that
	// never reached a leaf cannot erase an arbitrarily guessed member's data.
	if r.Outbound == "" || errors.Is(r.Err, context.Canceled) {
		return
	}
	h := &adapter.URLTestHistory{URL: r.URL, Time: r.Started, Delay: r.Delay, Outbound: r.Outbound}
	if r.Err != nil {
		h.Delay = 0
	}
	history.StoreURLTestHistory(r.Outbound, h)
}

// Pin the entire currently selected chain, including Smart groups, before
// testing. A latency/capability test of C must not silently succeed through a
// business-traffic fallback D and attribute D's result to C.
func Resolve(outbound adapter.Outbound) (adapter.Outbound, error) {
	return ResolveNetwork(outbound, N.NetworkTCP)
}

// ResolveNetwork pins the selection for the payload network. Callers measuring
// TCP latency keep using Resolve; UDP routing must follow the UDP selection.
func ResolveNetwork(outbound adapter.Outbound, network string) (adapter.Outbound, error) {
	seen := make(map[string]bool)
	for outbound != nil {
		if seen[outbound.Tag()] {
			return nil, errors.New("circular URL test selection")
		}
		seen[outbound.Tag()] = true
		group, ok := adapter.OutboundGroupFor(outbound)
		if !ok {
			return outbound, nil
		}
		outbound = group.Selected(network)
	}
	return nil, errors.New("URL test group has no selected outbound")
}

func OutboundTestURL(outbound adapter.Outbound) string {
	if group, ok := adapter.OutboundGroupFor(outbound); ok {
		if configured, ok := group.(interface{ TestURL() string }); ok {
			return NormalizeURL(configured.TestURL())
		}
	}
	return DefaultURL
}

// Queries follow the current selected chain, including dynamic groups. They
// never read a stored group alias that could outlive a selection change.
func SelectedLeaf(outbound adapter.Outbound) adapter.Outbound {
	leaf, _ := Resolve(outbound)
	return leaf
}

func (s *HistoryStorage) LoadOutboundHistory(outbound adapter.Outbound, link string) *adapter.URLTestHistory {
	if leaf := SelectedLeaf(outbound); leaf != nil {
		return s.LoadURLTestHistory(leaf.Tag(), link)
	}
	return nil
}

func URLTestWithResult(ctx context.Context, link string, outbound adapter.Outbound) Result {
	r := Result{URL: NormalizeURL(link), Started: time.Now(), Via: outbound.Tag()}
	resolved, err := Resolve(outbound)
	if err != nil {
		r.Err = err
		return r
	}
	r.Via = resolved.Tag()
	ctx, metadata := adapter.ExtendContext(ctx)
	trace := adapter.NewOutboundTrace([]adapter.Outbound{resolved})
	metadata.OutboundTrace = trace
	r.Delay, r.Err = URLTest(ctx, r.URL, resolved)
	for _, node := range metadata.OutboundTrace.Chain() {
		if _, group := adapter.OutboundGroupFor(node); !group {
			r.Outbound = node.Tag()
			break
		}
	}
	return r
}
