package clashapi

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
)

// Shared-session dialers may finish establishing a transport after the caller
// cancels. Keep their slots until they actually return, but bound the API wait
// by the request deadline. Repeated timed-out requests cannot spawn unbounded
// background dials. Waiting for local capacity is not a failure of the node.
var apiProbeSlots = make(chan struct{}, 256)

func probeWithDeadline(ctx context.Context, link string, outbound adapter.Outbound) urltest.Result {
	result := urltest.Result{URL: urltest.NormalizeURL(link), Started: time.Now(), Via: outbound.Tag()}
	leaf, err := urltest.Resolve(outbound)
	if err != nil {
		result.Err = err
		return result
	}
	select {
	case apiProbeSlots <- struct{}{}:
	case <-ctx.Done():
		result.Err = ctx.Err()
		return result
	}
	if err := ctx.Err(); err != nil {
		<-apiProbeSlots
		result.Err = err
		return result
	}
	result.Started = time.Now()
	result.Via, result.Outbound = leaf.Tag(), leaf.Tag()
	done := make(chan urltest.Result, 1)
	go func() {
		measured := urltest.URLTestWithResult(ctx, link, leaf)
		measured.Started = result.Started
		<-apiProbeSlots
		done <- measured
	}()
	select {
	case measured := <-done:
		if ctx.Err() == nil {
			return measured
		}
	case <-ctx.Done():
	}
	result.Err = ctx.Err()
	return result
}
