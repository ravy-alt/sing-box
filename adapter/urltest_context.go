package adapter

import "context"

type urlTestContextKey struct{}

// ContextWithURLTest marks the entire probe path, including nested groups and
// detours. Probe failures must never feed a group's business-traffic health
// counters or destination cache.
func ContextWithURLTest(ctx context.Context) context.Context {
	return context.WithValue(ctx, urlTestContextKey{}, true)
}

func IsURLTest(ctx context.Context) bool {
	probe, _ := ctx.Value(urlTestContextKey{}).(bool)
	return probe
}
