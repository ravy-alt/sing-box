package adapter

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutboundNamesConcurrentRegistration(t *testing.T) {
	names := NewOutboundNames([]string{"香港-1", "direct"})
	var workers sync.WaitGroup
	for provider := range 12 {
		workers.Go(func() {
			entries := []OutboundName{{Tag: fmt.Sprintf("subscription/%d/hk", provider), Name: "香港"}, {Tag: fmt.Sprintf("subscription/%d/escaped", provider), Name: "node%41"}}
			for range 40 {
				names.Register(entries, nil)
				for _, entry := range entries {
					name := names.Name(entry.Tag)
					if names.Tag(name) != entry.Tag {
						t.Errorf("name %q changed ownership", name)
					}
				}
				_ = names.Snapshot()
			}
		})
	}
	workers.Wait()
	saved := names.Snapshot()
	require.Len(t, saved, 27) // 24 members, two static tags and GLOBAL.
	seen := make(map[string]string)
	for tag, name := range saved {
		require.NotContains(t, seen, name)
		seen[name] = tag
	}
	restored := NewOutboundNames([]string{"香港-1", "direct"})
	restored.Register(nil, saved)
	require.Equal(t, saved, restored.Snapshot())
}
