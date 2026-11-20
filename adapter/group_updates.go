package adapter

import "sync"

// GroupUpdates invalidates membership views without calling back into groups
// under a provider's publication lock. Readers reconcile lazily; workers use the
// same signal to update selection and health checks even when there is no traffic.
type GroupUpdates struct {
	mu       sync.Mutex
	revision uint64
	changed  chan struct{}
}

func NewGroupUpdates() *GroupUpdates { return &GroupUpdates{changed: make(chan struct{})} }

func (u *GroupUpdates) State() (uint64, <-chan struct{}) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.revision, u.changed
}

func (u *GroupUpdates) Notify() {
	u.mu.Lock()
	u.revision++
	close(u.changed)
	u.changed = make(chan struct{})
	u.mu.Unlock()
}

// Commit prevents a completed health check from writing results for an older
// membership revision. The callback must not call a provider or another group.
func (u *GroupUpdates) Commit(revision uint64, commit func()) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if revision != u.revision {
		return false
	}
	commit()
	return true
}

// GroupImportValidator is run after subscription bootstrap, when placeholder
// handles have become real groups and their initial membership is available.
type GroupImportValidator interface{ ValidateImports(allowPending bool) error }

// OptionalGroupImport marks previously admitted dynamic imports whose source
// may disappear. Fixed dependencies remain mandatory.
type OptionalGroupImport interface{ OptionalImportDependency(tag string) bool }
