package group

import (
	"context"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

type filter struct {
	option.GroupImport
	include, exclude []*regexp.Regexp
}

type Snapshot struct {
	Revision  uint64
	Tags      []string
	Outbounds []adapter.Outbound
}

// Members resolves only immediate source members. A source with its own imports
// resolves those first, so arbitrarily deep import chains remain live. Group
// members themselves retain their identity and selection behavior.
type Members struct {
	manager adapter.OutboundManager
	updates *adapter.GroupUpdates
	owner   string
	base    func() []string
	imports []filter
	history *ImportHistory
	mu      sync.Mutex
	cached  *Snapshot
}

func New(ctx context.Context, owner string, base func() []string, imports []option.GroupImport) (*Members, error) {
	updates := service.PtrFromContext[adapter.GroupUpdates](ctx)
	if updates == nil {
		updates = adapter.NewGroupUpdates()
		service.MustRegisterPtr(ctx, updates)
	}
	m := &Members{manager: service.FromContext[adapter.OutboundManager](ctx), updates: updates, owner: owner, base: base, history: service.PtrFromContext[ImportHistory](ctx)}
	for i, spec := range imports {
		if spec.Group == "" {
			return nil, E.New("import[", i, "]: missing group")
		}
		if spec.Group == owner {
			return nil, E.New("circular group import: ", owner)
		}
		if spec.Match != "" && spec.Match != "name" && spec.Match != "tag" {
			return nil, E.New("import[", i, "]: invalid match: ", spec.Match)
		}
		f := filter{GroupImport: spec}
		for _, field := range []struct {
			name     string
			patterns []string
			target   *[]*regexp.Regexp
		}{
			{"include", spec.Include, &f.include}, {"exclude", spec.Exclude, &f.exclude},
		} {
			for _, pattern := range field.patterns {
				r, err := regexp.Compile(pattern)
				if err != nil {
					return nil, E.Cause(err, "import[", i, "].", field.name)
				}
				*field.target = append(*field.target, r)
			}
		}
		m.imports = append(m.imports, f)
	}
	return m, nil
}

func Dependencies(base []string, imports []option.GroupImport) []string {
	deps := slices.Clone(base)
	for _, spec := range imports {
		if !slices.Contains(deps, spec.Group) {
			deps = append(deps, spec.Group)
		}
	}
	return deps
}

func ValidateBase(base []string, hasImports bool) error {
	if len(base) == 0 && !hasImports {
		return E.New("missing tags or import")
	}
	for i, tag := range base {
		if slices.Contains(base[:i], tag) {
			return E.New("duplicate tag: ", tag)
		}
	}
	return nil
}

func (m *Members) Validate(allowPending bool) error {
	for _, spec := range m.imports {
		source, found := m.manager.Outbound(spec.Group)
		if !found {
			if m.OptionalImportDependency(spec.Group) {
				continue
			}
			return E.New("import group not found: ", spec.Group)
		}
		if pending, ok := source.(adapter.ProvidedOutbound); ok && !pending.Available() && allowPending {
			continue
		}
		if _, ok := adapter.OutboundGroupFor(source); !ok {
			if m.OptionalImportDependency(spec.Group) {
				continue
			}
			return E.New("import source is not a group: ", spec.Group)
		}
	}
	return nil
}

func (m *Members) OptionalImportDependency(tag string) bool {
	if !m.history.Allows(m.owner, tag) || slices.Contains(m.base(), tag) {
		return false
	}
	for _, spec := range m.imports {
		if spec.Group == tag {
			return true
		}
	}
	return false
}

func (m *Members) Updates() *adapter.GroupUpdates { return m.updates }

func (m *Members) Resolve() *Snapshot {
	for {
		revision, _ := m.updates.State()
		m.mu.Lock()
		previous := m.cached
		m.mu.Unlock()
		if previous != nil && previous.Revision == revision {
			return previous
		}
		next := &Snapshot{Revision: revision}
		seen := make(map[string]bool)
		previousMembers := make(map[string]adapter.Outbound)
		if previous != nil {
			for _, o := range previous.Outbounds {
				previousMembers[o.Tag()] = o
			}
		}
		add := func(tag string, f *filter) {
			if seen[tag] || tag == m.owner {
				return
			}
			o, found := m.manager.Outbound(tag)
			if !found {
				return
			}
			if f != nil && !f.matches(o) {
				return
			}
			if old, ok := previousMembers[tag].(adapter.ProvidedOutbound); ok {
				o = old
			}
			seen[tag] = true
			next.Tags = append(next.Tags, tag)
			next.Outbounds = append(next.Outbounds, o)
		}
		for _, tag := range m.base() {
			add(tag, nil)
		}
		for _, spec := range m.imports {
			source, found := m.manager.Outbound(spec.Group)
			if !found {
				continue
			}
			group, ok := adapter.OutboundGroupFor(source)
			if !ok {
				continue
			}
			for _, tag := range group.All() {
				add(tag, &spec)
			}
		}
		m.mu.Lock()
		current, _ := m.updates.State()
		if current != revision {
			m.mu.Unlock()
			continue
		}
		m.cached = next
		m.mu.Unlock()
		return next
	}
}

func (f *filter) matches(o adapter.Outbound) bool {
	name := o.Tag()
	if f.Match != "tag" && strings.HasPrefix(name, "subscription/") {
		parts := strings.SplitN(strings.TrimPrefix(name, "subscription/"), "/", 2)
		if len(parts) == 2 {
			if decoded, err := url.PathUnescape(parts[1]); err == nil {
				name = decoded
			}
		}
	}
	match := func(patterns []*regexp.Regexp) bool {
		for _, pattern := range patterns {
			if pattern.MatchString(name) {
				return true
			}
		}
		return false
	}
	return (len(f.include) == 0 || match(f.include)) && !match(f.exclude) && (len(f.Types) == 0 || slices.Contains(f.Types, o.Type()))
}

// Watch coalesces updates. Resolve reads the latest revision rather than relying
// on delivery order; a read immediately after Refresh is therefore fresh too.
func (m *Members) Watch(scope *adapter.Scope, update func()) func() {
	ctx, cancel := context.WithCancel(scope.Context())
	done := make(chan struct{})
	stop := func() { cancel(); <-done }
	scope.Add(func() error { stop(); return nil })
	_, changed := m.updates.State()
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-changed:
				_, changed = m.updates.State()
				if ctx.Err() == nil {
					update()
				}
			}
		}
	}()
	return stop
}
