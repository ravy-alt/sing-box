package group

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	groupmembers "github.com/sagernet/sing-box/common/group"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterSelector(registry *outbound.Registry) {
	outbound.Register[option.SelectorOutboundOptions](registry, C.TypeSelector, NewSelector)
}

var (
	_ adapter.OutboundGroup = (*Selector)(nil)
	_ adapter.Referrer      = (*Selector)(nil)
)

type Selector struct {
	stopWatch func()
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       logger.ContextLogger
	members                      *groupmembers.Members
	mu                           sync.Mutex
	initialized                  bool
	bootstrapped                 bool
	hasImports                   bool
	resolved                     *groupmembers.Snapshot
	initialTag                   string
	tags                         []string
	defaultTag                   string
	outbounds                    map[string]adapter.Outbound
	selected                     adapter.Outbound
	history                      *urltest.HistoryStorage
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
}

func NewSelector(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SelectorOutboundOptions) (adapter.Outbound, error) {
	outbound := &Selector{
		Adapter:                      outbound.NewAdapter(C.TypeSelector, tag, nil, groupmembers.Dependencies(options.Outbounds, options.Import, options.EmptyFallback)),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		defaultTag:                   options.Default,
		hasImports:                   len(options.Import) > 0,
		outbounds:                    make(map[string]adapter.Outbound),
		history:                      service.PtrFromContext[urltest.HistoryStorage](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if err := groupmembers.ValidateBase(options.Outbounds, len(options.Import) > 0, options.EmptyFallback); err != nil {
		return nil, err
	}
	members, err := groupmembers.New(ctx, tag, func() []string { return options.Outbounds }, options.Import, options.EmptyFallback)
	if err != nil {
		return nil, err
	}
	outbound.members = members

	return outbound, nil
}

func (s *Selector) Network() []string {
	selected := s.Selected(N.NetworkTCP)
	if selected == nil {
		return nil
	}
	return selected.Network()
}

func (s *Selector) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		for i, tag := range s.tags {
			if _, loaded := s.outbound.Outbound(tag); !loaded {
				return E.New("outbound ", i, " not found: ", tag)
			}
		}
		if err := s.members.Validate(true); err != nil {
			return err
		}
		if cache := adapter.SelectionCacheFromContext(s.ctx); cache != nil {
			s.initialTag = cache.LoadSelected(s.Tag())
		}
		s.reconcile()
	case adapter.StartStateStarted:
		s.stopWatch = s.members.Watch(scope, s.reconcile)
		s.reconcile()
	}
	return nil
}

func (s *Selector) PauseUpdates() {
	if s.stopWatch != nil {
		s.stopWatch()
	}
}

func (s *Selector) ValidateImports(allowPending bool) error {
	if err := s.members.Validate(allowPending); err != nil {
		return err
	}
	if allowPending {
		return nil
	}
	s.mu.Lock()
	s.bootstrapped = true
	s.resolved = nil
	s.mu.Unlock()
	s.reconcile()
	if s.defaultTag != "" && !slices.Contains(s.members.Resolve().Tags, s.defaultTag) && !s.hasImports {
		return E.New("default outbound not found: ", s.defaultTag)
	}
	return nil
}

func (s *Selector) OptionalImportDependency(tag string) bool {
	return s.members.OptionalImportDependency(tag)
}

func (s *Selector) ImportsSource(tag string) bool { return s.members.ImportsSource(tag) }

func (s *Selector) reconcile() {
	members := s.members.Resolve()
	s.mu.Lock()
	// A slower reader must not overwrite a newer membership snapshot.
	current, _ := s.members.Updates().State()
	if current != members.Revision {
		s.mu.Unlock()
		return
	}
	if s.resolved == members {
		s.mu.Unlock()
		return
	}
	s.resolved = members
	outbounds := make(map[string]adapter.Outbound, len(members.Tags))
	for _, o := range members.Outbounds {
		outbounds[o.Tag()] = o
	}
	old := s.selected
	var selected adapter.Outbound
	if !s.initialized {
		selected = outbounds[s.initialTag]
		if selected == nil {
			selected = outbounds[s.defaultTag]
		}
	}
	if selected == nil && old != nil {
		selected = outbounds[old.Tag()]
	}
	if selected == nil {
		if !s.initialized {
			selected = outbounds[s.initialTag]
		}
		if selected == nil {
			selected = outbounds[s.defaultTag]
		}
		if selected == nil && len(members.Outbounds) > 0 {
			selected = members.Outbounds[0]
		}
	}
	if selected != nil && s.bootstrapped {
		s.initialized = true
	}
	s.outbounds, s.selected = outbounds, selected
	changed := old != nil && (selected == nil || selected.Tag() != old.Tag())
	s.mu.Unlock()
	if changed {
		s.interruptGroup.Interrupt(s.interruptExternalConnections)
		if s.history != nil {
			s.history.NotifyUpdated()
		}
	}
}

func (s *Selector) All() []string {
	s.reconcile()
	return slices.Clone(s.members.Resolve().Tags)
}

func (s *Selector) Selected(network string) adapter.Outbound {
	s.reconcile()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selected
}

func (s *Selector) AttachConnection(closer io.Closer) func() {
	return s.interruptGroup.Add(closer, true)
}

func (s *Selector) References() []string {
	if selected := s.Selected(N.NetworkTCP); selected != nil {
		return []string{selected.Tag()}
	}
	return nil
}

func (s *Selector) SelectOutbound(tag string) bool {
	s.reconcile()
	s.mu.Lock()
	detour, loaded := s.outbounds[tag]
	if !loaded {
		s.mu.Unlock()
		return false
	}
	old := s.selected
	s.selected = detour
	s.initialized = true
	s.mu.Unlock()
	if old != nil && old.Tag() == tag {
		return true
	}

	if s.Tag() != "" {
		cacheFile := adapter.SelectionCacheFromContext(s.ctx)
		if cacheFile != nil {
			err := cacheFile.StoreSelected(s.Tag(), tag)
			if err != nil {
				s.logger.Error("store selected: ", err)
			}
		}
	}
	s.interruptGroup.Interrupt(s.interruptExternalConnections)
	if s.history != nil {
		s.history.NotifyUpdated()
	}
	return true
}

func (s *Selector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	selected := s.Selected(network)
	if selected == nil {
		return nil, E.New("selector has no available outbound: ", s.Tag())
	}
	adapter.TraceOutbound(ctx, selected)
	conn, err := selected.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Selector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	selected := s.Selected(N.NetworkUDP)
	if selected == nil {
		return nil, E.New("selector has no available outbound: ", s.Tag())
	}
	adapter.TraceOutbound(ctx, selected)
	conn, err := selected.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func RealTag(detour adapter.Outbound, network string) string {
	for {
		// A per-destination group can successfully use a fallback instead of
		// Selected(). Attribute a probe of that group to the group itself.
		if _, dynamic := detour.(adapter.DynamicOutboundGroup); dynamic {
			return detour.Tag()
		}
		group, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			return detour.Tag()
		}
		detour = group.Selected(network)
		if detour == nil {
			return ""
		}
	}
}
