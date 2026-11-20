package subscription

import (
	"context"
	"io"
	"net"
	"net/netip"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
)

// Retain every enclosing generation for the whole operation. Controls are
// stable handles too: refreshing a parent cannot leave an API holding a closed child.
func (m *member) control(action func(adapter.Outbound) error) error {
	g := m.owner.activeSnapshot()
	if g == nil {
		return net.ErrClosed
	}
	defer g.release()
	outbound, found := g.outbounds.Outbound(m.tag)
	if !found {
		return E.New("subscription member not found: ", m.tag)
	}
	if nested, ok := outbound.(*member); ok {
		return nested.control(action)
	}
	return action(outbound)
}

func (m *member) GroupControl() (adapter.OutboundGroup, bool) {
	var result adapter.OutboundGroup
	_ = m.control(func(outbound adapter.Outbound) error {
		if _, ok := outbound.(adapter.OutboundGroup); !ok {
			return nil
		}
		group := &managedGroup{m}
		if _, ok := outbound.(adapter.Subscription); ok {
			result = &managedSubscription{&managedSelector{group}}
		} else if _, ok := outbound.(adapter.SelectableOutboundGroup); ok {
			result = &managedSelector{group}
		} else if _, ok := outbound.(adapter.URLTestGroup); ok {
			result = &managedURLTest{group}
		} else {
			result = group
		}
		return nil
	})
	return result, result != nil
}

func (m *member) SubscriptionControl() (adapter.Subscription, bool) {
	group, _ := m.GroupControl()
	subscription, ok := group.(adapter.Subscription)
	return subscription, ok
}

type managedGroup struct{ *member }

func (g *managedGroup) All() (tags []string) {
	_ = g.control(func(outbound adapter.Outbound) error {
		if group, ok := outbound.(adapter.OutboundGroup); ok {
			tags = group.All()
		}
		return nil
	})
	return
}

func (g *managedGroup) Selected(network string) (selected adapter.Outbound) {
	_ = g.control(func(outbound adapter.Outbound) error {
		if group, ok := outbound.(adapter.OutboundGroup); ok {
			selected = group.Selected(network)
			if selected != nil && ownsMember(g.owner.Tag(), selected.Tag()) {
				selected = &member{owner: g.owner, tag: selected.Tag()}
			}
		}
		return nil
	})
	return
}
func (g *managedGroup) AttachConnection(conn io.Closer) func() { return g.owner.AttachConnection(conn) }

type managedSelector struct{ *managedGroup }

func (g *managedSelector) SelectOutbound(tag string) (selected bool) {
	_ = g.control(func(outbound adapter.Outbound) error {
		if group, ok := outbound.(adapter.SelectableOutboundGroup); ok {
			selected = group.SelectOutbound(tag)
		}
		return nil
	})
	if selected {
		g.owner.notify()
	}
	return
}

type managedURLTest struct{ *managedGroup }

func (g *managedGroup) test(ctx context.Context) (result map[string]uint16, err error) {
	err = g.control(func(outbound adapter.Outbound) error {
		group, ok := outbound.(adapter.URLTestGroup)
		if !ok {
			return E.New("subscription member is no longer a URL test group")
		}
		result, err = group.URLTest(ctx)
		return err
	})
	return
}
func (g *managedURLTest) URLTest(ctx context.Context) (map[string]uint16, error) { return g.test(ctx) }
func (g *managedURLTest) PerformUpdateCheck() {
	_ = g.control(func(outbound adapter.Outbound) error {
		if group, ok := outbound.(adapter.URLTestGroup); ok {
			group.PerformUpdateCheck()
		}
		return nil
	})
}

type managedSubscription struct{ *managedSelector }

func (g *managedSubscription) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.test(ctx)
}
func (g *managedSubscription) PerformUpdateCheck() {
	(&managedURLTest{g.managedGroup}).PerformUpdateCheck()
}
func (g *managedSubscription) subscription(action func(adapter.Subscription) error) error {
	return g.control(func(outbound adapter.Outbound) error {
		subscription, ok := outbound.(adapter.Subscription)
		if !ok {
			return E.New("subscription member is no longer a subscription")
		}
		return action(subscription)
	})
}
func (g *managedSubscription) Refresh(ctx context.Context) error {
	return g.subscription(func(s adapter.Subscription) error { return s.Refresh(ctx) })
}
func (g *managedSubscription) Restore() error {
	return g.subscription(func(s adapter.Subscription) error { return s.Restore() })
}
func (g *managedSubscription) Status() (status adapter.SubscriptionStatus) {
	status.Name = g.Tag()
	if err := g.subscription(func(s adapter.Subscription) error { status = s.Status(); return nil }); err != nil {
		status.LastError = err.Error()
	}
	return
}
func (g *managedSubscription) ProviderOutbounds() (outbounds []adapter.Outbound) {
	_ = g.subscription(func(s adapter.Subscription) error {
		for _, outbound := range s.ProviderOutbounds() {
			outbounds = append(outbounds, &member{g.owner, outbound.Tag()})
		}
		return nil
	})
	return
}
func (g *managedSubscription) ProviderOutbound(tag string) (outbound adapter.Outbound, found bool) {
	_ = g.subscription(func(s adapter.Subscription) error {
		if _, found = s.ProviderOutbound(tag); found {
			outbound = &member{g.owner, tag}
		}
		return nil
	})
	return
}

// Preferred-route rules may retain this handle across refreshes just like dialers.
func (m *member) PreferredDomain(metadata *adapter.InboundContext, domain string) (preferred bool) {
	_ = m.control(func(outbound adapter.Outbound) error {
		if routes, ok := outbound.(adapter.OutboundWithPreferredRoutes); ok {
			preferred = routes.PreferredDomain(metadata, domain)
		}
		return nil
	})
	return
}
func (m *member) PreferredAddress(metadata *adapter.InboundContext, address netip.Addr) (preferred bool) {
	_ = m.control(func(outbound adapter.Outbound) error {
		if routes, ok := outbound.(adapter.OutboundWithPreferredRoutes); ok {
			preferred = routes.PreferredAddress(metadata, address)
		}
		return nil
	})
	return
}
