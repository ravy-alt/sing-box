package subscription

import "github.com/sagernet/sing-box/adapter"

func (s *Subscription) VisitDNSInspectors(visit func(string, adapter.DNSRouter) error) error {
	g := s.activeSnapshot()
	if g == nil {
		return nil
	}
	defer g.release()
	if g.dnsRouter != nil {
		if err := visit(s.Tag(), g.dnsRouter); err != nil {
			return err
		}
	}
	for _, child := range g.children {
		if err := child.VisitDNSInspectors(visit); err != nil {
			return err
		}
	}
	return nil
}
