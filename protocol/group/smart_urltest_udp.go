package group

import (
	"slices"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
)

type smartProbeHealth struct {
	tcp, udp  uint16
	tcpFailed bool
}

func (h smartProbeHealth) tier() int {
	if h.tcp == 0 {
		return 2
	}
	if h.udp == 0 {
		return 1
	}
	return 0
}

func (s *SmartURLTest) probeHealth(outbound adapter.Outbound) smartProbeHealth {
	var health smartProbeHealth
	leaf := urltest.SelectedLeaf(outbound)
	if leaf == nil {
		health.tcpFailed = true
		return health
	}
	if h := s.history.LoadURLTestObservation(leaf.Tag(), s.options.URL); h != nil {
		health.tcp = h.Delay
		health.tcpFailed = h.Delay == 0
	}
	if h := s.history.LoadUDPHistory(leaf.Tag(), *s.options.UDPTest); h != nil && h.Alive {
		health.udp = h.Delay
	}
	return health
}

func compareSmartHealth(a, b smartProbeHealth) int {
	if tier := a.tier() - b.tier(); tier != 0 {
		return tier
	}
	if tcp := int(a.tcp) - int(b.tcp); tcp != 0 {
		return tcp
	}
	return int(a.udp) - int(b.udp)
}

func smartChooseHealthy(tags []string, health map[string]smartProbeHealth, current string, tolerance uint16) string {
	best := ""
	for _, tag := range tags {
		if health[tag].tcpFailed {
			continue
		}
		if best == "" || compareSmartHealth(health[tag], health[best]) < 0 {
			best = tag
		}
	}
	if best == "" {
		return ""
	}
	old, candidate := health[current], health[best]
	if slices.Contains(tags, current) && !old.tcpFailed && old.tier() == candidate.tier() {
		if old.tcp == 0 {
			return current
		} // No measurement yet: preserve startup affinity.
		// Tolerance never retains a node whose UDP check failed or is unknown.
		// For equal TCP delays, UDP latency breaks the tie.
		if old.udp > 0 && old.tcp != candidate.tcp && uint32(old.tcp) <= uint32(candidate.tcp)+uint32(tolerance) {
			return current
		}
	}
	return best
}

func (s *SmartURLTest) usableAffinity(cached string, candidates []adapter.Outbound, best string) bool {
	var previous, preferred adapter.Outbound
	for _, candidate := range candidates {
		if candidate.Tag() == cached {
			previous = candidate
		}
		if candidate.Tag() == best {
			preferred = candidate
		}
	}
	if previous == nil || preferred == nil {
		return false
	}
	a, b := s.probeHealth(previous), s.probeHealth(preferred)
	if a.tcpFailed || a.tier() > b.tier() {
		return false
	}
	// A working TCP dial cannot validate UDP. Do not let its cached affinity
	// undo a switch away from an unanswered UDP probe.
	return cached == best || a.udp > 0 || a.tcp == 0
}
