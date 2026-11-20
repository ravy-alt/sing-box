package option

import (
	"strconv"

	E "github.com/sagernet/sing/common/exceptions"
)

// ValidateGroupOutbounds checks fixed group members against the declarations in
// one complete document. Call after merging host configuration files and before
// rewriting subscription tags. Runtime provider members are deliberately absent.
func ValidateGroupOutbounds(options Options) error {
	tagFor := func(tag string, index int) string {
		if tag == "" {
			return strconv.Itoa(index)
		}
		return tag
	}
	tags := make(map[string]bool, len(options.Outbounds)+len(options.Endpoints))
	for i, outbound := range options.Outbounds {
		tags[tagFor(outbound.Tag, i)] = true
	}
	for i, endpoint := range options.Endpoints {
		tags[tagFor(endpoint.Tag, i)] = true
	}
	for i, outbound := range options.Outbounds {
		var members []string
		switch group := outbound.Options.(type) {
		case *SelectorOutboundOptions:
			members = group.Outbounds
		case *URLTestOutboundOptions:
			members = group.Outbounds
		default:
			// Subscription.Outbounds selects from its downloaded document;
			// parseDocument validates that whitelist against that document.
			continue
		}
		for j, tag := range members {
			if !tags[tag] {
				return E.New("outbound[", i, "] (", tagFor(outbound.Tag, i), ").outbounds[", j, "]: ", tag,
					" is not declared in this configuration document; outbounds must reference local declarations, use import for subscription members")
			}
		}
	}
	return nil
}
