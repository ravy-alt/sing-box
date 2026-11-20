package group

// ImportHistory contains immutable references from the previously published
// resource tree. It permits an unchanged import to remain empty while its source
// is absent, without admitting a newly configured missing source at startup.
type ImportHistory struct {
	References map[string]map[string]string
	// Cache aliases preserve namespacing after restart, but never relax initial
	// availability validation as a live, previously admitted reference does.
	Aliases map[string]map[string]string
}

func (h *ImportHistory) Resolve(owner, original string) string {
	if h == nil {
		return ""
	}
	if mapped := h.References[owner][original]; mapped != "" {
		return mapped
	}
	return h.Aliases[owner][original]
}

func (h *ImportHistory) Allows(owner, source string) bool {
	if h != nil {
		for _, mapped := range h.References[owner] {
			if mapped == source {
				return true
			}
		}
	}
	return false
}
