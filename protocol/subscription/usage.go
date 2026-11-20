package subscription

import (
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/adapter"
)

func parseSubscriptionInfo(header string) *adapter.SubscriptionInfo {
	var info adapter.SubscriptionInfo
	found := false
	for _, field := range strings.Split(header, ";") {
		key, raw, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "upload":
			info.Upload = value
		case "download":
			info.Download = value
		case "total":
			info.Total = value
		case "expire":
			info.Expire = value
		default:
			continue
		}
		found = true
	}
	if !found {
		return nil
	}
	return &info
}
