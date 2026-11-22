package clashapi

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badjson"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func proxyRouter(server *Server, router adapter.Router) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProxies(server))

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName(server))
		r.Get("/", getProxy(server))
		r.Get("/delay", getProxyDelay(server))
		r.Put("/", updateProxy(server))
	})
	return r
}

func parseProxyName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProxyByName(server *Server) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := adapter.OutboundControlTag(server.ctx, r.Context().Value(CtxKeyProxyName).(string))
			proxy, exist := server.outbound.Outbound(name)
			if !exist || adapter.IsSubscriptionSource(proxy) {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			ctx := context.WithValue(r.Context(), CtxKeyProxy, proxy)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func proxyInfo(server *Server, detour adapter.Outbound, links ...string) *badjson.JSONObject {
	var info badjson.JSONObject
	info.Put("x-sing-box-revision", server.proxyRevision())
	nativeType := detour.Type()
	var clashType string
	switch nativeType {
	case C.TypeBlock:
		clashType = "Reject"
	default:
		clashType = C.ProxyDisplayName(nativeType)
	}
	info.Put("type", clashType)
	info.Put("name", adapter.OutboundDisplayName(server.ctx, detour.Tag()))
	if member, ok := detour.(adapter.ProvidedOutbound); ok {
		info.Put("provider-name", adapter.OutboundDisplayName(server.ctx, member.ProviderTag()))
	}
	info.Put("udp", common.Contains(detour.Network(), N.NetworkUDP))
	link := urltest.OutboundTestURL(detour)
	if len(links) > 0 && links[0] != "" {
		link = urltest.NormalizeURL(links[0])
	}
	info.Put("testUrl", urltest.OutboundTestURL(detour))
	leaf := urltest.SelectedLeaf(detour)
	var targets []string
	if leaf != nil {
		targets = server.urlTestHistory.URLs(leaf.Tag())
	}
	makeHistory := func(link string) []*adapter.URLTestHistory {
		if leaf == nil {
			return []*adapter.URLTestHistory{}
		}
		if h := server.urlTestHistory.LoadURLTestHistory(leaf.Tag(), link); h != nil {
			copy := *h
			copy.Outbound = adapter.OutboundDisplayName(server.ctx, copy.Outbound)
			return []*adapter.URLTestHistory{&copy}
		}
		return []*adapter.URLTestHistory{}
	}
	info.Put("history", makeHistory(link))
	extra := make(map[string]any)
	for _, target := range append(targets, link) {
		history := makeHistory(target)
		extra[target] = render.M{"history": history, "alive": len(history) > 0}
	}
	info.Put("extra", extra)
	if group, isGroup := adapter.OutboundGroupFor(detour); isGroup {
		_, selectable := group.(adapter.SelectableOutboundGroup)
		info.Put("selectable", selectable)
		var now string
		if selected := group.Selected(N.NetworkTCP); selected != nil {
			now = adapter.OutboundDisplayName(server.ctx, selected.Tag())
		}
		info.Put("now", now)
		all := adapter.OutboundDisplayNames(server.ctx, group.All())
		if all == nil {
			all = []string{}
		}
		info.Put("all", all)
	}
	return &info
}

func getProxies(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var proxyMap badjson.JSONObject
		outbounds := common.Filter(server.outbound.Outbounds(), func(detour adapter.Outbound) bool {
			return detour.Tag() != "" && !adapter.IsSubscriptionSource(detour)
		})
		outbounds = append(outbounds, common.Map(common.Filter(server.endpoint.Endpoints(), func(detour adapter.Endpoint) bool {
			return detour.Tag() != ""
		}), func(it adapter.Endpoint) adapter.Outbound {
			return it
		})...)

		allProxies := make([]string, 0, len(outbounds))

		for _, detour := range outbounds {
			switch detour.Type() {
			case C.TypeDirect, C.TypeBlock, C.TypeDNS:
				continue
			}
			allProxies = append(allProxies, adapter.OutboundDisplayName(server.ctx, detour.Tag()))
		}

		defaultTag := adapter.OutboundDisplayName(server.ctx, server.outbound.Default().Tag())
		// Direct/reject are normally omitted from GLOBAL, but its actual current
		// selection must remain a member for recursive dashboard resolution.
		if defaultTag != "" && !common.Contains(allProxies, defaultTag) {
			allProxies = append(allProxies, defaultTag)
		}

		sort.SliceStable(allProxies, func(i, j int) bool {
			return allProxies[i] == defaultTag
		})

		// fix clash dashboard
		proxyMap.Put("GLOBAL", map[string]any{
			"type":       "Fallback",
			"name":       "GLOBAL",
			"udp":        true,
			"history":    []*adapter.URLTestHistory{},
			"all":        allProxies,
			"now":        defaultTag,
			"selectable": false,
		})

		for i, detour := range outbounds {
			var tag string
			if detour.Tag() == "" {
				tag = F.ToString(i)
			} else {
				tag = adapter.OutboundDisplayName(server.ctx, detour.Tag())
			}
			proxyMap.Put(tag, proxyInfo(server, detour, r.URL.Query().Get("url")))
		}
		var responseMap badjson.JSONObject
		responseMap.Put("proxies", &proxyMap)
		response, err := responseMap.MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

func getProxy(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		response, err := proxyInfo(server, proxy, r.URL.Query().Get("url")).MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

type UpdateProxyRequest struct {
	Name string `json:"name"`
}

func updateProxy(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := UpdateProxyRequest{}
		if err := render.DecodeJSON(r.Body, &req); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		control, _ := adapter.OutboundGroupFor(proxy)
		selector, ok := control.(adapter.SelectableOutboundGroup)
		if !ok {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("Must be a Selector"))
			return
		}

		if !selector.SelectOutbound(adapter.OutboundControlTag(server.ctx, req.Name)) {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("Selector update error: not found"))
			return
		}

		render.NoContent(w, r)
	}
}

func groupContains(outboundManager adapter.OutboundManager, outboundGroup adapter.OutboundGroup, tag string, visited map[string]bool) bool {
	for _, memberTag := range outboundGroup.All() {
		if memberTag == tag {
			return true
		}
		member, loaded := outboundManager.Outbound(memberTag)
		if !loaded {
			continue
		}
		if group.RealTag(member, N.NetworkTCP) == tag {
			return true
		}
		memberGroup, isGroup := adapter.OutboundGroupFor(member)
		if !isGroup || visited[memberTag] {
			continue
		}
		visited[memberTag] = true
		if groupContains(outboundManager, memberGroup, tag, visited) {
			return true
		}
	}
	return false
}

func getProxyDelay(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		url := query.Get("url")
		timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 32)
		if err != nil || timeout <= 0 || timeout > 60000 {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		if url == "" {
			url = urltest.OutboundTestURL(proxy)
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Millisecond*time.Duration(timeout))
		defer cancel()
		var updates *adapter.GroupUpdates
		var revision uint64
		if server.ctx != nil {
			updates = service.PtrFromContext[adapter.GroupUpdates](server.ctx)
			if updates != nil {
				revision, _ = updates.State()
			}
		}

		measurement := probeWithDeadline(server.probeContext(ctx), url, proxy)
		delay, err := measurement.Delay, measurement.Err
		defer func() {
			if ctx.Err() == context.Canceled {
				return
			}
			realTag := measurement.Outbound
			if realTag == "" {
				realTag = measurement.Via
			}
			store := func() { measurement.Store(server.urlTestHistory) }

			if updates != nil {
				if !updates.Commit(revision, store) {
					return
				}
			} else {
				store()
			}
			for _, detour := range server.outbound.Outbounds() {
				control, _ := adapter.OutboundGroupFor(detour)
				urlTestGroup, isURLTestGroup := control.(adapter.URLTestGroup)
				if !isURLTestGroup {
					continue
				}
				if !groupContains(server.outbound, urlTestGroup, realTag, map[string]bool{detour.Tag(): true}) {
					continue
				}
				urlTestGroup.PerformUpdateCheck()
			}
		}()

		if ctx.Err() != nil {
			render.Status(r, http.StatusGatewayTimeout)
			render.JSON(w, r, ErrRequestTimeout)
			return
		}

		if err != nil || delay == 0 {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, newError("An error occurred in the delay test"))
			return
		}

		render.JSON(w, r, render.M{
			"delay": delay,
			"url":   measurement.URL,
			"node":  adapter.OutboundDisplayName(server.ctx, measurement.Outbound),
		})
	}
}
