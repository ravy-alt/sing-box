package clashapi

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func proxyProviderRouter(server *Server) http.Handler {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		providers := make(map[string]any)
		for _, outbound := range server.outbound.Outbounds() {
			if provider, ok := adapter.SubscriptionFor(outbound); ok {
				providers[adapter.OutboundDisplayName(server.ctx, provider.Tag())] = providerInfo(server, provider, r.URL.Query().Get("url"))
			}
		}
		render.JSON(w, r, render.M{"providers": providers})
	})
	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProviderName, findProviderByName(server))
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			render.JSON(w, r, providerInfo(server, r.Context().Value(CtxKeyProvider).(adapter.Subscription), r.URL.Query().Get("url")))
		})
		r.Put("/", func(w http.ResponseWriter, r *http.Request) {
			if err := r.Context().Value(CtxKeyProvider).(adapter.Subscription).Refresh(r.Context()); err != nil {
				render.Status(r, http.StatusServiceUnavailable)
				render.JSON(w, r, newError(err.Error()))
				return
			}
			render.NoContent(w, r)
		})
		r.Get("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
			provider := r.Context().Value(CtxKeyProvider).(adapter.URLTestGroup)
			var err error
			query := r.URL.Query()
			if query.Has("url") || query.Has("timeout") {
				timeout := int64(15000)
				if query.Has("timeout") {
					timeout, err = strconv.ParseInt(query.Get("timeout"), 10, 32)
				}
				if err != nil || timeout <= 0 || timeout > 60000 {
					render.Status(r, http.StatusBadRequest)
					render.JSON(w, r, ErrBadRequest)
					return
				}
				link := query.Get("url")
				if link == "" {
					link = provider.(adapter.Subscription).Status().TestURL
				}
				ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Millisecond)
				defer cancel()
				_, err = server.testGroup(ctx, provider, link)
			} else {
				_, err = provider.URLTest(r.Context())
			}
			if err != nil {
				render.Status(r, http.StatusServiceUnavailable)
				render.JSON(w, r, newError(err.Error()))
				return
			}
			render.NoContent(w, r)
		})
		r.Get("/{proxy}/healthcheck", func(w http.ResponseWriter, r *http.Request) {
			provider := r.Context().Value(CtxKeyProvider).(adapter.Subscription)
			tag := adapter.OutboundControlTag(server.ctx, getEscapeParam(r, "proxy"))
			if !slices.Contains(provider.All(), tag) {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			outbound, found := provider.ProviderOutbound(tag)
			if !found {
				outbound, found = server.outbound.Outbound(tag)
			}
			if !found || adapter.IsSubscriptionSource(outbound) {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			getProxyDelay(server)(w, r.WithContext(context.WithValue(r.Context(), CtxKeyProxy, outbound)))
		})
	})
	return r
}

func providerInfo(server *Server, provider adapter.Subscription, links ...string) any {
	status := provider.Status()
	if len(links) == 0 || links[0] == "" {
		links = []string{status.TestURL}
	}
	proxies := make([]any, 0)
	for _, tag := range provider.All() {
		outbound, ok := provider.ProviderOutbound(tag)
		if !ok {
			outbound, ok = server.outbound.Outbound(tag)
		}
		if ok {
			proxies = append(proxies, proxyInfo(server, outbound, links...))
		}
	}
	info := render.M{"name": adapter.OutboundDisplayName(server.ctx, status.Name), "type": "Proxy", "vehicleType": "HTTP", "updatedAt": status.UpdatedAt, "ready": status.Ready, "generation": status.Generation, "lastError": status.LastError, "proxies": proxies, "testUrl": status.TestURL}
	if status.SubscriptionInfo != nil {
		info["subscriptionInfo"] = status.SubscriptionInfo
	}
	return info
}

func parseProviderName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), CtxKeyProviderName, getEscapeParam(r, "name"))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProviderByName(server *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			outbound, _ := server.outbound.Outbound(adapter.OutboundControlTag(server.ctx, r.Context().Value(CtxKeyProviderName).(string)))
			provider, ok := adapter.SubscriptionFor(outbound)
			if !ok {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), CtxKeyProvider, provider)))
		})
	}
}
