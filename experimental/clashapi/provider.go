package clashapi

import (
	"context"
	"net/http"

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
				providers[provider.Tag()] = providerInfo(server, provider)
			}
		}
		render.JSON(w, r, render.M{"providers": providers})
	})
	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProviderName, findProviderByName(server))
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			render.JSON(w, r, providerInfo(server, r.Context().Value(CtxKeyProvider).(adapter.Subscription)))
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
			if _, err := provider.URLTest(r.Context()); err != nil {
				render.Status(r, http.StatusServiceUnavailable)
				render.JSON(w, r, newError(err.Error()))
				return
			}
			render.NoContent(w, r)
		})
	})
	return r
}

func providerInfo(server *Server, provider adapter.Subscription) any {
	status := provider.Status()
	proxies := make([]any, 0)
	for _, tag := range provider.All() {
		outbound, ok := provider.ProviderOutbound(tag)
		if !ok {
			outbound, ok = server.outbound.Outbound(tag)
		}
		if ok {
			proxies = append(proxies, proxyInfo(server, outbound))
		}
	}
	return render.M{"name": status.Name, "type": "Proxy", "vehicleType": "HTTP", "updatedAt": status.UpdatedAt, "ready": status.Ready, "generation": status.Generation, "lastError": status.LastError, "proxies": proxies}
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
			outbound, _ := server.outbound.Outbound(r.Context().Value(CtxKeyProviderName).(string))
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
