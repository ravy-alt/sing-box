package clashapi

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type providerProbe struct{ *namedTestOutbound }

func (*providerProbe) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func TestStandaloneProviderDashboardAndSingleProbe(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	names := adapter.NewOutboundNames([]string{"机场 / A", "direct"})
	tag := "subscription/source/node"
	names.Register([]adapter.OutboundName{{Tag: tag, Name: "香港 / 特殊%+?#"}}, nil)
	service.MustRegisterPtr(ctx, names)
	updates := adapter.NewGroupUpdates()
	service.MustRegisterPtr(ctx, updates)
	provider := &testProvider{name: "机场 / A", sourceOnly: true, all: []string{tag}}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Outbound, error) {
		if tag == provider.Tag() {
			return provider, nil
		}
		return &providerProbe{&namedTestOutbound{name: tag}}, nil
	})
	endpoints := endpoint.NewManager(nil)
	manager := outbound.NewManager(registry, endpoints, "direct")
	for _, name := range []string{"direct", provider.Tag(), tag, "unrelated"} {
		require.NoError(t, manager.Create(ctx, nil, logger.NOP(), name, "test", nil))
	}
	server := &Server{ctx: ctx, outbound: manager, endpoint: endpoints, urlTestHistory: urltest.NewHistoryStorage()}
	get := func(handler http.Handler, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	response := get(proxyRouter(server, nil), "/")
	require.NotContains(t, response.Body.String(), provider.Tag())
	require.Contains(t, response.Body.String(), "香港 / 特殊%+?#")
	require.Equal(t, 404, get(proxyRouter(server, nil), "/"+url.PathEscape(provider.Tag())).Code)
	require.NotContains(t, get(groupRouter(server), "/").Body.String(), provider.Tag())
	providers := proxyProviderRouter(server)
	response = get(providers, "/")
	require.Contains(t, response.Body.String(), provider.Tag())
	require.Contains(t, response.Body.String(), `"vehicleType":"HTTP"`)
	require.Contains(t, response.Body.String(), `"testUrl":`)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	path := "/" + url.PathEscape(provider.Tag()) + "/" + url.PathEscape(names.Name(tag)) + "/healthcheck?" + url.Values{"url": {target.URL}, "timeout": {"1000"}}.Encode()
	response = get(providers, path)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"delay":`)
	require.NotNil(t, server.urlTestHistory.LoadURLTestHistory(tag))
	require.Equal(t, 404, get(providers, "/"+url.PathEscape(provider.Tag())+"/unrelated/healthcheck?timeout=1000").Code)
	require.Equal(t, 400, get(providers, "/"+url.PathEscape(provider.Tag())+"/"+url.PathEscape(names.Name(tag))+"/healthcheck?timeout=-1").Code)
	server.urlTestHistory.DeleteURLTestHistory(tag)
	changedTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { updates.Notify(); w.WriteHeader(204) }))
	defer changedTarget.Close()
	path = "/" + url.PathEscape(provider.Tag()) + "/" + url.PathEscape(names.Name(tag)) + "/healthcheck?" + url.Values{"url": {changedTarget.URL}, "timeout": {"1000"}}.Encode()
	require.Equal(t, 200, get(providers, path).Code)
	require.Nil(t, server.urlTestHistory.LoadURLTestHistory(tag), "old-generation probes must not update new-generation history")
}
