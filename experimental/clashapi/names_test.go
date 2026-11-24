package clashapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type namedTestOutbound struct {
	adapter.Outbound
	name string
}

func (o *namedTestOutbound) Tag() string            { return o.name }
func (o *namedTestOutbound) Type() string           { return "socks" }
func (o *namedTestOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *namedTestOutbound) Dependencies() []string { return nil }

func TestProxyPublicNameRoundTrip(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	names := adapter.NewOutboundNames([]string{"test", "香港"})
	service.MustRegisterPtr(ctx, names)
	first, second := "subscription/a/"+url.PathEscape("香港"), "subscription/b/"+url.PathEscape("東京 / 100%+?#")
	names.Register([]adapter.OutboundName{{Tag: first, Name: "香港"}, {Tag: second, Name: "東京 / 100%+?#"}}, nil)
	provider := &testProvider{all: []string{first, second}}
	objects := map[string]adapter.Outbound{
		"test": provider, "香港": &namedTestOutbound{name: "香港"},
		first: &namedTestOutbound{name: first}, second: &namedTestOutbound{name: second},
	}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Outbound, error) {
		return objects[tag], nil
	})
	endpoints := endpoint.NewManager(nil)
	manager := outbound.NewManager(registry, endpoints, "test")
	for _, tag := range []string{"test", "香港", first, second} {
		require.NoError(t, manager.Create(ctx, nil, logger.NOP(), tag, "test", nil))
	}
	server := &Server{ctx: ctx, outbound: manager, endpoint: endpoints, urlTestHistory: urltest.NewHistoryStorage()}
	handler := proxyRouter(server, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var result struct {
		Proxies map[string]struct {
			Name string   `json:"name"`
			All  []string `json:"all"`
		} `json:"proxies"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Contains(t, result.Proxies, "香港")
	require.Contains(t, result.Proxies, "香港-1")
	require.Contains(t, result.Proxies, "東京 / 100%+?#")
	require.Equal(t, []string{"香港-1", "東京 / 100%+?#"}, result.Proxies["test"].All)
	for _, tag := range []string{first, second} {
		name := names.Name(tag)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(name), nil))
		require.Equal(t, http.StatusOK, response.Code)
		var info map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &info))
		require.Equal(t, name, info["name"])
		body, err := json.Marshal(map[string]string{"name": name})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPut, "/test", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		require.Equal(t, http.StatusNoContent, response.Code)
		require.Equal(t, tag, provider.selected)
	}
}
