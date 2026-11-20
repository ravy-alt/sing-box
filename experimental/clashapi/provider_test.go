package clashapi

import (
	"context"
	"errors"
	"io"
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

	"github.com/stretchr/testify/require"
)

type testProvider struct {
	name string
	adapter.Outbound
	calls, checks int
	fail          bool
	selected      string
	all           []string
}

func (p *testProvider) Tag() string {
	if p.name != "" {
		return p.name
	}
	return "test"
}
func (p *testProvider) Type() string                                     { return "subscription" }
func (p *testProvider) Network() []string                                { return []string{"tcp", "udp"} }
func (p *testProvider) Dependencies() []string                           { return nil }
func (p *testProvider) All() []string                                    { return p.all }
func (p *testProvider) Selected(string) adapter.Outbound                 { return nil }
func (p *testProvider) AttachConnection(io.Closer) func()                { return func() {} }
func (p *testProvider) ProviderOutbounds() []adapter.Outbound            { return nil }
func (p *testProvider) ProviderOutbound(string) (adapter.Outbound, bool) { return nil, false }
func (p *testProvider) Restore() error                                   { return nil }
func (p *testProvider) Refresh(context.Context) error {
	p.calls++
	if p.fail {
		return errors.New("download failed")
	}
	return nil
}
func (p *testProvider) Status() adapter.SubscriptionStatus {
	return adapter.SubscriptionStatus{Name: p.Tag(), Ready: true}
}
func (p *testProvider) URLTest(context.Context) (map[string]uint16, error) {
	p.checks++
	return map[string]uint16{}, nil
}
func (p *testProvider) PerformUpdateCheck()            {}
func (p *testProvider) SelectOutbound(tag string) bool { p.selected = tag; return true }

func TestSubscriptionProviderAPI(t *testing.T) {
	provider := new(testProvider)
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(context.Context, adapter.Router, log.ContextLogger, string, option.StubOptions) (adapter.Outbound, error) {
		return provider, nil
	})
	manager := outbound.NewManager(registry, endpoint.NewManager(nil), "")
	require.NoError(t, manager.Create(context.Background(), nil, logger.NOP(), "test", "test", nil))
	server := &Server{outbound: manager, urlTestHistory: urltest.NewHistoryStorage()}
	handler := proxyProviderRouter(server)
	request := func(method, path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		return response
	}
	response := request(http.MethodGet, "/")
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), `"ready":true`)
	response = request(http.MethodGet, "/test")
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), `"name":"test"`)
	require.Equal(t, 204, request(http.MethodPut, "/test").Code)
	require.Equal(t, 1, provider.calls)
	require.Equal(t, 204, request(http.MethodGet, "/test/healthcheck").Code)
	require.Equal(t, 1, provider.checks)
	require.Equal(t, 404, request(http.MethodGet, "/absent").Code)
	provider.fail = true
	require.Equal(t, 503, request(http.MethodPut, "/test").Code)
	req := httptest.NewRequest(http.MethodPut, "/test", strings.NewReader(`{"name":"node"}`))
	req.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	proxyRouter(server, nil).ServeHTTP(response, req)
	require.Equal(t, 204, response.Code)
	require.Equal(t, "node", provider.selected)
}

// Provider members expose controls without implementing routing group interfaces.
type testControlledMember struct {
	adapter.Outbound
	provider *testProvider
}

func (m *testControlledMember) GroupControl() (adapter.OutboundGroup, bool) { return m.provider, true }
func (m *testControlledMember) SubscriptionControl() (adapter.Subscription, bool) {
	return m.provider, true
}

func TestNestedProviderControlAPI(t *testing.T) {
	provider := &testProvider{name: "subscription/root/child"}
	member := &testControlledMember{Outbound: provider, provider: provider}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(context.Context, adapter.Router, log.ContextLogger, string, option.StubOptions) (adapter.Outbound, error) {
		return member, nil
	})
	manager := outbound.NewManager(registry, endpoint.NewManager(nil), "")
	require.NoError(t, manager.Create(context.Background(), nil, logger.NOP(), provider.Tag(), "test", nil))
	server := &Server{outbound: manager, urlTestHistory: urltest.NewHistoryStorage()}
	handler := proxyProviderRouter(server)
	request := func(method, path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		return response
	}
	path := "/" + url.PathEscape(provider.Tag())
	require.Contains(t, request(http.MethodGet, "/").Body.String(), provider.Tag())
	require.Equal(t, 200, request(http.MethodGet, path).Code)
	require.Equal(t, 204, request(http.MethodPut, path).Code)
	require.Equal(t, 1, provider.calls)
	require.Equal(t, 204, request(http.MethodGet, path+"/healthcheck").Code)
	require.Equal(t, 1, provider.checks)
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"name":"node"}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	proxyRouter(server, nil).ServeHTTP(response, req)
	require.Equal(t, 204, response.Code)
	require.Equal(t, "node", provider.selected)
}

func TestProviderAPIIncludesImportedMembers(t *testing.T) {
	provider := &testProvider{name: "aggregate", all: []string{"external"}}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Outbound, error) {
		if tag == provider.Tag() {
			return &testControlledMember{Outbound: provider, provider: provider}, nil
		}
		return &testProvider{name: tag}, nil
	})
	manager := outbound.NewManager(registry, endpoint.NewManager(nil), "")
	for _, tag := range []string{"aggregate", "external"} {
		require.NoError(t, manager.Create(context.Background(), nil, logger.NOP(), tag, "test", nil))
	}
	server := &Server{outbound: manager, urlTestHistory: urltest.NewHistoryStorage()}
	response := httptest.NewRecorder()
	proxyProviderRouter(server).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/aggregate", nil))
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), `"name":"external"`)
	response = httptest.NewRecorder()
	groupRouter(server).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/aggregate", nil))
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), `"all":["external"]`)
	provider.all = nil
	response = httptest.NewRecorder()
	proxyProviderRouter(server).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/aggregate", nil))
	require.Contains(t, response.Body.String(), `"proxies":[]`)
}
