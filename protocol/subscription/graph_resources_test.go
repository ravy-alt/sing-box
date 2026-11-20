package subscription

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestEndpointDNSDependencyValidation(t *testing.T) {
	ctx := testContext()
	endpoint.Register[option.WireGuardEndpointOptions](service.FromContext[adapter.EndpointRegistry](ctx).(*endpoint.Registry), "wireguard", func(context.Context, adapter.Router, log.ContextLogger, string, option.WireGuardEndpointOptions) (adapter.Endpoint, error) {
		panic("parse only")
	})
	for _, address := range []string{"127.0.0.1", "peer.invalid"} {
		content := fmt.Sprintf(`{"dns":{"servers":[{"type":"udp","tag":"d","server":"1.1.1.1","detour":"wg"}]},"endpoints":[{"type":"wireguard","tag":"wg","peers":[{"address":%q}]}]}`, address)
		_, err := parseDocument(ctx, "p", []byte(content), nil)
		if address == "peer.invalid" {
			require.ErrorContains(t, err, "circular")
		} else {
			require.NoError(t, err)
		}
	}
}

type testCertificateProvider struct{ certificate.Adapter }

func (p *testCertificateProvider) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return nil, nil
}
func (p *testCertificateProvider) Start(adapter.StartStage, *adapter.Scope) error { return nil }

func TestCertificateProviderEndpointDependency(t *testing.T) {
	ctx := testContext()
	certificate.Register[option.TailscaleCertificateProviderOptions](service.FromContext[adapter.CertificateProviderRegistry](ctx).(*certificate.Registry), "test", func(ctx context.Context, _ log.ContextLogger, tag string, options option.TailscaleCertificateProviderOptions) (adapter.CertificateProviderService, error) {
		_, found := service.FromContext[adapter.EndpointManager](ctx).Get(options.Endpoint)
		if !found {
			return nil, fmt.Errorf("endpoint must be constructed before certificate provider: %s", options.Endpoint)
		}
		return &testCertificateProvider{certificate.NewAdapter("test", tag)}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"certificate_providers":[{"type":"test","tag":"cert","endpoint":"ep"}],"endpoints":[{"type":"test","tag":"ep"}]}`)
	}))
	defer server.Close()
	_, ctx = startBoxWithContext(t, ctx, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	require.NoError(t, s.Refresh(ctx))
	provider, found := service.FromContext[adapter.CertificateProviderManager](s.current.scope.Context()).Get("subscription-certificate_provider/sub/cert")
	require.True(t, found)
	require.Equal(t, "test", provider.Type())
}

func TestNativeImplicitTags(t *testing.T) {
	doc, err := parseDocument(testContext(), "p", []byte(`{"dns":{"servers":[{"type":"hosts"}]},"outbounds":[{"type":"test"},{"type":"test","detour":"0"}]}`), nil)
	require.NoError(t, err)
	require.Equal(t, []string{memberTag("p", "0"), memberTag("p", "1")}, doc.members)
	require.Equal(t, "subscription-dns/p/0", doc.options.DNS.Final)
	require.Equal(t, memberTag("p", "0"), doc.options.Outbounds[1].Options.(*testOptions).Detour)
}
