package subscription

import (
	"context"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/httpclient"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/logger"
)

type subscriptionAncestorsKey struct{}

type endpointView struct {
	*endpoint.Manager
	parent adapter.EndpointManager
}

func (m *endpointView) Get(tag string) (adapter.Endpoint, bool) {
	if result, found := m.Manager.Get(tag); found {
		return result, true
	}
	if m.parent != nil {
		return m.parent.Get(tag)
	}
	return nil, false
}

type outboundView struct {
	*outbound.Manager
	parent     adapter.OutboundManager
	owner      string
	defaultTag string
}

func (m *outboundView) Default() adapter.Outbound {
	if m.defaultTag != "" {
		result, _ := m.Outbound(m.defaultTag)
		return result
	}
	return m.Manager.Default()
}

func (m *outboundView) Outbound(tag string) (adapter.Outbound, bool) {
	if result, found := m.Manager.Outbound(tag); found {
		return result, true
	}
	if ownsMember(m.owner, tag) {
		return nil, false
	}
	if m.parent != nil {
		return m.parent.Outbound(tag)
	}
	return nil, false
}

type httpView struct {
	*httpclient.Manager
	parent          adapter.HTTPClientManager
	hasDefault      bool
	externalDefault string
	ctx             context.Context
	logger          logger.ContextLogger
}

func (m *httpView) HasClient(tag string) bool {
	if m.Manager.HasClient(tag) {
		return true
	}
	parent, ok := m.parent.(interface{ HasClient(string) bool })
	return ok && parent.HasClient(tag)
}

func (m *httpView) ResolveTransport(ctx context.Context, logger logger.ContextLogger, options option.HTTPClientOptions) (adapter.HTTPTransport, error) {
	if options.Tag != "" && !m.Manager.HasClient(options.Tag) && m.parent != nil {
		return m.parent.ResolveTransport(ctx, logger, options)
	}
	return m.Manager.ResolveTransport(ctx, logger, options)
}

func (m *httpView) NewDownloadTransport(ctx context.Context, logger logger.ContextLogger, options *option.HTTPClientOptions) (adapter.HTTPTransport, func() error, error) {
	if options == nil && m.externalDefault != "" {
		options = &option.HTTPClientOptions{Tag: m.externalDefault}
	}
	if m.parent != nil && (options == nil && !m.hasDefault || options != nil && options.Tag != "" && !m.Manager.HasClient(options.Tag)) {
		return m.parent.NewDownloadTransport(ctx, logger, options)
	}
	return m.Manager.NewDownloadTransport(ctx, logger, options)
}

func (m *httpView) DefaultTransport() adapter.HTTPTransport {
	if m.externalDefault != "" && m.parent != nil {
		transport, err := m.parent.ResolveTransport(m.ctx, m.logger, option.HTTPClientOptions{Tag: m.externalDefault})
		if err != nil {
			m.logger.Error("resolve default subscription HTTP client: ", err)
			return nil
		}
		return transport
	}
	if !m.hasDefault && m.parent != nil {
		return m.parent.DefaultTransport()
	}
	return m.Manager.DefaultTransport()
}

type certificateView struct {
	*certificate.Manager
	parent adapter.CertificateProviderManager
}

func (m *certificateView) Get(tag string) (adapter.CertificateProviderService, bool) {
	if result, found := m.Manager.Get(tag); found {
		return result, true
	}
	if m.parent != nil {
		return m.parent.Get(tag)
	}
	return nil, false
}

type namespaceView struct {
	local  adapter.NetworkNamespaceManager
	parent adapter.NetworkNamespaceManager
	tags   map[string]bool
}

func (m *namespaceView) ResolvePath(tag string) string {
	if m.tags[tag] {
		return m.local.ResolvePath(tag)
	}
	if m.parent != nil {
		return m.parent.ResolvePath(tag)
	}
	return tag
}

func (m *namespaceView) NamespaceHolderArgs() []string {
	if parent, ok := m.parent.(interface{ NamespaceHolderArgs() []string }); ok {
		return parent.NamespaceHolderArgs()
	}
	return nil
}

type routerView struct {
	adapter.Router
	ruleSets map[string]adapter.RuleSet
}

func (r *routerView) RuleSet(tag string) (adapter.RuleSet, bool) {
	if ruleSet, found := r.ruleSets[tag]; found {
		return ruleSet, true
	}
	return r.Router.RuleSet(tag)
}

func (g *snapshot) allOutbounds() []adapter.Outbound {
	result := g.outbounds.Outbounds()
	if g.endpoints != nil {
		for _, endpoint := range g.endpoints.Endpoints() {
			result = append(result, endpoint)
		}
	}
	return result
}

func (s *Subscription) createRuleSets(ctx context.Context, g *snapshot, doc *document, router *routerView) ([]adapter.SubscriptionBootstrapTask, error) {
	if doc.options.Route == nil {
		return nil, nil
	}
	var all []adapter.RuleSet
	var tasks []adapter.SubscriptionBootstrapTask
	for _, source := range doc.options.Route.RuleSet {
		for _, originalTag := range source.Tag {
			options := source
			tag := doc.resources["rule_set"][originalTag]
			options.LocalOptions.Path = strings.ReplaceAll(options.LocalOptions.Path, C.RuleSetTagPlaceholder, originalTag)
			options.RemoteOptions.URL = strings.ReplaceAll(options.RemoteOptions.URL, C.RuleSetTagPlaceholder, originalTag)
			options.RemoteOptions.InitialPath = strings.ReplaceAll(options.RemoteOptions.InitialPath, C.RuleSetTagPlaceholder, originalTag)
			ruleSet, err := R.NewRuleSet(ctx, s.logger, tag, options)
			if err != nil {
				return nil, err
			}
			g.scope.Add(ruleSet.Close)
			router.ruleSets[tag] = ruleSet
			all = append(all, ruleSet)
			tasks = append(tasks, adapter.SubscriptionBootstrapTask{Name: tag, Run: func(ctx context.Context) error {
				start := adapter.NewHTTPStartContext()
				defer start.Close()
				return ruleSet.StartContext(ctx, start)
			}})
		}
	}
	g.ruleSetUpdater = R.NewRuleSetUpdater(ctx, all)
	if g.ruleSetUpdater != nil {
		g.scope.Add(g.ruleSetUpdater.Close)
	}
	return tasks, nil
}
