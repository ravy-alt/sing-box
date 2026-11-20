package transport

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

func RegisterRace(registry *dns.TransportRegistry) {
	dns.RegisterTransport[option.RaceDNSServerOptions](registry, C.DNSTypeRace, NewRace)
}

var _ adapter.DNSTransportWithEnvironment = (*RaceTransport)(nil)

type RaceTransport struct {
	dns.TransportAdapter
	ctx     context.Context
	timeout time.Duration
	servers []adapter.DNSTransport // initialized before queries are accepted
}

func NewRace(ctx context.Context, _ log.ContextLogger, tag string, options option.RaceDNSServerOptions) (adapter.DNSTransport, error) {
	if len(options.Servers) < 2 {
		return nil, E.New("DNS race requires at least two servers")
	}
	seen := make(map[string]bool)
	for _, server := range options.Servers {
		if server == "" || server == tag || seen[server] {
			return nil, E.New("invalid or duplicate DNS race server: ", server)
		}
		seen[server] = true
	}
	timeout := time.Duration(options.Timeout)
	if timeout < 0 {
		return nil, E.New("DNS race timeout must be positive")
	}
	if timeout == 0 {
		timeout = C.DNSTimeout
	}
	return &RaceTransport{
		TransportAdapter: dns.NewTransportAdapter(C.DNSTypeRace, tag, slices.Clone(options.Servers)),
		ctx:              ctx, timeout: timeout,
	}, nil
}

func (t *RaceTransport) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		t.ctx = scope.Context()
	}
	if stage != adapter.StartStateStart {
		return nil
	}
	manager := service.FromContext[adapter.DNSTransportManager](t.ctx)
	if manager == nil {
		return E.New("missing DNS transport manager for race")
	}
	var servers []adapter.DNSTransport
	for _, tag := range t.Dependencies() {
		server, found := manager.Transport(tag)
		if !found {
			return E.New("DNS race server not found: ", tag)
		}
		if server.Type() == C.DNSTypeFakeIP {
			return E.New("DNS race cannot contain a fakeip server")
		}
		servers = append(servers, server)
	}
	t.servers = servers
	return nil
}

// Children are owned and reset by TransportManager, not by the race group.
func (t *RaceTransport) Reset() {}

func (t *RaceTransport) Environment() []string {
	environment := []string{"dns-race", t.timeout.String()}
	for _, server := range t.servers {
		environment = append(environment, server.Type(), server.Tag())
		if child, ok := server.(adapter.DNSTransportWithEnvironment); ok {
			environment = append(environment, child.Environment()...)
		}
	}
	return environment
}

func (t *RaceTransport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(t.servers) == 0 || message == nil || len(message.Question) != 1 {
		return nil, E.New("DNS race requires started servers and one question")
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	stop := context.AfterFunc(t.ctx, cancel)
	defer stop()
	if t.ctx.Err() != nil {
		return nil, t.ctx.Err()
	}
	type result struct {
		server   string
		response *mDNS.Msg
		err      error
	}
	results := make(chan result, len(t.servers))
	for _, server := range t.servers {
		// DNS transports may mutate IDs, questions and EDNS fields while packing.
		query := message.Copy()
		go func() {
			response, err := server.Exchange(ctx, query)
			results <- result{server.Tag(), response, err}
		}()
	}
	var negative *mDNS.Msg
	var failures []error
	canceled := func() (*mDNS.Msg, error) {
		if parent.Err() == nil && t.ctx.Err() == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && negative != nil {
			return negative, nil
		}
		return nil, errors.Join(append(failures, ctx.Err())...)
	}
	for range t.servers {
		select {
		case <-ctx.Done():
			return canceled()
		case r := <-results:
			if err := ctx.Err(); err != nil {
				return canceled()
			}
			if r.err == nil && r.response == nil {
				r.err = E.New("empty DNS response")
			}
			if r.err != nil {
				failures = append(failures, E.Cause(r.err, r.server))
				continue
			}
			response := r.response
			if response.Truncated {
				failures = append(failures, E.New(r.server, ": truncated DNS response"))
				continue
			}
			if response.Rcode == mDNS.RcodeSuccess {
				if raceHasAnswer(response, message.Question[0].Qtype) {
					return response, nil // cancel slower or failing private upstreams
				}
				negative = response
			} else if response.Rcode == mDNS.RcodeNameError {
				if negative == nil {
					negative = response
				}
			} else {
				failures = append(failures, E.Cause(dns.RcodeError(response.Rcode), r.server))
			}
		}
	}
	if negative != nil {
		return negative, nil
	}
	return nil, errors.Join(failures...)
}

func raceHasAnswer(response *mDNS.Msg, queryType uint16) bool {
	for _, answer := range response.Answer {
		if queryType != mDNS.TypeA && queryType != mDNS.TypeAAAA || answer.Header().Rrtype == queryType {
			return true
		}
	}
	return false
}

func (t *RaceTransport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(*mDNS.Msg, error)) {
	// Match Exchange's request ownership before handing work to another goroutine.
	if message != nil {
		message = message.Copy()
	}
	go func() {
		response, err := t.Exchange(ctx, message)
		callback(response, err)
	}()
}
