package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

type raceTestUpstream struct {
	dns.TransportAdapter
	exchange func(context.Context, *mDNS.Msg) (*mDNS.Msg, error)
}

func TestDNSRaceDiagnosticWinner(t *testing.T) {
	ctx, trace := adapter.ContextWithDNSQueryTrace(context.Background())
	group := raceTestGroup(t, time.Second,
		func(ctx context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
			<-ctx.Done()
			adapter.DNSQueryTraceFromContext(ctx).SetServer("loser")
			return nil, ctx.Err()
		},
		func(ctx context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
			adapter.DNSQueryTraceFromContext(ctx).SetServer("nested-winner")
			return raceTestReply(q, mDNS.RcodeSuccess, "192.0.2.1"), nil
		},
	)
	_, err := group.Exchange(ctx, new(mDNS.Msg).SetQuestion("example.com.", mDNS.TypeA))
	require.NoError(t, err)
	require.Equal(t, "nested-winner", trace.Server("race"))
}

func (*raceTestUpstream) Start(adapter.StartStage, *adapter.Scope) error { return nil }
func (*raceTestUpstream) Reset()                                         {}
func (u *raceTestUpstream) Exchange(ctx context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
	return u.exchange(ctx, q)
}
func (u *raceTestUpstream) ExchangeAsync(ctx context.Context, q *mDNS.Msg, cb func(*mDNS.Msg, error)) {
	go func() { response, err := u.Exchange(ctx, q); cb(response, err) }()
}

func raceTestReply(q *mDNS.Msg, rcode int, address string) *mDNS.Msg {
	response := new(mDNS.Msg).SetRcode(q, rcode)
	if address != "" {
		response.Answer = []mDNS.RR{&mDNS.A{
			Hdr: mDNS.RR_Header{Name: q.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60},
			A:   net.ParseIP(address).To4(),
		}}
	}
	return response
}

func raceTestGroup(t *testing.T, timeout time.Duration, functions ...func(context.Context, *mDNS.Msg) (*mDNS.Msg, error)) *RaceTransport {
	t.Helper()
	group := &RaceTransport{ctx: context.Background(), timeout: timeout}
	for index, function := range functions {
		group.servers = append(group.servers, &raceTestUpstream{
			TransportAdapter: dns.NewTransportAdapter("test", fmt.Sprint(index), nil),
			exchange:         function,
		})
	}
	return group
}

func TestDNSRaceConcurrentPositiveWinsAndCancelsLoser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{}, 4)
	allStarted := make(chan struct{})
	loserCanceled := make(chan struct{})
	var upstreams []func(context.Context, *mDNS.Msg) (*mDNS.Msg, error)
	for index := range 4 {
		upstreams = append(upstreams, func(ctx context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
			// Each upstream must receive its own mutable DNS message.
			q.Id = uint16(index)
			q.Question[0].Name = fmt.Sprintf("upstream-%d.example.", index)
			started <- struct{}{}
			select {
			case <-allStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if q.Id != uint16(index) || q.Question[0].Name != fmt.Sprintf("upstream-%d.example.", index) {
				return nil, errors.New("upstreams shared a mutable query")
			}
			switch index {
			case 0:
				return raceTestReply(q, mDNS.RcodeServerFailure, ""), nil
			case 1:
				return raceTestReply(q, mDNS.RcodeNameError, ""), nil
			case 2:
				select {
				case <-time.After(20 * time.Millisecond):
					return raceTestReply(q, mDNS.RcodeSuccess, "192.0.2.2"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			default:
				<-ctx.Done()
				close(loserCanceled)
				return nil, ctx.Err()
			}
		})
	}
	group := raceTestGroup(t, time.Second, upstreams...)
	query := new(mDNS.Msg).SetQuestion("original.example.", mDNS.TypeA)
	original := query.Copy()
	type result struct {
		response *mDNS.Msg
		err      error
	}
	finished := make(chan result, 1)
	go func() { r, err := group.Exchange(ctx, query); finished <- result{r, err} }()
	for range 4 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("all four upstreams must start concurrently")
		}
	}
	close(allStarted)
	r := <-finished
	require.NoError(t, r.err)
	require.Equal(t, "192.0.2.2", r.response.Answer[0].(*mDNS.A).A.String())
	require.Equal(t, original, query.Copy())
	select {
	case <-loserCanceled:
	case <-ctx.Done():
		t.Fatal("losing upstream was not canceled")
	}
}

func TestDNSRaceFailureAndNegativeResponses(t *testing.T) {
	for _, test := range []struct {
		name      string
		rcodes    []int
		wantRcode int
		wantError bool
	}{
		{"all failures", []int{mDNS.RcodeServerFailure, mDNS.RcodeRefused}, 0, true},
		{"negative with failure", []int{mDNS.RcodeNameError, mDNS.RcodeServerFailure}, mDNS.RcodeNameError, false},
		{"empty success with negative", []int{mDNS.RcodeSuccess, mDNS.RcodeNameError}, mDNS.RcodeSuccess, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var upstreams []func(context.Context, *mDNS.Msg) (*mDNS.Msg, error)
			for _, rcode := range test.rcodes {
				upstreams = append(upstreams, func(_ context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
					return raceTestReply(q, rcode, ""), nil
				})
			}
			group := raceTestGroup(t, time.Second, upstreams...)
			response, err := group.Exchange(context.Background(), new(mDNS.Msg).SetQuestion("example.", mDNS.TypeA))
			if test.wantError {
				require.Error(t, err)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantRcode, response.Rcode)
			}
		})
	}
}

func TestDNSRaceDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"timeout", "negative timeout", "parent canceled", "group stopped"} {
		t.Run(mode, func(t *testing.T) {
			query := new(mDNS.Msg).SetQuestion("example.", mDNS.TypeA)
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			lifetime, cancelLifetime := context.WithCancel(context.Background())
			defer cancelLifetime()
			release := make(chan struct{})
			defer close(release)
			group := raceTestGroup(t, 50*time.Millisecond,
				func(_ context.Context, q *mDNS.Msg) (*mDNS.Msg, error) {
					if mode == "timeout" {
						return nil, errors.New("upstream failed")
					}
					return raceTestReply(q, mDNS.RcodeNameError, ""), nil
				},
				func(context.Context, *mDNS.Msg) (*mDNS.Msg, error) {
					if mode == "parent canceled" {
						cancelParent()
					} else if mode == "group stopped" {
						cancelLifetime()
					}
					// A non-cooperative transport must not hold the caller open.
					<-release
					return nil, errors.New("late result")
				})
			group.ctx = lifetime
			finished := make(chan struct{})
			var response *mDNS.Msg
			var err error
			go func() { response, err = group.Exchange(parent, query); close(finished) }()
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("DNS race ignored timeout or cancellation")
			}
			if mode == "negative timeout" {
				require.NoError(t, err)
				require.Equal(t, mDNS.RcodeNameError, response.Rcode)
			} else if mode == "timeout" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestDNSRaceDependenciesAndConfiguration(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			registry := dns.NewTransportRegistry()
			RegisterRace(registry)
			dns.RegisterTransport[struct{}](registry, "test", func(_ context.Context, _ log.ContextLogger, tag string, _ struct{}) (adapter.DNSTransport, error) {
				return &raceTestUpstream{TransportAdapter: dns.NewTransportAdapter("test", tag, nil)}, nil
			})
			manager := dns.NewTransportManager(registry, nil, "race")
			ctx := service.ContextWith[adapter.DNSTransportManager](context.Background(), manager)
			ctx = service.ContextWith[option.DNSTransportOptionsRegistry](ctx, registry)
			logger := log.NewNOPFactory().Logger()
			var options option.DNSServerOptions
			require.NoError(t, json.UnmarshalContext(ctx, []byte(`{"type":"race","tag":"race","servers":["a","b"],"timeout":"5s"}`), &options))
			require.NoError(t, manager.Create(ctx, logger, options.Tag, options.Type, options.Options))
			require.NoError(t, manager.Create(ctx, logger, "a", "test", &struct{}{}))
			if mode == "valid" {
				require.NoError(t, manager.Create(ctx, logger, "b", "test", &struct{}{}))
			} else if mode == "cycle" {
				require.NoError(t, manager.Create(ctx, logger, "b", C.DNSTypeRace, &option.RaceDNSServerOptions{Servers: []string{"race", "a"}}))
			}
			scope := adapter.NewScope(ctx, logger)
			t.Cleanup(func() { require.NoError(t, scope.Close()) })
			require.NoError(t, manager.Start(adapter.StartStateInitialize, scope))
			err := manager.Start(adapter.StartStateStart, scope)
			if mode == "missing" {
				require.ErrorContains(t, err, "dependency[b] not found")
			} else if mode == "cycle" {
				require.ErrorContains(t, err, "circular server dependency")
			} else {
				require.NoError(t, err)
				group, _ := manager.Transport("race")
				require.Len(t, group.(*RaceTransport).servers, 2)
				require.Equal(t, []string{"a", "b"}, group.Dependencies())
			}
		})
	}
	for _, servers := range [][]string{nil, {"a"}, {"a", "a"}, {"race", "a"}, {"", "a"}} {
		_, err := NewRace(context.Background(), nil, "race", option.RaceDNSServerOptions{Servers: servers})
		require.Error(t, err)
	}
	_, err := NewRace(context.Background(), nil, "race", option.RaceDNSServerOptions{Servers: []string{"a", "b"}, Timeout: badoption.Duration(-time.Second)})
	require.Error(t, err)
}
