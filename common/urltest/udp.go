package urltest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
)

func NormalizeUDPTest(options option.UDPTestOptions) (option.UDPTestOptions, error) {
	options.Server = strings.ToLower(strings.TrimSpace(options.Server))
	if options.Server == "" {
		options.Server = "8.8.8.8"
	}
	if ip, err := netip.ParseAddr(options.Server); err == nil {
		options.Server = ip.String()
	} else if _, valid := dns.IsDomainName(options.Server); !valid || strings.ContainsAny(options.Server, ":/\\ \t\r\n") {
		return options, errors.New("udp_test: server must be an IP address or hostname without a port")
	}
	if options.ServerPort == 0 {
		options.ServerPort = 53
	}
	options.Domain = strings.ToLower(strings.TrimSpace(options.Domain))
	if options.Domain == "" {
		options.Domain = "example.com"
	}
	options.Domain = dns.Fqdn(options.Domain)
	if _, valid := dns.IsDomainName(options.Domain); !valid || len(options.Domain) > 254 || strings.ContainsAny(options.Domain, " /\\\t\r\n") {
		return options, errors.New("udp_test: invalid query domain")
	}
	if options.Timeout == 0 {
		options.Timeout = badoption.Duration(5 * time.Second)
	}
	if time.Duration(options.Timeout) < time.Millisecond || time.Duration(options.Timeout) > time.Minute {
		return options, errors.New("udp_test: timeout must be between 1ms and 1m")
	}
	return options, nil
}

func UDPTestKey(options option.UDPTestOptions) string {
	return net.JoinHostPort(options.Server, strconv.Itoa(int(options.ServerPort))) + "/" + options.Domain + "?timeout=" + strconv.FormatInt(time.Duration(options.Timeout).Milliseconds(), 10)
}

func OutboundUDPTestOptions(outbound adapter.Outbound) option.UDPTestOptions {
	if group, ok := adapter.OutboundGroupFor(outbound); ok {
		if configured, ok := group.(interface{ UDPTestOptions() option.UDPTestOptions }); ok {
			return configured.UDPTestOptions()
		}
	}
	options, _ := NormalizeUDPTest(option.UDPTestOptions{})
	return options
}

type UDPResult struct {
	Options  option.UDPTestOptions
	Started  time.Time
	Outbound string
	Delay    uint16
	Err      error
}

func (r UDPResult) Store(history *HistoryStorage) {
	if history == nil || r.Outbound == "" || errors.Is(r.Err, context.Canceled) {
		return
	}
	h := adapter.UDPTestHistory{Time: r.Started, Delay: r.Delay, Alive: r.Err == nil && r.Delay > 0, Outbound: r.Outbound, Server: r.Options.Server, Port: r.Options.ServerPort, Domain: r.Options.Domain, Timeout: time.Duration(r.Options.Timeout).Milliseconds()}
	if !h.Alive {
		h.Delay = 0
	}
	history.storeUDPHistory(r.Outbound, UDPTestKey(r.Options), h)
}

// A caller deadline bounds both waiting for capacity and a non-cooperative
// underlying dial. Its slot is retained until the operation actually exits.
var udpProbeSlots = make(chan struct{}, 256)

func UDPTestWithResult(parent context.Context, options option.UDPTestOptions, outbound adapter.Outbound) UDPResult {
	r := UDPResult{Options: options, Started: time.Now()}
	var err error
	r.Options, err = NormalizeUDPTest(options)
	if err != nil {
		r.Err = err
		return r
	}
	ctx, cancel := context.WithTimeout(adapter.ContextWithURLTest(parent), time.Duration(r.Options.Timeout))
	defer cancel()
	leaf, err := Resolve(outbound)
	if err != nil {
		r.Err = err
		return r
	}
	select {
	case udpProbeSlots <- struct{}{}:
	case <-ctx.Done():
		r.Err = ctx.Err()
		return r
	}
	if ctx.Err() != nil {
		<-udpProbeSlots
		r.Err = ctx.Err()
		return r
	}
	r.Started = time.Now()
	r.Outbound = leaf.Tag()
	done := make(chan UDPResult, 1)
	go func(result UDPResult) {
		defer func() { <-udpProbeSlots }()
		result.Delay, result.Err = exchangeUDP(ctx, result.Options, leaf)
		done <- result
	}(r)
	select {
	case measured := <-done:
		if ctx.Err() == nil {
			return measured
		}
	case <-ctx.Done():
	}
	r.Err = ctx.Err()
	return r
}

func exchangeUDP(ctx context.Context, options option.UDPTestOptions, outbound adapter.Outbound) (uint16, error) {
	start := time.Now()
	conn, err := outbound.DialContext(ctx, "udp", M.ParseSocksaddrHostPort(options.Server, options.ServerPort))
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return 0, err
	}
	if conn == nil {
		return 0, errors.New("UDP probe dialer returned no connection")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			return 0, err
		}
	}
	query := new(dns.Msg).SetQuestion(options.Domain, dns.TypeA)
	packet, err := query.Pack()
	if err != nil {
		return 0, err
	}
	if _, err = conn.Write(packet); err != nil {
		return 0, err
	}
	buffer := make([]byte, 65535)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return 0, err
		}
		var reply dns.Msg
		if reply.Unpack(buffer[:n]) != nil || !reply.Response || reply.Opcode != dns.OpcodeQuery || reply.Id != query.Id || len(reply.Question) != 1 {
			continue
		}
		question := reply.Question[0]
		if !strings.EqualFold(question.Name, options.Domain) || question.Qtype != dns.TypeA || question.Qclass != dns.ClassINET {
			continue
		}
		// TCP fallback would hide the exact UDP failure this probe measures.
		if reply.Truncated || reply.Rcode != dns.RcodeSuccess {
			return 0, fmt.Errorf("UDP DNS query unsuccessful: rcode=%d truncated=%v", reply.Rcode, reply.Truncated)
		}
		for _, answer := range reply.Answer {
			if _, ok := answer.(*dns.A); ok {
				return uint16(max(1, min(time.Since(start)/time.Millisecond, 65535))), nil
			}
		}
		return 0, errors.New("UDP DNS response has no A answer")
	}
}

func (s *HistoryStorage) storeUDPHistory(tag, key string, h adapter.UDPTestHistory) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return
	}
	target := s.targetLocked(tag, true)
	if target.udpHistory == nil {
		target.udpHistory = make(map[string]map[string]adapter.UDPTestHistory)
	}
	bucket := target.udpHistory[tag]
	if bucket == nil {
		bucket = make(map[string]adapter.UDPTestHistory)
		target.udpHistory[tag] = bucket
	}
	if previous, ok := bucket[key]; ok {
		if h.Time.Before(previous.Time) {
			return
		}
	} else if len(bucket) >= maxURLsPerNode {
		oldest := ""
		for k, v := range bucket {
			if oldest == "" || v.Time.Before(bucket[oldest].Time) {
				oldest = k
			}
		}
		delete(bucket, oldest)
	}
	bucket[key] = h
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadUDPHistory(tag string, options option.UDPTestOptions) *adapter.UDPTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	h, ok := s.targetLocked(tag, false).udpHistory[tag][UDPTestKey(options)]
	if !ok {
		return nil
	}
	return &h
}

func (s *HistoryStorage) UDPHistories(tag string) map[string]adapter.UDPTestHistory {
	result := make(map[string]adapter.UDPTestHistory)
	if s == nil {
		return result
	}
	s.access.RLock()
	defer s.access.RUnlock()
	for key, h := range s.targetLocked(tag, false).udpHistory[tag] {
		result[key] = h
	}
	return result
}

// Failed observations are retained separately from the legacy success-only
// history accessor, so unknown startup state is not confused with a dead node.
func (s *HistoryStorage) LoadURLTestObservation(tag, link string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	h, ok := s.targetLocked(tag, false).delayHistory[tag][NormalizeURL(link)]
	if !ok {
		return nil
	}
	return &h
}
