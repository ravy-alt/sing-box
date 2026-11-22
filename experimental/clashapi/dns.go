package clashapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/miekg/dns"
)

func dnsRouter(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Get("/query", queryDNS(s))
	r.Get("/cache", dnsCache(s))
	r.Delete("/cache", deleteDNSCache(s))
	r.Get("/servers", dnsServers(s))
	r.Get("/rules", dnsRules(s))
	return r
}

func queryDNS(s *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		qTypeStr := strings.ToUpper(r.URL.Query().Get("type"))
		if qTypeStr == "" {
			qTypeStr = "A"
		}

		if _, valid := dns.IsDomainName(dns.Fqdn(name)); !valid || strings.TrimSpace(name) == "" || len(name) > 253 {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("invalid domain name"))
			return
		}
		qType, exist := dns.StringToType[qTypeStr]
		if !exist {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("invalid query type"))
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), C.DNSTimeout)
		defer cancel()

		msg := dns.Msg{}
		msg.SetQuestion(dns.Fqdn(name), qType)
		ctx = adapter.ContextWithDNSQueryInfo(ctx, adapter.DNSQueryInfo{Purpose: "manual"})
		ctx, trace := adapter.ContextWithDNSQueryTrace(ctx)
		var resp *dns.Msg
		found := false
		err := s.visitDNS(func(scope string, router adapter.DNSRouter) error {
			if scope != r.URL.Query().Get("scope") {
				return nil
			}
			found = true
			options := adapter.DNSQueryOptions{}
			if tag := r.URL.Query().Get("server"); tag != "" {
				lookup, ok := router.(interface {
					DNSTransport(string) (adapter.DNSTransport, bool)
				})
				if !ok {
					return fmt.Errorf("DNS server selection unavailable")
				}
				transport, exists := lookup.DNSTransport(tag)
				if !exists {
					return fmt.Errorf("DNS server not found: %s", tag)
				}
				options.Transport = transport
				options.DiagnosticRule = "manual-server"
			}
			var err error
			resp, err = router.Exchange(ctx, &msg, options)
			return err
		})
		if err == nil && !found {
			err = fmt.Errorf("DNS scope not found")
		}
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}

		responseData := render.M{
			"Status":   resp.Rcode,
			"Question": resp.Question,
			"Server":   trace.Result(resp).Server,
			"Trace":    trace.Result(resp),
			"TC":       resp.Truncated,
			"RD":       resp.RecursionDesired,
			"RA":       resp.RecursionAvailable,
			"AD":       resp.AuthenticatedData,
			"CD":       resp.CheckingDisabled,
		}

		rr2Json := func(rr dns.RR) render.M {
			header := rr.Header()
			return render.M{
				"name": header.Name,
				"type": header.Rrtype,
				"TTL":  header.Ttl,
				"data": rr.String()[len(header.String()):],
			}
		}

		if len(resp.Answer) > 0 {
			responseData["Answer"] = common.Map(resp.Answer, rr2Json)
		}
		if len(resp.Ns) > 0 {
			responseData["Authority"] = common.Map(resp.Ns, rr2Json)
		}
		if len(resp.Extra) > 0 {
			responseData["Additional"] = common.Map(resp.Extra, rr2Json)
		}

		render.JSON(w, r, responseData)
	}
}
