package clashapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/render"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"
)

func udpTestSettings(options option.UDPTestOptions) render.M {
	return render.M{"server": options.Server, "port": options.ServerPort, "domain": options.Domain, "timeout": time.Duration(options.Timeout).Milliseconds()}
}

func getProxyUDPDelay(server *Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		options := urltest.OutboundUDPTestOptions(proxy)
		query := r.URL.Query()
		if query.Has("server") {
			options.Server = query.Get("server")
		}
		if query.Has("domain") {
			options.Domain = query.Get("domain")
		}
		badRequest := func() { render.Status(r, http.StatusBadRequest); render.JSON(w, r, ErrBadRequest) }
		if query.Has("port") {
			port, err := strconv.ParseUint(query.Get("port"), 10, 16)
			if err != nil || port == 0 {
				badRequest()
				return
			}
			options.ServerPort = uint16(port)
		}
		if query.Has("timeout") {
			timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 64)
			if err != nil || timeout < 1 || timeout > 60000 {
				badRequest()
				return
			}
			options.Timeout = badoption.Duration(time.Duration(timeout) * time.Millisecond)
		}
		var err error
		options, err = urltest.NormalizeUDPTest(options)
		if err != nil {
			badRequest()
			return
		}
		ctx := server.probeContext(r.Context())
		var updates *adapter.GroupUpdates
		var revision uint64
		if server.ctx != nil {
			updates = service.PtrFromContext[adapter.GroupUpdates](server.ctx)
			if updates != nil {
				revision, _ = updates.State()
			}
		}
		measurement := urltest.UDPTestWithResult(ctx, options, proxy)
		if r.Context().Err() == context.Canceled {
			return
		}
		if measurement.Outbound == "" {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, newError("UDP probe could not start"))
			return
		}
		commit := func() { measurement.Store(server.urlTestHistory) }
		if updates != nil {
			if !updates.Commit(revision, commit) {
				render.Status(r, http.StatusConflict)
				render.JSON(w, r, newError("Proxy changed during UDP test; retry"))
				return
			}
		} else {
			commit()
		}
		if errors.Is(measurement.Err, urltest.ErrUDPTestUnsupported) {
			render.JSON(w, r, render.M{
				"skipped":  true,
				"reason":   "udp_not_supported",
				"outbound": adapter.OutboundDisplayName(server.ctx, measurement.Outbound),
			})
			return
		}
		// Rank against current leaf histories, not a possibly stale group alias.
		for _, outbound := range server.outbound.Outbounds() {
			group, _ := adapter.OutboundGroupFor(outbound)
			if update, ok := group.(adapter.URLTestGroup); ok {
				update.PerformUpdateCheck()
			}
		}
		history := server.urlTestHistory.LoadUDPHistory(measurement.Outbound, options)
		if history == nil {
			render.Status(r, http.StatusConflict)
			render.JSON(w, r, newError("UDP result is no longer current"))
			return
		}
		history.Outbound = adapter.OutboundDisplayName(server.ctx, history.Outbound)
		render.JSON(w, r, history)
	}
}
