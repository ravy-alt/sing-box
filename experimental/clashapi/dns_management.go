package clashapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/ws"
	"github.com/sagernet/ws/wsutil"

	"github.com/go-chi/render"
)

func (s *Server) visitDNS(visit func(string, adapter.DNSRouter) error) error {
	if err := visit("", s.dnsRouter); err != nil {
		return err
	}
	if s.outbound != nil {
		for _, outbound := range s.outbound.Outbounds() {
			if visitor, ok := outbound.(adapter.DNSInspectorVisitor); ok {
				if err := visitor.VisitDNSInspectors(visit); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Server) dnsEntries() ([]adapter.DNSCacheEntry, error) {
	entries := []adapter.DNSCacheEntry{}
	err := s.visitDNS(func(scope string, router adapter.DNSRouter) error {
		inspector, ok := router.(adapter.DNSCacheInspector)
		if !ok {
			return fmt.Errorf("DNS cache inspection unavailable")
		}
		current, err := inspector.DNSCacheEntries()
		if err != nil {
			return err
		}
		for _, entry := range current {
			entry.Scope = scope
			entry.ID = base64.RawURLEncoding.EncodeToString([]byte(scope + "\x00" + entry.ID))
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

type dnsCacheFrame struct {
	Full     bool                    `json:"full"`
	Revision uint64                  `json:"revision"`
	Entries  []adapter.DNSCacheEntry `json:"entries"`
	Removed  []string                `json:"removed"`
}

func dnsCache(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entries, err := s.dnsEntries()
		if err != nil {
			dnsAPIError(w, r, err)
			return
		}
		if r.Header.Get("Upgrade") != "websocket" {
			render.JSON(w, r, dnsCacheFrame{Full: true, Entries: entries, Removed: []string{}})
			return
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		closed := make(chan struct{})
		pings := make(chan []byte, 4)
		// One reader detects browser close immediately; it never writes, so all
		// outgoing frames remain serialized by the handler. No unbounded reads.
		go func() {
			defer close(closed)
			reader := wsutil.NewReader(conn, ws.StateServerSide)
			reader.MaxFrameSize = 4096
			for {
				header, err := reader.NextFrame()
				if err != nil || header.OpCode == ws.OpClose {
					return
				}
				if header.OpCode == ws.OpPing {
					payload, err := io.ReadAll(reader)
					if err != nil {
						return
					}
					select {
					case pings <- payload:
					default:
						return
					}
				} else if _, err := io.Copy(io.Discard, reader); err != nil {
					return
				}
			}
		}()
		previous := make(map[string][]byte)
		var revision uint64
		send := func(entries []adapter.DNSCacheEntry) error {
			frame := dnsCacheFrame{Full: revision == 0, Revision: revision + 1, Entries: []adapter.DNSCacheEntry{}, Removed: []string{}}
			current := make(map[string][]byte, len(entries))
			for _, entry := range entries {
				encoded, err := json.Marshal(entry)
				if err != nil {
					return err
				}
				current[entry.ID] = encoded
				if !bytes.Equal(encoded, previous[entry.ID]) {
					frame.Entries = append(frame.Entries, entry)
				}
			}
			for id := range previous {
				if _, exists := current[id]; !exists {
					frame.Removed = append(frame.Removed, id)
				}
			}
			encoded, err := json.Marshal(frame)
			if err != nil {
				return err
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err = wsutil.WriteServerText(conn, encoded); err != nil {
				return err
			}
			previous, revision = current, frame.Revision
			return nil
		}
		if send(entries) != nil {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-r.Context().Done():
				return
			case <-closed:
				return
			case payload := <-pings:
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if wsutil.WriteServerMessage(conn, ws.OpPong, payload) != nil {
					return
				}
			case <-ticker.C:
				entries, err = s.dnsEntries()
				if err != nil || send(entries) != nil {
					return
				}
			}
		}
	}
}

func deleteDNSCache(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, scope, server := r.URL.Query().Get("id"), r.URL.Query().Get("scope"), r.URL.Query().Get("server")
		if id != "" {
			decoded, err := base64.RawURLEncoding.DecodeString(id)
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError("invalid cache id"))
				return
			}
			var ok bool
			scope, id, ok = strings.Cut(string(decoded), "\x00")
			if !ok || len(id) != 64 {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError("invalid cache id"))
				return
			}
		}
		count := 0
		err := s.visitDNS(func(current string, router adapter.DNSRouter) error {
			if (id != "" || server != "" || r.URL.Query().Has("scope")) && current != scope {
				return nil
			}
			inspector, ok := router.(adapter.DNSCacheInspector)
			if !ok {
				return fmt.Errorf("DNS cache management unavailable")
			}
			deleted, err := inspector.DeleteDNSCacheEntries(id, server)
			count += deleted
			return err
		})
		if err != nil {
			dnsAPIError(w, r, err)
			return
		}
		render.JSON(w, r, render.M{"deleted": count})
	}
}

func dnsServers(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		servers := []adapter.DNSServerInfo{}
		err := s.visitDNS(func(scope string, router adapter.DNSRouter) error {
			inspector, ok := router.(adapter.DNSInspector)
			if !ok {
				return fmt.Errorf("DNS inspection unavailable")
			}
			current, err := inspector.DNSServers()
			if err != nil {
				return err
			}
			for _, server := range current {
				server.Scope = scope
				servers = append(servers, server)
			}
			return nil
		})
		if err != nil {
			dnsAPIError(w, r, err)
			return
		}
		render.JSON(w, r, render.M{"servers": servers})
	}
}

func dnsRules(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rules := []adapter.DNSRuleInfo{}
		err := s.visitDNS(func(scope string, router adapter.DNSRouter) error {
			inspector, ok := router.(adapter.DNSInspector)
			if !ok {
				return fmt.Errorf("DNS inspection unavailable")
			}
			for _, rule := range inspector.DNSRules() {
				rule.Scope = scope
				rules = append(rules, rule)
			}
			return nil
		})
		if err != nil {
			dnsAPIError(w, r, err)
			return
		}
		render.JSON(w, r, render.M{"rules": rules})
	}
}

func dnsAPIError(w http.ResponseWriter, r *http.Request, err error) {
	render.Status(r, http.StatusInternalServerError)
	render.JSON(w, r, newError(err.Error()))
}
