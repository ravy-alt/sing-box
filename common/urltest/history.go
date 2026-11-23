package urltest

import (
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/observable"
)

const DefaultURL = "https://www.gstatic.com/generate_204"
const maxURLsPerNode = 64

func NormalizeURL(link string) string {
	if link = strings.TrimSpace(link); link != "" {
		return link
	}
	return DefaultURL
}

type HistoryStorage struct {
	access       *sync.RWMutex
	delayHistory map[string]map[string]adapter.URLTestHistory
	udpHistory   map[string]map[string]adapter.UDPTestHistory
	updateHooks  []*observable.Subscriber[struct{}]
	root         *HistoryStorage
	parent       *HistoryStorage
	scope        string
	owns         func(string) bool
	children     map[string]*HistoryStorage
	closed       bool
}

func NewHistoryStorage() *HistoryStorage {
	s := &HistoryStorage{access: new(sync.RWMutex), delayHistory: make(map[string]map[string]adapter.URLTestHistory), udpHistory: make(map[string]map[string]adapter.UDPTestHistory), children: make(map[string]*HistoryStorage)}
	s.root = s
	return s
}

// NewScope stages a provider generation privately. Publish makes the same
// storage visible to the parent API; old generations retain their own view and
// cannot write into their replacement. The ownership predicate must be pure.
func (s *HistoryStorage) NewScope(scope string, owns func(string) bool) *HistoryStorage {
	if s == nil {
		s = NewHistoryStorage()
	}
	child := NewHistoryStorage()
	child.access, child.root, child.parent = s.access, s.root, s
	child.scope, child.owns = scope, owns
	return child
}

func (s *HistoryStorage) Publish() {
	if s == nil || s.parent == nil {
		return
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.parent.closed {
		return
	}
	s.parent.children[s.scope] = s
	// Remove observations written before this provider had an active scope.
	for tag := range s.parent.delayHistory {
		if s.owns(tag) {
			delete(s.parent.delayHistory, tag)
		}
	}
	for tag := range s.parent.udpHistory {
		if s.owns(tag) {
			delete(s.parent.udpHistory, tag)
		}
	}
	s.notifyUpdated()
}

func (s *HistoryStorage) activeLocked() bool {
	for s.parent != nil {
		if s.closed || s.parent.children[s.scope] != s {
			return false
		}
		s = s.parent
	}
	return !s.closed
}

// All scope maps use the root lock. External references are shared only while
// this generation is active, so a retiring probe cannot modify a host node.
func (s *HistoryStorage) targetLocked(tag string, write bool) *HistoryStorage {
	if s.parent != nil && !s.owns(tag) {
		if write && !s.activeLocked() {
			return s
		}
		return s.parent.targetLocked(tag, write)
	}
	for _, child := range s.children {
		if child.owns(tag) {
			return child.targetLocked(tag, write)
		}
	}
	return s
}

// An omitted URL means the standard test URL, never the most recent other URL.
func (s *HistoryStorage) LoadURLTestHistory(tag string, links ...string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	link := DefaultURL
	if len(links) > 0 {
		link = NormalizeURL(links[0])
	}
	s.access.RLock()
	defer s.access.RUnlock()
	h, ok := s.targetLocked(tag, false).delayHistory[tag][link]
	if !ok || h.Delay == 0 {
		return nil
	}
	return &h
}

// Delete without a URL is reserved for node retirement / transport failure.
func (s *HistoryStorage) DeleteURLTestHistory(tag string, links ...string) {
	if s == nil {
		return
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return
	}
	target := s.targetLocked(tag, true)
	if len(links) == 0 {
		delete(target.delayHistory, tag)
		delete(target.udpHistory, tag)
	} else {
		delete(target.delayHistory[tag], NormalizeURL(links[0]))
		if len(target.delayHistory[tag]) == 0 {
			delete(target.delayHistory, tag)
		}
	}
	s.notifyUpdated()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	if s == nil || history == nil || tag == "" {
		return
	}
	h := *history
	h.URL = NormalizeURL(h.URL)
	if h.Outbound == "" {
		h.Outbound = tag
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return
	}
	target := s.targetLocked(tag, true)
	bucket := target.delayHistory[tag]
	if bucket == nil {
		bucket = make(map[string]adapter.URLTestHistory)
		target.delayHistory[tag] = bucket
	}
	if previous, exists := bucket[h.URL]; exists {
		// Probe start times prevent an older slow result (including failures)
		// from replacing a newer observation of the same node and URL.
		if h.Time.Before(previous.Time) {
			return
		}
	} else if len(bucket) >= maxURLsPerNode {
		oldest := ""
		for link, value := range bucket {
			if oldest == "" || value.Time.Before(bucket[oldest].Time) {
				oldest = link
			}
		}
		delete(bucket, oldest)
	}
	bucket[h.URL] = h
	s.notifyUpdated()
}

func (s *HistoryStorage) URLs(tag string) []string {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	history := s.targetLocked(tag, false).delayHistory[tag]
	urls := make([]string, 0, len(history))
	for link := range history {
		urls = append(urls, link)
	}
	return urls
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.root.updateHooks = append(s.root.updateHooks, hook)
}
func (s *HistoryStorage) NotifyUpdated() {
	if s == nil {
		return
	}
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}
func (s *HistoryStorage) notifyUpdated() {
	if !s.activeLocked() {
		return
	}
	for _, hook := range s.root.updateHooks {
		hook.Emit(struct{}{})
	}
}
func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.closed = true
	if s.parent != nil && s.parent.children[s.scope] == s {
		delete(s.parent.children, s.scope)
	}
	clear(s.children)
	s.updateHooks = nil
	return nil
}
