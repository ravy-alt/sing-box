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
	access       sync.RWMutex
	delayHistory map[string]map[string]adapter.URLTestHistory
	updateHooks  []*observable.Subscriber[struct{}]
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{delayHistory: make(map[string]map[string]adapter.URLTestHistory)}
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
	h, ok := s.delayHistory[tag][link]
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
	if len(links) == 0 {
		delete(s.delayHistory, tag)
	} else {
		delete(s.delayHistory[tag], NormalizeURL(links[0]))
		if len(s.delayHistory[tag]) == 0 {
			delete(s.delayHistory, tag)
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
	bucket := s.delayHistory[tag]
	if bucket == nil {
		bucket = make(map[string]adapter.URLTestHistory)
		s.delayHistory[tag] = bucket
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
	urls := make([]string, 0, len(s.delayHistory[tag]))
	for link := range s.delayHistory[tag] {
		urls = append(urls, link)
	}
	return urls
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
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
	for _, hook := range s.updateHooks {
		hook.Emit(struct{}{})
	}
}
func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}
