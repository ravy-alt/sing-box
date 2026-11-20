package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
)

type savedSubscription struct {
	Version   int                          `json:"version"`
	URLHash   string                       `json:"url_hash"`
	ETag      string                       `json:"etag,omitempty"`
	UpdatedAt time.Time                    `json:"updated_at"`
	Content   []byte                       `json:"content"`
	Resources *savedResources              `json:"resources,omitempty"`
	Imports   map[string]map[string]string `json:"imports,omitempty"`
	Names     map[string]string            `json:"names,omitempty"`
}

func (s *Subscription) urlHash() string {
	hash := sha256.Sum256([]byte(s.options.URL))
	return hex.EncodeToString(hash[:])
}

func (s *Subscription) Restore() error {
	if err := s.lockRefresh(s.ctx); err != nil {
		return err
	}
	defer s.unlockRefresh()
	if s.Status().Ready {
		return nil
	}
	if parent := service.PtrFromContext[resourceCache](s.ctx); parent != nil {
		if saved, found := parent.loadSubscription(s.Tag()); found && saved.URLHash == s.urlHash() {
			if err := s.restoreSaved(saved); err == nil {
				return nil
			}
		}
	}
	if s.options.CachePath == "" {
		return nil
	}
	file, err := os.Open(filemanager.BasePath(s.ctx, s.options.CachePath))
	if err != nil {
		return err
	}
	defer file.Close()
	limit := s.options.MaxCacheSize
	content, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return err
	}
	if int64(len(content)) > limit {
		return E.New("subscription cache exceeds size limit")
	}
	var saved savedSubscription
	if err = json.Unmarshal(content, &saved); err != nil {
		return err
	}
	return s.restoreSaved(saved)
}

func (s *Subscription) restoreSaved(saved savedSubscription) error {
	if saved.Version != 1 || saved.URLHash != s.urlHash() {
		return E.New("subscription cache version or URL mismatch")
	}
	if int64(len(saved.Content)) > s.options.MaxDownloadSize {
		return E.New("cached subscription exceeds max_download_size")
	}
	s.mu.Lock()
	s.importAliases = saved.Imports
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(s.options.DownloadTimeout))
	defer cancel()
	g, err := s.buildResources(ctx, saved.Content, saved.Resources)
	if err != nil {
		return err
	}
	g.savedNames = saved.Names
	return s.publish(g, saved.Content, saved.ETag, saved.UpdatedAt)
}

// Only committed snapshots enter the cache. Rename keeps the previous good
// cache usable if writing the new one fails or the process is interrupted.
func (s *Subscription) saveCache() error {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.mu.Lock()
	if s.current == nil || s.closed {
		s.mu.Unlock()
		return nil
	}
	saved := savedSubscription{Version: 1, URLHash: s.urlHash(), ETag: s.etag, UpdatedAt: s.status.UpdatedAt, Content: s.content, Resources: s.current.resources.snapshot(), Imports: s.current.imports}
	s.mu.Unlock()
	saved.Names = s.savedNames()
	if parent := service.PtrFromContext[resourceCache](s.ctx); parent != nil {
		parent.saveSubscription(s.Tag(), saved)
	}
	if s.options.CachePath == "" {
		return nil
	}
	content, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	if int64(len(content)) > s.options.MaxCacheSize {
		return E.New("subscription cache exceeds max_cache_size")
	}
	path := filemanager.BasePath(s.ctx, s.options.CachePath)
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".subscription-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(content); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func (s *Subscription) savedNames() map[string]string {
	names := adapter.OutboundNamesFromContext(s.ctx).Snapshot()
	for tag := range names {
		if !ownsMember(s.Tag(), tag) {
			delete(names, tag)
		}
	}
	return names
}
