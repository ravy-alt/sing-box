package cachefile

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
)

func dnsRecordKey(name string, qType uint16) []byte {
	key := make([]byte, 2+len(name))
	binary.BigEndian.PutUint16(key, qType)
	copy(key[2:], name)
	return key
}

func (c *CacheFile) saveDNSCacheRecord(key saveCacheKey, entry saveDNSCacheEntry) error {
	value := make([]byte, 8+len(entry.rawMessage))
	binary.BigEndian.PutUint64(value, uint64(entry.expireAt.Unix()))
	copy(value[8:], entry.rawMessage)
	info, err := json.Marshal(entry.info)
	if err != nil {
		return err
	}
	return c.batch(func(tx *bbolt.Tx) error {
		c.saveDNSCacheAccess.RLock()
		defer c.saveDNSCacheAccess.RUnlock()
		if current, exists := c.saveDNSCache[key]; !exists || current.sequence != entry.sequence {
			return nil
		}
		for _, item := range []struct{ bucket, value []byte }{{bucketDNSCache, value}, {bucketDNSCacheInfo, info}} {
			bucket, err := c.createBucket(tx, item.bucket)
			if err != nil {
				return err
			}
			bucket, err = bucket.CreateBucketIfNotExists([]byte(key.TransportName))
			if err != nil {
				return err
			}
			if err = bucket.Put(dnsRecordKey(key.QuestionName, key.QType), item.value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *CacheFile) DNSCacheInfo(transport, name string, qType uint16) adapter.DNSQueryInfo {
	c.saveDNSCacheAccess.RLock()
	entry, found := c.saveDNSCache[saveCacheKey{transport, name, qType}]
	c.saveDNSCacheAccess.RUnlock()
	if found {
		return entry.info
	}
	var info adapter.DNSQueryInfo
	_ = c.view(func(tx *bbolt.Tx) error {
		if bucket := c.bucket(tx, bucketDNSCacheInfo); bucket != nil {
			if bucket = bucket.Bucket([]byte(transport)); bucket != nil {
				_ = json.Unmarshal(bucket.Get(dnsRecordKey(name, qType)), &info)
			}
		}
		return nil
	})
	return info
}

func (c *CacheFile) UpdateDNSCacheInfo(transport, name string, qType uint16, info adapter.DNSQueryInfo) error {
	// Serialize with deletion and flushing. Queue a complete record so metadata
	// cannot outlive the corresponding answer or overwrite a newer answer.
	c.dnsCacheWriteAccess.Lock()
	raw, expires, found := c.LoadDNSCache(transport, name, qType)
	if !found {
		c.dnsCacheWriteAccess.Unlock()
		return nil
	}
	key := saveCacheKey{transport, name, qType}
	c.saveDNSCacheAccess.Lock()
	if c.dnsCacheClosing {
		c.saveDNSCacheAccess.Unlock()
		c.dnsCacheWriteAccess.Unlock()
		return nil
	}
	entry := c.saveDNSCache[key]
	start := !entry.saving
	if start {
		c.dnsCacheWorkers.Add(1)
	}
	entry.rawMessage, entry.expireAt, entry.info = raw, expires, info
	c.dnsCacheSequence++
	entry.sequence = c.dnsCacheSequence
	entry.saving = true
	c.saveDNSCache[key] = entry
	c.saveDNSCacheAccess.Unlock()
	c.dnsCacheWriteAccess.Unlock()
	if start {
		go c.flushPendingDNSCache(key, c.logger)
	}
	return nil
}

func (c *CacheFile) ListDNSCache() ([]adapter.DNSCacheRecord, error) {
	c.dnsCacheWriteAccess.Lock()
	defer c.dnsCacheWriteAccess.Unlock()
	c.saveDNSCacheAccess.RLock()
	defer c.saveDNSCacheAccess.RUnlock()
	entries := make(map[saveCacheKey]adapter.DNSCacheRecord)
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := c.bucket(tx, bucketDNSCache)
		if bucket == nil {
			return nil
		}
		infos := c.bucket(tx, bucketDNSCacheInfo)
		return bucket.ForEachBucket(func(transport []byte) error {
			var infoBucket *bbolt.Bucket
			if infos != nil {
				infoBucket = infos.Bucket(transport)
			}
			return bucket.Bucket(transport).ForEach(func(key, value []byte) error {
				if len(key) < 3 || len(value) < 8 {
					return nil
				}
				entry := adapter.DNSCacheRecord{
					Transport: string(transport), Name: string(key[2:]), Type: binary.BigEndian.Uint16(key),
					ExpiresAt: time.Unix(int64(binary.BigEndian.Uint64(value)), 0), Message: append([]byte(nil), value[8:]...),
				}
				if infoBucket != nil {
					_ = json.Unmarshal(infoBucket.Get(key), &entry.Info)
				}
				entries[saveCacheKey{entry.Transport, entry.Name, entry.Type}] = entry
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}
	for key, entry := range c.saveDNSCache {
		entries[key] = adapter.DNSCacheRecord{Transport: key.TransportName, Name: key.QuestionName, Type: key.QType,
			Message: append([]byte(nil), entry.rawMessage...), ExpiresAt: entry.expireAt, Info: entry.info}
	}
	result := make([]adapter.DNSCacheRecord, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}
	return result, nil
}

func (c *CacheFile) DeleteDNSCache(transport, name string, qType uint16) error {
	return c.DeleteDNSCacheRecords([]adapter.DNSCacheRecord{{Transport: transport, Name: name, Type: qType}})
}

func (c *CacheFile) DeleteDNSCacheRecords(records []adapter.DNSCacheRecord) error {
	c.dnsCacheWriteAccess.Lock()
	defer c.dnsCacheWriteAccess.Unlock()
	c.saveDNSCacheAccess.Lock()
	for _, record := range records {
		delete(c.saveDNSCache, saveCacheKey{record.Transport, record.Name, record.Type})
	}
	c.saveDNSCacheAccess.Unlock()
	return c.batch(func(tx *bbolt.Tx) error {
		for _, record := range records {
			for _, bucketName := range [][]byte{bucketDNSCache, bucketDNSCacheInfo} {
				if bucket := c.bucket(tx, bucketName); bucket != nil {
					if bucket = bucket.Bucket([]byte(record.Transport)); bucket != nil {
						if err := bucket.Delete(dnsRecordKey(record.Name, record.Type)); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
}
