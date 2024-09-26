package nyxutils

import (
	"strings"
	"sync"
	"time"
)

const shardCount = 32

type entry[V any] struct {
	value      V
	expireTime *time.Time
}

type mapShard[V any] struct {
	mu sync.RWMutex
	m  map[string]*entry[V]
}

type SafeMap[V any] struct {
	shards [shardCount]*mapShard[V]
}

func NewSafeMap[V any]() *SafeMap[V] {
	sm := &SafeMap[V]{}
	for i := 0; i < shardCount; i++ {
		sm.shards[i] = &mapShard[V]{
			m: make(map[string]*entry[V]),
		}
	}
	return sm
}

// fnv32 hashes a string to a uint32 using FNV-1a algorithm.
func fnv32(key string) uint32 {
	var hash uint32 = 2166136261
	const prime32 = 16777619
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

// getShard returns the shard corresponding to the given key.
func (sm *SafeMap[V]) getShard(key string) *mapShard[V] {
	hash := fnv32(key)
	return sm.shards[hash%shardCount]
}

func (sm *SafeMap[V]) Exists(key string) bool {
	shard := sm.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.m[key]
	if !exists {
		shard.mu.RUnlock()
		return false
	}
	if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
		shard.mu.RUnlock()
		// Remove expired entry
		shard.mu.Lock()
		defer shard.mu.Unlock()
		// Double-check to ensure thread-safety
		if entry2, exists2 := shard.m[key]; exists2 && entry2 == entry {
			delete(shard.m, key)
		}
		return false
	}
	shard.mu.RUnlock()
	return true
}

func (sm *SafeMap[V]) Len() int {
	total := 0
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for _, entry := range shard.m {
			if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
				continue
			}
			total++
		}
		shard.mu.RUnlock()
	}
	return total
}

func (sm *SafeMap[V]) Keys() []string {
	keys := make([]string, 0)
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for k, entry := range shard.m {
			if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
				continue
			}
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}
	return keys
}

func (sm *SafeMap[V]) Clear() {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		shard.m = make(map[string]*entry[V])
		shard.mu.Unlock()
	}
}

func (sm *SafeMap[V]) Set(key string, value V) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.m[key] = &entry[V]{value: value, expireTime: nil}
}

func (sm *SafeMap[V]) SetWithExpireDuration(key string, value V, expireDuration time.Duration) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	expire := time.Now().Add(expireDuration)
	shard.m[key] = &entry[V]{value: value, expireTime: &expire}
}

func (sm *SafeMap[V]) Get(key string) (V, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.m[key]
	if !exists {
		shard.mu.RUnlock()
		var zero V
		return zero, false
	}
	// Check expiration
	if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
		shard.mu.RUnlock()
		// Remove expired entry
		shard.mu.Lock()
		defer shard.mu.Unlock()
		// Double-check to ensure thread-safety
		if entry2, exists2 := shard.m[key]; exists2 && entry2 == entry {
			delete(shard.m, key)
		}
		var zero V
		return zero, false
	}
	// Entry is valid
	value := entry.value
	shard.mu.RUnlock()
	return value, true
}

func (sm *SafeMap[V]) UpdateExpireTime(key string, expireDuration time.Duration) bool {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if entry, exists := shard.m[key]; exists {
		expire := time.Now().Add(expireDuration)
		entry.expireTime = &expire
		return true
	}
	return false
}

func (sm *SafeMap[V]) Delete(key string) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.m, key)
}

func (sm *SafeMap[V]) DeleteAllKeysStartingWith(prefix string) {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		for k := range shard.m {
			if strings.HasPrefix(k, prefix) {
				delete(shard.m, k)
			}
		}
		shard.mu.Unlock()
	}
}

func (sm *SafeMap[V]) ExpiredAndGet(key string) (V, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.m[key]
	if !exists {
		shard.mu.RUnlock()
		var zero V
		return zero, false
	}

	// If expireTime is nil, the entry never expires
	if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
		shard.mu.RUnlock()

		shard.mu.Lock()
		defer shard.mu.Unlock()
		// Double-check to ensure thread-safety
		if entry2, exists2 := shard.m[key]; exists2 && entry2 == entry {
			delete(shard.m, key)
			var zero V
			return zero, false
		}
		return entry.value, true
	}

	// Entry is valid
	value := entry.value
	shard.mu.RUnlock()
	return value, true
}

func (sm *SafeMap[V]) Range(f func(key, value interface{}) bool) {
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for k, entry := range shard.m {
			if entry.expireTime != nil && time.Now().After(*entry.expireTime) {
				continue
			}
			if !f(k, entry.value) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}
