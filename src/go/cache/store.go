package cache

import (
	"sync"
	"time"
)

const (
	ShardCount                  uint64 = 256
	StoreItemOverhead                  = 8 + 8 + 8 + 24 + 8
	StoreItemExpirationOverhead        = 8
)

type StoreKey struct {
	Key      uint64
	Conflict uint64
}

type StoreItem struct {
	Key        StoreKey
	Value      CacheValue
	Cost       int64
	Expiration time.Time
}

type store struct {
	shards []*concurrentMap
}

func NewStore(now time.Time, bucketSize time.Duration) *store {
	s := &store{
		shards: make([]*concurrentMap, ShardCount),
	}

	for i := range s.shards {
		s.shards[i] = NewConcurrentMap(now, bucketSize)
	}

	return s
}

func (s *store) Cost() int64 {
	cost := int64(0)
	for _, shard := range s.shards {
		cost += shard.Cost()
	}

	return cost
}

func (s *store) Set(now time.Time, item *StoreItem) {
	s.shards[item.Key.Key%ShardCount].Set(now, item)
}

func (s *store) PurgeExpired(now time.Time) {
	for _, shard := range s.shards {
		shard.PurgeExpired(now)
	}
}

func (s *store) Get(now time.Time, key StoreKey) (*StoreItem, bool) {
	return s.shards[key.Key%ShardCount].Get(now, key)
}

func (s *store) Remove(key StoreKey) {
	s.shards[key.Key%ShardCount].Remove(key)
}

type storeItemMap struct {
	data map[uint64][]*StoreItem
}

func newStoreItemMap() storeItemMap {
	return storeItemMap{
		data: make(map[uint64][]*StoreItem),
	}
}

func (m *storeItemMap) Set(item *StoreItem) *StoreItem {
	var removed *StoreItem

	existing, ok := m.data[item.Key.Key]
	if ok {
		for i, e := range existing {
			if e.Key.Conflict == item.Key.Conflict {
				removed = existing[i]
				existing[i] = item
				break
			}
		}
	}

	if removed == nil {
		if existing == nil {
			existing = []*StoreItem{item}
		} else {
			existing = append(existing, item)
		}
	}
	m.data[item.Key.Key] = existing

	return removed
}

func (m *storeItemMap) Remove(key StoreKey) *StoreItem {
	existing, ok := m.data[key.Key]
	if !ok {
		return nil
	}

	if len(existing) == 1 {
		if existing[0].Key.Conflict == key.Conflict {
			delete(m.data, key.Key)
			return existing[0]
		}
		return nil
	}

	for i, e := range existing {
		if e.Key.Conflict == key.Conflict {
			removed := existing[i]

			m.data[key.Key] = append(existing[:i], existing[i+1:]...)

			return removed
		}
	}

	return nil
}

func (m *storeItemMap) Get(key StoreKey) (*StoreItem, bool) {
	existing, ok := m.data[key.Key]
	if !ok {
		return nil, false
	}

	for _, e := range existing {
		if e.Key.Conflict == key.Conflict {
			return e, true
		}
	}

	return nil, false
}

func (m *storeItemMap) IsEmpty() bool {
	return len(m.data) == 0
}

func (m *storeItemMap) Length() int {
	i := 0
	for _, v := range m.data {
		i += len(v)
	}

	return i
}

func (m *storeItemMap) Clear() {
	for k := range m.data {
		delete(m.data, k)
	}
}

func (m *storeItemMap) Items() []*StoreItem {
	result := make([]*StoreItem, 0, m.Length())

	for _, v := range m.data {
		result = append(result, v...)
	}

	return result
}

func currentBucket(t time.Time, size time.Duration) int64 {
	return t.UnixNano() / int64(size)
}

type concurrentMap struct {
	mutex sync.RWMutex
	data  storeItemMap
	cost  int64

	bucketSize        time.Duration
	expirationBuckets map[int64]storeItemMap
	purgedBucket      int64
}

func NewConcurrentMap(now time.Time, bucketSize time.Duration) *concurrentMap {
	m := &concurrentMap{
		data: newStoreItemMap(),

		bucketSize:        bucketSize,
		expirationBuckets: make(map[int64]storeItemMap),
		purgedBucket:      currentBucket(now, bucketSize) - 1,
	}

	return m
}

func (m *concurrentMap) Cost() int64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.cost
}

func (m *concurrentMap) Set(now time.Time, item *StoreItem) {
	item.Cost += StoreItemOverhead
	if !item.Expiration.IsZero() {
		item.Cost += StoreItemExpirationOverhead
	}

	if !item.Expiration.IsZero() && !item.Expiration.After(now) {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	removed := m.data.Set(item)

	if removed != nil {
		m.cost -= removed.Cost
	}
	m.cost += item.Cost

	m.addItemToTimeBucketLocked(item)
	m.removeItemFromTimeBucketLocked(removed)
}

func (m *concurrentMap) addItemToTimeBucketLocked(item *StoreItem) {
	if item == nil || item.Expiration.IsZero() {
		return
	}

	bucket := currentBucket(item.Expiration, m.bucketSize)
	if bucket <= m.purgedBucket {
		return
	}

	b, ok := m.expirationBuckets[bucket]
	if !ok {
		b = newStoreItemMap()
	}

	b.Set(item)
	m.expirationBuckets[bucket] = b
}

func (m *concurrentMap) removeItemFromTimeBucketLocked(item *StoreItem) {
	if item == nil || item.Expiration.IsZero() {
		return
	}

	bucket := currentBucket(item.Expiration, m.bucketSize)
	if bucket <= m.purgedBucket {
		return
	}

	b, ok := m.expirationBuckets[bucket]
	if !ok {
		return
	}

	b.Remove(item.Key)
	if b.IsEmpty() {
		delete(m.expirationBuckets, bucket)
	}
}

func (m *concurrentMap) PurgeExpired(now time.Time) {
	currentBucket := currentBucket(now, m.bucketSize) - 1
	if currentBucket <= m.purgedBucket {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.expirationBuckets) == 0 {
		m.purgedBucket = currentBucket
		return
	}

	if len(m.expirationBuckets) <= int(currentBucket-m.purgedBucket) {
		for number, bucket := range m.expirationBuckets {
			if number > currentBucket {
				continue
			}

			m.purgeRemoveItemsLocked(bucket)
		}
	} else {
		for m.purgedBucket++; m.purgedBucket <= currentBucket; m.purgedBucket++ {
			bucket, ok := m.expirationBuckets[m.purgedBucket]
			if !ok {
				continue
			}

			m.purgeRemoveItemsLocked(bucket)
		}
	}

	m.purgedBucket = currentBucket
}

func (m *concurrentMap) purgeRemoveItemsLocked(bucket storeItemMap) {
	for _, item := range bucket.Items() {
		m.cost -= item.Cost
		m.data.Remove(item.Key)
	}
	delete(m.expirationBuckets, m.purgedBucket)
}

func (m *concurrentMap) Get(now time.Time, key StoreKey) (*StoreItem, bool) {
	item, ok := m.get(key)
	if !ok {
		return nil, false
	}

	if item.Expiration.IsZero() || now.Before(item.Expiration) {
		return item, true
	}

	m.Remove(key)
	return nil, false
}

func (m *concurrentMap) get(key StoreKey) (*StoreItem, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.data.Get(key)
}

func (m *concurrentMap) Remove(key StoreKey) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	removed := m.data.Remove(key)
	if removed != nil {
		m.cost -= removed.Cost
	}
	m.removeItemFromTimeBucketLocked(removed)
}
