package cache

import "time"

type Cache[K any, V any] struct {
	store *store[V]
}

func keyToHash[K any](key K) StoreKey {
	switch k := any(key).(type) {
	case uint64:
		return StoreKey{k, 0}
	case StoreKey:
		return k
	default:
		panic("CacheKey type not supported")
	}
}

func NewCache[K any, V any](bucketSize time.Duration) *Cache[K, V] {
	c := &Cache[K, V]{
		store: NewStore[V](time.Now(), bucketSize),
	}

	t := time.NewTicker(bucketSize)
	go func() {
		for now := range t.C {
			c.store.PurgeExpired(now)
		}
	}()

	return c
}

func (c *Cache[K, V]) Cost() int64 {
	return c.store.Cost()
}

func (c *Cache[K, V]) Set(key K, value V, cost int64, ttl time.Duration) bool {
	if c == nil {
		return false
	}

	var expiration time.Time
	now := time.Now()
	switch {
	case ttl == 0:
		break
	case ttl < 0:
		return false
	default:
		expiration = now.Add(ttl)
	}

	hashKey := keyToHash(key)

	i := &StoreItem[V]{
		Key:        hashKey,
		Value:      value,
		Cost:       cost,
		Expiration: expiration,
	}

	c.store.Set(now, i)
	return true
}

func (c *Cache[K, V]) Get(key K) (StoreItem[V], bool) {
	hashKey := keyToHash(key)

	item, ok := c.store.Get(time.Now(), hashKey)
	if !ok {
		var zeroValue StoreItem[V]
		return zeroValue, false
	}

	return *item, true
}
