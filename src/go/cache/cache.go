package cache

import "time"

type Cache struct {
	store *store
}

type CacheValue struct {
	Status  int
	Headers map[string][]string
	Body    []byte
}

func NewCache(bucketSize time.Duration) *Cache {
	c := &Cache{
		store: NewStore(time.Now(), bucketSize),
	}

	t := time.NewTicker(bucketSize)
	go func() {
		for now := range t.C {
			c.store.PurgeExpired(now)
		}
	}()

	return c
}

func (c *Cache) Cost() int64 {
	return c.store.Cost()
}

func (c *Cache) Set(key StoreKey, value CacheValue, cost int64, ttl time.Duration) bool {
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

	i := &StoreItem{
		Key:        key,
		Value:      value,
		Cost:       cost,
		Expiration: expiration,
	}

	c.store.Set(now, i)
	return true
}

func (c *Cache) Get(key StoreKey) (StoreItem, bool) {
	item, ok := c.store.Get(time.Now(), key)
	if !ok {
		var zeroValue StoreItem
		return zeroValue, false
	}

	return *item, true
}
