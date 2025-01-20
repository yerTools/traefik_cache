package traefik_cache

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/pierrec/xxHash/xxHash64"
)

type Config struct {
	Path string `json:"path" yaml:"path" toml:"path"`
}

func CreateConfig() *Config {
	return &Config{}
}

type cacheValue struct {
	Status  int
	Headers map[string][]string
	Body    []byte
	Cost    int64
}

type cache struct {
	config *Config
	next   http.Handler
	cache  *ristretto.Cache[uint64, cacheValue]
}

func New(_ context.Context, next http.Handler, cfg *Config, name string) (http.Handler, error) {
	ristrettoCache, err := ristretto.NewCache(&ristretto.Config[uint64, cacheValue]{
		NumCounters: 1e6,     // number of keys to track frequency of (1M).
		MaxCost:     1 << 28, // maximum cost of cache (256MiB).
		BufferItems: 64,      // number of keys per Get buffer.
		KeyToHash:   func(key uint64) (uint64, uint64) { return key, 0 },
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ristretto cache: %w", err)
	}

	c := &cache{
		config: cfg,
		next:   next,
		cache:  ristrettoCache,
	}

	return c, nil
}

// ServeHTTP serves an HTTP request.
func (c *cache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Treafik-Cache-Status", "miss")

	key, ok := calculateKey(r)
	if !ok {
		w.Header().Set("X-Treafik-Cache-Cacheable", "false")
		c.next.ServeHTTP(w, r)
		return
	}

	cached, ok := c.cache.Get(key)
	if ok {
		ttl, ok := c.cache.GetTTL(key)
		if !ok {
			ttl = 0
		}

		w.Header().Set("X-Treafik-Cache-Status", "hit")
		w.Header().Set("X-Treafik-Cache-Cost", string(cached.Cost))
		w.Header().Set("X-Treafik-Cache-TTL", string(ttl.Milliseconds()))

		deleteKeys := make([]string, 0, len(cached.Headers))
		for k := range w.Header() {
			_, ok := cached.Headers[k]
			if !ok {
				deleteKeys = append(deleteKeys, k)
			}
		}
		for _, k := range deleteKeys {
			delete(w.Header(), k)
		}
		for k, v := range cached.Headers {
			w.Header()[k] = v
		}

		w.WriteHeader(cached.Status)
		w.Write(cached.Body)

		return
	}

	w.Header().Set("X-Treafik-Cache-Cacheable", "true")
	w.Header().Set("X-Treafik-Cache-Key", string(key))

	vw := &cacheValueWriter{
		dest: w,
		value: cacheValue{
			Status:  0,
			Headers: make(map[string][]string),
			Body:    make([]byte, 0, 1024),
		},
	}
	c.next.ServeHTTP(vw, r)

	vw.value.Headers = w.Header().Clone()

	vw.value.Cost = int64(8+8+4) + int64(len(vw.value.Body))
	for k, v := range vw.value.Headers {
		vw.value.Cost += int64(len(k))
		for _, vv := range v {
			vw.value.Cost += int64(len(vv))
		}
	}

	c.cache.SetWithTTL(key, vw.value, vw.value.Cost, 10*time.Second)
}

type cacheValueWriter struct {
	value cacheValue
	dest  http.ResponseWriter
}

func (w *cacheValueWriter) Header() http.Header {
	return w.dest.Header()
}

func (w *cacheValueWriter) Write(p []byte) (int, error) {
	w.value.Body = append(w.value.Body, p...)
	return w.dest.Write(p)
}

func (w *cacheValueWriter) WriteHeader(s int) {
	w.value.Status = s
	w.dest.WriteHeader(s)
}

func calculateKey(r *http.Request) (uint64, bool) {
	if r.Method != "HEAD" && r.Method != "GET" {
		return 0, false
	}

	hasher := xxHash64.New(161_269)
	hasher.Write([]byte(r.Method))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.Host))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.URL.Path))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.URL.RawQuery))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.Header.Get("Accept-Encoding")))

	return hasher.Sum64(), true
}
