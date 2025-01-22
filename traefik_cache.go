package traefik_cache

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/pierrec/xxHash/xxHash64"
	"github.com/yerTools/traefik_cache/src/go/cache"
)

type Config struct {
	Path string `json:"path" yaml:"path" toml:"path"`
}

func CreateConfig() *Config {
	return &Config{}
}

type cachePlugin struct {
	config *Config
	next   http.Handler
	cache  *cache.Cache
}

func New(_ context.Context, next http.Handler, cfg *Config, name string) (http.Handler, error) {
	cache := cache.NewCache(time.Millisecond * 500)

	c := &cachePlugin{
		config: cfg,
		next:   next,
		cache:  cache,
	}

	return c, nil
}

// ServeHTTP serves an HTTP request.
func (c *cachePlugin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Treafik-Cache-Status", "miss")

	key, ok := calculateKey(r)
	if !ok {
		w.Header().Set("X-Treafik-Cache-Cacheable", "false")
		c.next.ServeHTTP(w, r)
		return
	}

	cached, ok := c.cache.Get(key)
	if ok {
		deleteKeys := make([]string, 0, len(cached.Value.Headers))
		for k := range w.Header() {
			_, ok := cached.Value.Headers[k]
			if !ok {
				deleteKeys = append(deleteKeys, k)
			}
		}
		for _, k := range deleteKeys {
			delete(w.Header(), k)
		}
		for k, v := range cached.Value.Headers {
			w.Header()[k] = v
		}

		w.Header().Set("X-Treafik-Cache-Status", "hit")
		w.Header().Set("X-Treafik-Cache-Cost", strconv.FormatInt(cached.Cost, 10))
		w.Header().Set("X-Treafik-Cache-Expiration", cached.Expiration.Format(time.RFC3339Nano))
		w.Header().Set("X-Treafik-Cache-Allocation", strconv.FormatInt(c.cache.Cost(), 10))

		w.WriteHeader(cached.Value.Status)
		w.Write(cached.Value.Body)

		return
	}

	w.Header().Set("X-Treafik-Cache-Cacheable", "true")
	w.Header().Set("X-Treafik-Cache-Key", fmt.Sprintf("%d:%d", key.Key, key.Conflict))
	w.Header().Set("X-Treafik-Cache-Allocation", strconv.FormatInt(c.cache.Cost(), 10))

	vw := &cacheValueWriter{
		dest: w,
		value: cache.CacheValue{
			Status:  0,
			Headers: make(map[string][]string),
			Body:    make([]byte, 0, 1024),
		},
	}
	c.next.ServeHTTP(vw, r)

	vw.value.Headers = w.Header().Clone()

	cost := int64(8+8+4) + int64(len(vw.value.Body))
	for k, v := range vw.value.Headers {
		cost += int64(len(k))
		for _, vv := range v {
			cost += int64(len(vv))
		}
	}

	c.cache.Set(key, vw.value, cost, 0)
}

type cacheValueWriter struct {
	value cache.CacheValue
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

func calculateKey(r *http.Request) (cache.StoreKey, bool) {
	if r.Method != "HEAD" && r.Method != "GET" {
		return cache.StoreKey{}, false
	}

	hasher := xxHash64.New(161_269)
	hasher.Write([]byte(r.Method))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.Host))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.URL.Path))

	hasher.Write([]byte{161, 2, 6, 9})
	hasher.Write([]byte(r.URL.RawQuery))

	pathHash := hasher.Sum64()

	hasher.Reset()
	hasher.Write([]byte(r.Header.Get("Accept-Encoding")))

	return cache.StoreKey{
		Key:      pathHash,
		Conflict: hasher.Sum64(),
	}, true
}
