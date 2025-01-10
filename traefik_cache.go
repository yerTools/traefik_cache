package traefikcache

import (
	"context"
	"log"
	"net/http"
)

type Config struct {
	Path            string `json:"path" yaml:"path" toml:"path"`
	MaxExpiry       int    `json:"maxExpiry" yaml:"maxExpiry" toml:"maxExpiry"`
	Cleanup         int    `json:"cleanup" yaml:"cleanup" toml:"cleanup"`
	AddStatusHeader bool   `json:"addStatusHeader" yaml:"addStatusHeader" toml:"addStatusHeader"`
}

// CreateConfig returns a config instance.
func CreateConfig() *Config {
	return &Config{}
}

func New(_ context.Context, next http.Handler, cfg *Config, name string) (http.Handler, error) {
	log.Printf("Creating cache plugin: %s\n", name)
	c := &cache{
		config: cfg,
		next:   next,
	}

	return c, nil
}

type cache struct {
	config *Config
	next   http.Handler
}

// ServeHTTP serves an HTTP request.
func (c *cache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Handling URL: %v\n", r.URL)
	for k, v := range r.Header {
		log.Printf("Header: %v: %v\n", k, v)
	}
	c.next.ServeHTTP(w, r)
}
