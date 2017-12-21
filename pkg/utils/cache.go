package utils

import (
	"time"

	"github.com/patrickmn/go-cache"
)

const (
	DefaultExpriration = time.Minute * 30
)

type RequestCache struct {
	Cache *cache.Cache
}

func NewRequestCache() *RequestCache {
	return &RequestCache{
		Cache: cache.New(DefaultExpriration, DefaultExpriration*2),
	}
}

func (c *RequestCache) Get(key string) bool {
	raw, found := c.Cache.Get(key)
	if !found {
		return false
	}
	return raw.(bool)
}

func (c *RequestCache) Set(key string, value bool) {
	c.Cache.SetDefault(key, value)
}
