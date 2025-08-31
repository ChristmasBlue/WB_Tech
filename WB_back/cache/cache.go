package cache

import (
	"bytes"
	"sync"
)

type Cache struct {
	mu    sync.RWMutex
	items map[string]bytes.Buffer
}

func NewCaсhe() *Cache {
	return &Cache{
		items: make(map[string]bytes.Buffer),
	}
}

func (c *Cache) Add(key string, value *bytes.Buffer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = *value
}

func (c *Cache) Get(key string) (bytes.Buffer, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.items[key]
	if !ok {
		return bytes.Buffer{}, false
	}
	return value, true
}
