package cache

import (
	"bytes"
	"container/list"
	"log"
	"sync"
	"time"
)

// Cache реализует LRU кэш с ограничением по количеству элементов
type Cache struct {
	mu       sync.RWMutex
	items    map[string]*list.Element
	order    *list.List // Для отслеживания порядка использования (LRU)
	maxItems int
}

// cacheItem представляет элемент кэша
type cacheItem struct {
	key      string
	value    bytes.Buffer
	lastUsed time.Time
}

// NewCache создает новый кэш с указанным максимальным количеством элементов
func NewCache(maxItems int) *Cache {
	if maxItems <= 0 {
		maxItems = 1000 // Значение по умолчанию
	}

	return &Cache{
		items:    make(map[string]*list.Element),
		order:    list.New(),
		maxItems: maxItems,
	}
}

// Add добавляет или обновляет элемент в кэше
func (c *Cache) Add(key string, value *bytes.Buffer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Создаем ГЛУБОКУЮ копию данных
	dataCopy := make([]byte, value.Len())
	copy(dataCopy, value.Bytes())
	newBuffer := bytes.NewBuffer(dataCopy)

	// Если элемент существует - обновляем
	if elem, exists := c.items[key]; exists {
		item := elem.Value.(*cacheItem)
		item.value = *newBuffer
		item.lastUsed = time.Now()
		c.order.MoveToFront(elem)
		log.Printf("Updated in cache: %s (size: %d bytes)", key, value.Len())
		return
	}

	// Создаем новый элемент
	item := &cacheItem{
		key:      key,
		value:    *newBuffer,
		lastUsed: time.Now(),
	}

	// Добавляем в начало
	elem := c.order.PushFront(item)
	c.items[key] = elem

	log.Printf("Added to cache: %s (size: %d bytes)", key, value.Len())

	// Если достигли максимума - удаляем 50% самых старых
	if len(c.items) > c.maxItems {
		c.evictHalf()
	}
}

// evictHalf удаляет 50% самых старых элементов
func (c *Cache) evictHalf() {
	itemsToRemove := len(c.items) / 2

	for i := 0; i < itemsToRemove; i++ {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}

		item := oldest.Value.(*cacheItem)
		delete(c.items, item.key)
		c.order.Remove(oldest)
	}
}

// Get получает элемент по ключу
func (c *Cache) Get(key string) (*bytes.Buffer, bool) {
	c.mu.RLock()
	elem, exists := c.items[key]
	if !exists {
		c.mu.RUnlock()
		log.Printf("Cache miss for key: %s", key)
		return &bytes.Buffer{}, false
	}

	item := elem.Value.(*cacheItem)
	valueCopy := bytes.NewBuffer(item.value.Bytes())
	c.mu.RUnlock()

	c.mu.Lock()
	if existing, ok := c.items[key]; ok {
		existing.Value.(*cacheItem).lastUsed = time.Now()
		c.order.MoveToFront(existing)
	}
	c.mu.Unlock()

	log.Printf("Cache hit for key: %s", key)
	return valueCopy, true
}
