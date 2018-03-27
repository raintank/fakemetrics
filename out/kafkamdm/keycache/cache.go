package keycache

import (
	"time"

	schema "gopkg.in/raintank/schema.v1"
)

// Cache is a single-tenant keycache
// it is sharded for 2 reasons:
// * more granular GC (eg. less latency perceived by caller)
// * mild space savings cause keys are 1 byte shorter
type Cache struct {
	shards [256]Shard // by org
}

func NewCache(ref Ref) *Cache {
	c := Cache{}
	for i := 0; i < 256; i++ {
		c.shards[i] = NewShard(ref)
	}
	return &c
}

func (c *Cache) Touch(key schema.Key, t time.Time) bool {
	shard := int(key[0])
	return c.shards[shard].Touch(key, t)
}

func (c *Cache) Len() int {
	var sum int
	for i := 0; i < 256; i++ {
		sum += c.shards[i].Len()
	}
	return sum
}

func (c *Cache) Prune(now time.Time, staleThresh uint8) int {
	var remaining int
	for i := 0; i < 256; i++ {
		remaining += c.shards[i].Prune(now, staleThresh)
	}
	return remaining
}
