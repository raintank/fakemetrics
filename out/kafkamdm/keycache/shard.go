package keycache

import (
	"sync"
	"time"

	schema "gopkg.in/raintank/schema.v1"
)

// stamp is a compacted way to represent a timestamp
// bucketed in 10minutely buckets, which allows us to cover
// a timeframe of 42 hours (just over a day and a half)
// 6 (10m periods per hour) *42 (hours) = 252 (10m periods)
type Stamp uint8

// callers responsability that t >= ref and t-ref <= 42 hours
func NewStamp(ref Ref, t time.Time) Stamp {
	unix := t.Unix()
	diff := uint32(unix/600) - uint32(ref)
	return Stamp(diff)
}

// ref is simply a unix timestamp bucketed in 10m buckets
type Ref uint32

func NewRef(t time.Time) Ref {
	unix := t.Unix()
	return Ref(unix / 600)
}

type Shard struct {
	sync.Mutex
	ref Ref
	m   map[SubKey]Stamp
}

func NewShard(ref Ref) Shard {
	return Shard{
		ref: ref,
		m:   make(map[SubKey]Stamp),
	}
}

// callers responsability that t >= ref and t-ref <= 42 hours
func (s *Shard) Touch(key schema.Key, t time.Time) bool {
	var sub SubKey
	copy(sub[:], key[1:])
	s.Lock()
	_, ok := s.m[sub]
	s.m[sub] = NewStamp(s.ref, t)
	s.Unlock()
	return ok
}

func (s *Shard) Len() int {
	s.Lock()
	l := len(s.m)
	s.Unlock()
	return l
}

// important that we update ref of the shard at least every 42 hours
// so that stamp doesn't overflow
func (s *Shard) Prune(now time.Time, diff uint8) int {
	newRef := NewRef(now)
	var remaining int
	s.Lock()

	// the amount to subtract of a stamp for it to be based on the new reference
	subtract := newRef - s.ref

	for subkey, stamp := range s.m {
		// remove entry if it is too old, e.g. if:
		// newRef - diff > "timestamp of the entry in 10minutely buckets"
		// newRef - diff > ref + stamp
		if uint32(newRef)-uint32(diff) > uint32(s.ref)+uint32(stamp) {
			delete(s.m, subkey)
			continue
		}

		// note that the update formula is only correct in these 2 cases:
		// 1) it does not underflow.
		// this must: stamp - subtract >= 0
		// or: stap >= subtract (1)
		// and we already know from above:
		// newRef - diff <= ref + stamp (2)
		// we also know that subtract == newRef - ref.
		// putting this into (1):
		// stamp >= newRef - ref
		// filling that into (2):
		// newRef - diff <= ref + newRef - ref
		// - diff <= 0
		// diff >= 0
		// 2) the result fits into a uint8. but since we decrease the amount, to a new >= 0 amount,
		// we know it does

		// we know subtract fits into a Stamp since we call Prune at least every 42 hours
		s.m[subkey] = stamp - Stamp(subtract)
		remaining++
	}
	s.ref = newRef
	s.Unlock()
	return remaining
}
