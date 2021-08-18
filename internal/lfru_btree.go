package internal

import (
	"sync"
	"github.com/google/btree"
	"github.com/jacobsa/fuse/fuseops"
)

type LFRU struct {
	mu sync.Mutex
	popularThreshold int64
	maxHits int64
	ageInterval int64
	ageDecrement int64
	toAge int64
	maxRecency uint64
	items map[fuseops.InodeID]*LFRUItem
	index *btree.BTree
}

func NewLFRU(popularThreshold int64, maxHits int64, ageInterval int64, ageDecrement int64) *LFRU {
	if maxHits < popularThreshold {
		maxHits = popularThreshold
	}
	return &LFRU{
		popularThreshold: popularThreshold,
		maxHits: maxHits,
		ageInterval: ageInterval,
		ageDecrement: ageDecrement,
		toAge: ageInterval,
		items: make(map[fuseops.InodeID]*LFRUItem),
		index: btree.New(32),
	}
}

func (c *LFRU) Hit(id fuseops.InodeID, hits int64) {
	oldRecency := uint64(0)
	c.mu.Lock()
	item := c.items[id]
	if item == nil {
		if hits > c.maxHits {
			hits = c.maxHits
		}
		item = &LFRUItem{
			id: id,
			popular: hits >= c.popularThreshold,
			hits: hits,
			recency: c.maxRecency,
		}
		c.items[id] = item
		c.index.ReplaceOrInsert(item)
	} else {
		oldRecency = item.recency
		newHits := item.hits+hits
		if newHits > c.maxHits {
			newHits = c.maxHits
		}
		if newHits != item.hits {
			c.index.Delete(item)
			item.hits = newHits
			item.popular = item.hits >= c.popularThreshold
			item.recency = c.maxRecency
			c.index.ReplaceOrInsert(item)
		}
	}
	if !item.popular && oldRecency != c.maxRecency {
		c.maxRecency++
	}
	c.toAge--
	if hits > 0 && c.toAge <= 0 {
		c.toAge = c.ageInterval
		c.ageAll(c.ageDecrement)
	}
	c.mu.Unlock()
}

func (c *LFRU) Pick(prev *LFRUItem) *LFRUItem {
	var next *LFRUItem
	c.mu.Lock()
	if prev == nil {
		c.index.Ascend(func(ai btree.Item) bool {
			next = ai.(*LFRUItem)
			return false
		})
	} else {
		c.index.AscendGreaterOrEqual(prev, func(ai btree.Item) bool {
			a := ai.(*LFRUItem)
			if a != prev {
				next = a
				return false
			}
			return true
		})
	}
	c.mu.Unlock()
	return next
}

func (c *LFRU) Forget(id fuseops.InodeID) {
	c.mu.Lock()
	item := c.items[id]
	if item != nil {
		c.index.Delete(item)
		delete(c.items, id)
	}
	c.mu.Unlock()
}

func (c *LFRU) ageAll(decrement int64) {
	newIndex := btree.New(32)
	c.index.Ascend(func(ai btree.Item) bool {
		a := ai.(*LFRUItem)
		ah := a.hits-decrement
		if ah < 0 {
			ah = 0
		}
		newIndex.ReplaceOrInsert(a)
		return true
	})
	c.index = newIndex
}

type LFRUItem struct {
	id fuseops.InodeID
	popular bool
	hits int64
	recency uint64
}

func (a *LFRUItem) Less(bi btree.Item) bool {
	if a == nil {
		return true
	}
	b := bi.(*LFRUItem)
	if a.popular != b.popular {
		return !a.popular
	}
	if a.popular {
		if a.hits != b.hits {
			return a.hits < b.hits
		}
	} else {
		if a.recency != b.recency {
			return a.recency < b.recency
		}
	}
	return a.id < b.id
}

func (a *LFRUItem) Id() fuseops.InodeID {
	return a.id
}

func (a *LFRUItem) Hits() int64 {
	return a.hits
}
