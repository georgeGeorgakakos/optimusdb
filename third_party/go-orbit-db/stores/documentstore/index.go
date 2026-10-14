package documentstore

// Patched for OptimusDB (see third_party/README.md):
//
//  1. PUTALL bug fix. Upstream v1.21.0 marked the operation key (always "") as handled instead of each
//     document key, so an older PUT/PUTALL of the same key, processed later, overwrote the newer value.
//     That bug is why OptimusDB inserted documents one by one.
//  2. Incremental index. Upstream re-parsed the whole operation log on every write (O(N) per write,
//     O(N^2) for a bulk load). Each entry is now parsed once; a key keeps the value of the entry with the
//     highest Lamport clock (time, then writer ID), the same last-writer-wins order the log uses. The
//     result does not depend on the order in which entries are applied, so replicated entries that arrive
//     late or out of order give the same index as a full rebuild.

import (
	"bytes"
	"fmt"
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
)

// stamp is the Lamport clock of the entry that last wrote (or deleted) a key.
type stamp struct {
	time int
	id   []byte
}

// newer reports whether clock (time, id) is later than the stamp already recorded for a key.
func newer(cur stamp, ok bool, time int, id []byte) bool {
	if !ok {
		return true
	}
	if time != cur.time {
		return time > cur.time
	}
	return bytes.Compare(id, cur.id) > 0
}

type documentIndex struct {
	iface.StoreIndex
	index   map[string][]byte
	clocks  map[string]stamp    // per key: clock of the winning PUT/PUTALL/DEL (DEL keeps a tombstone clock)
	seen    map[string]struct{} // hashes of entries already applied
	muIndex sync.RWMutex
	opts    *iface.CreateDocumentDBOptions
}

func (i *documentIndex) Keys() []string {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	keys := make([]string, len(i.index))

	idx := 0
	for key := range i.index {
		keys[idx] = key
		idx++
	}

	return keys
}

func (i *documentIndex) Get(key string) interface{} {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	if i.index == nil {
		return nil
	}

	entry, ok := i.index[key]
	if !ok {
		return nil
	}

	return entry
}

func (i *documentIndex) UpdateIndex(oplog ipfslog.Log, _ []ipfslog.Entry) error {
	entries := oplog.Values().Slice()

	i.muIndex.Lock()
	defer i.muIndex.Unlock()

	for _, e := range entries {
		h := e.GetHash().String()
		if _, done := i.seen[h]; done {
			continue
		}

		item, err := operation.ParseOperation(e)
		if err != nil {
			return fmt.Errorf("unable to parse log documentstore operation: %w", err)
		}

		var t int
		var id []byte
		if c := e.GetClock(); c != nil {
			t, id = c.GetTime(), c.GetID()
		}
		st := stamp{time: t, id: id}

		switch item.GetOperation() {
		case "PUTALL":
			for _, opDoc := range item.GetDocs() {
				k := opDoc.GetKey()
				if k == "" {
					continue
				}
				if cur, ok := i.clocks[k]; newer(cur, ok, t, id) {
					i.index[k] = opDoc.GetValue()
					i.clocks[k] = st
				}
			}

		case "PUT", "DEL":
			key := item.GetKey()
			if key == nil || *key == "" {
				break // ignoring entries with nil or empty keys
			}
			if cur, ok := i.clocks[*key]; newer(cur, ok, t, id) {
				if item.GetOperation() == "PUT" {
					i.index[*key] = item.GetValue()
				} else {
					delete(i.index, *key)
				}
				i.clocks[*key] = st
			}
		}

		i.seen[h] = struct{}{}
	}

	return nil
}

// newDocumentIndex Creates a new index for a Document Store
func newDocumentIndex(opts *iface.CreateDocumentDBOptions) iface.StoreIndex {
	return &documentIndex{
		index:  map[string][]byte{},
		clocks: map[string]stamp{},
		seen:   map[string]struct{}{},
		opts:   opts,
	}
}

var _ iface.StoreIndex = &documentIndex{}
