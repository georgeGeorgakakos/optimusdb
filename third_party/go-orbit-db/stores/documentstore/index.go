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
//  3. No log traversal per write. A local write passes its new entry, which is applied directly. Other
//     updates (replication, load) read the log's entry map instead of Values(), which walked and sorted the
//     whole log on every call; the order does not matter because of (2).
//  4. Field indexes. For the top-level fields named in OPTIMUSDB_INDEX_FIELDS, the index keeps the keys of
//     the documents per string value, so an equality query reads only the matching documents.
//  5. Decoded-document cache. Up to OPTIMUSDB_DOC_CACHE_MB (default 128 MiB, estimated as three times the
//     stored size; 0 disables it) of decoded documents are kept per store, each tied to the
//     exact stored bytes it was decoded from, so a read does not parse the JSON again. Callers get a deep
//     copy and cannot change the cached value.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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

// decodedDoc is a cached decoded document and the stored bytes it was decoded from.
type decodedDoc struct {
	raw  []byte
	doc  map[string]interface{}
	cost int64
}

type documentIndex struct {
	iface.StoreIndex
	index   map[string][]byte
	clocks  map[string]stamp    // per key: clock of the winning PUT/PUTALL/DEL (DEL keeps a tombstone clock)
	seen    map[string]struct{} // hashes of entries already applied
	muIndex sync.RWMutex
	opts    *iface.CreateDocumentDBOptions

	fields    []string                                  // indexed top-level fields
	byField   map[string]map[string]map[string]struct{} // field -> string value -> keys
	fieldVals map[string]map[string]string              // key -> field -> string value (indexed fields only)
	fieldsOff bool                                      // a stored value was not JSON: field indexes are not used

	decoded    sync.Map // key -> decodedDoc
	decodedN   int64    // estimated bytes held by the cache
	decodedMax int64    // budget in bytes
}

func envFields() []string {
	v, ok := os.LookupEnv("OPTIMUSDB_INDEX_FIELDS")
	if !ok {
		v = "type,status,region,resource_id"
	}
	v = strings.TrimSpace(v)
	if v == "" || strings.EqualFold(v, "off") || strings.EqualFold(v, "false") {
		return nil
	}
	var out []string
	for _, f := range strings.Split(v, ",") {
		f = strings.TrimSpace(f)
		if f != "" && !strings.Contains(f, ".") {
			out = append(out, f)
		}
	}
	return out
}

// envCacheMax is the decoded-document cache budget in bytes.
func envCacheMax() int64 {
	if v := os.Getenv("OPTIMUSDB_DOC_CACHE_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n << 20
		}
	}
	return 128 << 20
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

// keysByField returns the keys of the documents whose top-level field has the string value. ok is false
// when the field is not indexed (the caller must then scan).
func (i *documentIndex) keysByField(field, value string) ([]string, bool) {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	if i.fieldsOff {
		return nil, false
	}
	vals, ok := i.byField[field]
	if !ok {
		return nil, false
	}
	set := vals[value]
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys, true
}

// keysByFields returns the keys of the documents that satisfy every equality on an indexed field (the
// intersection of the per-value key sets). ok is false when none of the fields is indexed.
func (i *documentIndex) keysByFields(equal map[string]string) ([]string, bool) {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	if i.fieldsOff {
		return nil, false
	}
	var sets []map[string]struct{}
	for f, v := range equal {
		vals, ok := i.byField[f]
		if !ok {
			continue
		}
		sets = append(sets, vals[v]) // nil (no document has the value) is an empty set
	}
	if len(sets) == 0 {
		return nil, false
	}
	smallest := 0
	for j := range sets {
		if len(sets[j]) < len(sets[smallest]) {
			smallest = j
		}
	}
	keys := make([]string, 0, len(sets[smallest]))
next:
	for k := range sets[smallest] {
		for j, set := range sets {
			if j == smallest {
				continue
			}
			if _, in := set[k]; !in {
				continue next
			}
		}
		keys = append(keys, k)
	}
	return keys, true
}

// hasKey reports whether a document is stored under key, without decoding it.
func (i *documentIndex) hasKey(key string) bool {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()
	_, ok := i.index[key]
	return ok
}

// cachedRef returns the cached decoded document itself (no copy) if it was decoded from exactly raw. Only for
// read-only use, such as evaluating a filter; anything returned to a caller must be copied first.
func (i *documentIndex) cachedRef(key string, raw []byte) (map[string]interface{}, bool) {
	v, ok := i.decoded.Load(key)
	if !ok {
		return nil, false
	}
	d := v.(decodedDoc)
	if !sameBytes(d.raw, raw) {
		return nil, false
	}
	return d.doc, true
}

// cachedDoc returns a copy of the decoded document for key if it was decoded from exactly raw.
func (i *documentIndex) cachedDoc(key string, raw []byte) (map[string]interface{}, bool) {
	v, ok := i.decoded.Load(key)
	if !ok {
		return nil, false
	}
	d := v.(decodedDoc)
	if !sameBytes(d.raw, raw) {
		return nil, false
	}
	return copyMap(d.doc), true
}

// cacheDoc keeps a copy of a document decoded from raw, while the cache budget allows. It holds the index
// read lock, so raw is checked against the current value and no write can remove the key meanwhile (every
// write of a key removes its cache entry under the write lock, so an entry present now is current).
func (i *documentIndex) cacheDoc(key string, raw []byte, doc map[string]interface{}) {
	if i.decodedMax <= 0 {
		return
	}
	cost := int64(len(raw))*3 + 256
	if atomic.LoadInt64(&i.decodedN)+cost > i.decodedMax {
		return
	}
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()
	if cur, ok := i.index[key]; !ok || !sameBytes(cur, raw) {
		return
	}
	if _, loaded := i.decoded.LoadOrStore(key, decodedDoc{raw: raw, doc: copyMap(doc), cost: cost}); !loaded {
		atomic.AddInt64(&i.decodedN, cost)
	}
}

func (i *documentIndex) uncache(key string) {
	if old, was := i.decoded.LoadAndDelete(key); was {
		atomic.AddInt64(&i.decodedN, -old.(decodedDoc).cost)
	}
}

func sameBytes(a, b []byte) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func copyValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return copyMap(t)
	case []interface{}:
		out := make([]interface{}, len(t))
		for j, x := range t {
			out[j] = copyValue(x)
		}
		return out
	default:
		return v
	}
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = copyValue(v)
	}
	return out
}

// stringFields extracts the string values of the indexed fields from a stored document.
func (i *documentIndex) stringFields(raw []byte) (map[string]string, bool) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, false
	}
	var out map[string]string
	for _, f := range i.fields {
		r, ok := top[f]
		if !ok || len(r) == 0 || r[0] != '"' {
			continue
		}
		var s string
		if err := json.Unmarshal(r, &s); err != nil {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(i.fields))
		}
		out[f] = s
	}
	return out, true
}

func (i *documentIndex) unindexFields(key string) {
	old, ok := i.fieldVals[key]
	if !ok {
		return
	}
	for f, v := range old {
		if set := i.byField[f][v]; set != nil {
			delete(set, key)
			if len(set) == 0 {
				delete(i.byField[f], v)
			}
		}
	}
	delete(i.fieldVals, key)
}

// setKey stores the winning value of a key. Called with muIndex held.
func (i *documentIndex) setKey(key string, raw []byte) {
	i.index[key] = raw
	i.uncache(key)
	if len(i.fields) == 0 || i.fieldsOff {
		return
	}
	i.unindexFields(key)
	vals, ok := i.stringFields(raw)
	if !ok {
		// Not JSON (custom marshaller): the field indexes could miss this document, so stop using them.
		i.fieldsOff = true
		i.byField = map[string]map[string]map[string]struct{}{}
		i.fieldVals = map[string]map[string]string{}
		return
	}
	if len(vals) == 0 {
		return
	}
	i.fieldVals[key] = vals
	for f, v := range vals {
		set := i.byField[f][v]
		if set == nil {
			set = map[string]struct{}{}
			i.byField[f][v] = set
		}
		set[key] = struct{}{}
	}
}

// delKey removes a key. Called with muIndex held.
func (i *documentIndex) delKey(key string) {
	delete(i.index, key)
	i.uncache(key)
	if len(i.fields) > 0 && !i.fieldsOff {
		i.unindexFields(key)
	}
}

// apply processes one log entry whose hash string is h. Called with muIndex held.
func (i *documentIndex) apply(e ipfslog.Entry, h string) error {
	if _, done := i.seen[h]; done {
		return nil
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
				i.setKey(k, opDoc.GetValue())
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
				i.setKey(*key, item.GetValue())
			} else {
				i.delKey(*key)
			}
			i.clocks[*key] = st
		}
	}

	i.seen[h] = struct{}{}
	return nil
}

func (i *documentIndex) UpdateIndex(oplog ipfslog.Log, entries []ipfslog.Entry) error {
	// A local write passes the entry it has just appended: apply only that entry.
	if len(entries) > 0 {
		i.muIndex.Lock()
		defer i.muIndex.Unlock()
		for _, e := range entries {
			if e == nil {
				continue
			}
			if err := i.apply(e, e.GetHash().String()); err != nil {
				return err
			}
		}
		return nil
	}

	// Otherwise apply every entry of the log not applied yet. The log's entry map is keyed by entry hash.
	all := oplog.GetEntries()
	keys := all.Keys()

	i.muIndex.Lock()
	defer i.muIndex.Unlock()

	for _, h := range keys {
		if _, done := i.seen[h]; done {
			continue
		}
		e := all.UnsafeGet(h)
		if e == nil {
			continue
		}
		if err := i.apply(e, h); err != nil {
			return err
		}
	}

	return nil
}

// newDocumentIndex Creates a new index for a Document Store
func newDocumentIndex(opts *iface.CreateDocumentDBOptions) iface.StoreIndex {
	idx := &documentIndex{
		index:      map[string][]byte{},
		clocks:     map[string]stamp{},
		seen:       map[string]struct{}{},
		opts:       opts,
		fields:     envFields(),
		byField:    map[string]map[string]map[string]struct{}{},
		fieldVals:  map[string]map[string]string{},
		decodedMax: envCacheMax(),
	}
	for _, f := range idx.fields {
		idx.byField[f] = map[string]map[string]struct{}{}
	}
	return idx
}

var _ iface.StoreIndex = &documentIndex{}
