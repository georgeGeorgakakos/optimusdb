package documentstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"

	coreapi "github.com/ipfs/interface-go-ipfs-core"
)

type orbitDBDocumentStore struct {
	basestore.BaseStore
	docOpts *iface.CreateDocumentDBOptions
}

func (o *orbitDBDocumentStore) Get(ctx context.Context, key string, opts *iface.DocumentStoreGetOptions) ([]interface{}, error) {
	if opts == nil {
		opts = &iface.DocumentStoreGetOptions{}
	}

	hasMultipleTerms := strings.Contains(key, " ")
	if hasMultipleTerms {
		key = strings.ReplaceAll(key, ".", " ")
	}
	if opts.CaseInsensitive {
		key = strings.ToLower(key)
	}

	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to documentIndex")
	}

	// Patched for OptimusDB: an exact-key lookup reads the index entry directly instead of comparing the
	// key with every key in the store. Same result as the loop below for this case.
	if !opts.CaseInsensitive && !opts.PartialMatches && !hasMultipleTerms {
		value := docIndex.Get(key)
		if value == nil {
			return nil, nil
		}
		raw, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("invalid type for key %s", key)
		}
		out, err := o.decode(docIndex, key, raw)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal value for key %s: %w", key, err)
		}
		return []interface{}{out}, nil
	}

	documents := []interface{}(nil)

	for _, indexKey := range docIndex.Keys() {
		indexKeyForSearch := indexKey

		if opts.CaseInsensitive {
			indexKeyForSearch = strings.ToLower(indexKeyForSearch)
			if hasMultipleTerms {
				indexKeyForSearch = strings.ReplaceAll(indexKeyForSearch, ".", " ")
			}
		}

		if !opts.PartialMatches {
			if indexKeyForSearch != key {
				continue
			}
		} else if opts.PartialMatches {
			if !strings.Contains(indexKeyForSearch, key) {
				continue
			}
		}

		value := o.Index().Get(indexKey)
		if value == nil {
			return nil, fmt.Errorf("value not found for key %s", indexKey)
		}

		if _, ok := value.([]byte); !ok {
			return nil, fmt.Errorf("invalid type for key %s", indexKey)
		}

		out := o.docOpts.ItemFactory()
		if err := o.docOpts.Unmarshal(value.([]byte), &out); err != nil {
			return nil, fmt.Errorf("unable to unmarshal value for key %s: %w", indexKey, err)
		}

		documents = append(documents, out)
	}

	return documents, nil
}

func (o *orbitDBDocumentStore) Put(ctx context.Context, document interface{}) (operation.Operation, error) {
	key, err := o.docOpts.KeyExtractor(document)
	if err != nil {
		return nil, fmt.Errorf("unable to extract key from value: %w", err)
	}

	data, err := o.docOpts.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal value: %w", err)
	}

	op := operation.NewOperation(&key, "PUT", data)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

func (o *orbitDBDocumentStore) Delete(ctx context.Context, key string) (operation.Operation, error) {
	if e := o.Index().Get(key); e == nil {
		return nil, fmt.Errorf("no entry with key '%s' in database", key)
	}

	op := operation.NewOperation(&key, "DEL", nil)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

// PutBatch Add values as multiple operations and returns the latest
func (o *orbitDBDocumentStore) PutBatch(ctx context.Context, values []interface{}) (operation.Operation, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("nothing to add to the store")
	}

	op := operation.Operation(nil)
	var err error
	for _, val := range values {
		op, err = o.Put(ctx, val)
		if err != nil {
			return nil, fmt.Errorf("unable to add data to the store: %w", err)
		}
	}

	return op, nil
}

// PutAll Add values as a single operation and returns it
func (o *orbitDBDocumentStore) PutAll(ctx context.Context, values []interface{}) (operation.Operation, error) {
	toAdd := map[string][]byte{}

	for _, val := range values {
		key, err := o.docOpts.KeyExtractor(val)
		if err != nil {
			return nil, fmt.Errorf("one of the provided documents has no index key")
		}

		data, err := o.docOpts.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal one of the provided documents")
		}

		toAdd[key] = data
	}

	empty := ""
	op := operation.NewOperationWithDocuments(&empty, "PUTALL", toAdd)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

// decode returns the document stored under key as raw, from the decoded-document cache when possible.
// Patched for OptimusDB (see index.go, change 5).
func (o *orbitDBDocumentStore) decode(docIndex *documentIndex, key string, raw []byte) (interface{}, error) {
	if doc, ok := docIndex.cachedDoc(key, raw); ok {
		return doc, nil
	}
	value := o.docOpts.ItemFactory()
	if err := o.docOpts.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if m, ok := value.(map[string]interface{}); ok {
		docIndex.cacheDoc(key, raw, m)
	}
	return value, nil
}

// Query Finds documents using a filter function
func (o *orbitDBDocumentStore) Query(ctx context.Context, filter func(doc interface{}) (bool, error)) ([]interface{}, error) {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to documentIndex")
	}

	return o.filterKeys(docIndex, docIndex.Keys(), filter)
}

func (o *orbitDBDocumentStore) filterKeys(docIndex *documentIndex, keys []string, filter func(doc interface{}) (bool, error)) ([]interface{}, error) {
	documents := []interface{}(nil)
	for _, indexKey := range keys {
		doc := docIndex.Get(indexKey)
		if doc == nil {
			continue
		}
		raw := doc.([]byte)

		// A cached document is filtered as it is and copied only if it matches (most candidates of a scan do
		// not); the filter only reads it.
		if ref, ok := docIndex.cachedRef(indexKey, raw); ok {
			if match, err := filter(ref); err != nil {
				return nil, fmt.Errorf("error while filtering value: %w", err)
			} else if match {
				documents = append(documents, copyMap(ref))
			}
			continue
		}

		value, err := o.decode(docIndex, indexKey, raw)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal document: %w", err)
		}

		if ok, err := filter(value); err != nil {
			return nil, fmt.Errorf("error while filtering value: %w", err)
		} else if ok {
			documents = append(documents, value)
		}
	}

	return documents, nil
}

// rawMatches returns the stored JSON of the documents among keys that pass filter (the stored bytes are never
// modified, so they are returned without copying). Patched for OptimusDB: lets a query answer with the stored
// documents instead of decoding them and encoding them again.
func (o *orbitDBDocumentStore) rawMatches(docIndex *documentIndex, keys []string, filter func(doc interface{}) (bool, error)) ([][]byte, error) {
	var out [][]byte
	for _, k := range keys {
		v := docIndex.Get(k)
		if v == nil {
			continue
		}
		raw := v.([]byte)
		var doc interface{}
		if ref, ok := docIndex.cachedRef(k, raw); ok {
			doc = ref
		} else {
			d, err := o.decode(docIndex, k, raw)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal document: %w", err)
			}
			doc = d
		}
		match, err := filter(doc)
		if err != nil {
			return nil, fmt.Errorf("error while filtering value: %w", err)
		}
		if match {
			out = append(out, raw)
		}
	}
	return out, nil
}

// QueryRaw is Query returning the stored JSON of the matching documents. With equal, it reads only the
// documents selected by the field indexes (as QueryIndexed); otherwise, or when no field is indexed, it scans.
// Patched for OptimusDB.
func (o *orbitDBDocumentStore) QueryRaw(ctx context.Context, equal map[string]string, filter func(doc interface{}) (bool, error)) ([][]byte, error) {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to documentIndex")
	}
	keys, indexed := docIndex.keysByFields(equal)
	if len(equal) == 0 || !indexed {
		keys = docIndex.Keys()
	}
	return o.rawMatches(docIndex, keys, filter)
}

// QueryRawExact returns the stored JSON of the documents whose top-level fields equal the given strings, from
// the field indexes alone, without decoding any document. ok is false when a field is not indexed (the caller
// then uses QueryRaw). Patched for OptimusDB.
func (o *orbitDBDocumentStore) QueryRawExact(equal map[string]string) ([][]byte, bool) {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, false
	}
	return docIndex.rawsByFields(equal)
}

// GetRaw returns the stored JSON of the document under key (nil if none). Patched for OptimusDB.
func (o *orbitDBDocumentStore) GetRaw(key string) []byte {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil
	}
	if v, ok := docIndex.Get(key).([]byte); ok {
		return v
	}
	return nil
}

// HasKey reports whether a document is stored under key, without reading it. Patched for OptimusDB.
func (o *orbitDBDocumentStore) HasKey(key string) bool {
	docIndex, ok := o.Index().(*documentIndex)
	return ok && docIndex.hasKey(key)
}

// QueryIndexed is Query restricted to the documents whose top-level fields equal the given string values,
// read through the field indexes (index.go, change 4). The filter is still applied to every candidate, so
// the result is the same as Query with that filter whenever the filter implies the equalities. indexed is
// false, and nothing is read, when none of the fields is indexed; the caller then uses Query.
// Patched for OptimusDB.
func (o *orbitDBDocumentStore) QueryIndexed(ctx context.Context, equal map[string]string, filter func(doc interface{}) (bool, error)) ([]interface{}, bool, error) {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, false, fmt.Errorf("unable to cast index to documentIndex")
	}

	keys, found := docIndex.keysByFields(equal) // documents satisfying every indexed equality
	if !found {
		return nil, false, nil
	}

	docs, err := o.filterKeys(docIndex, keys, filter)
	return docs, true, err
}

func (o *orbitDBDocumentStore) Type() string {
	return "docstore"
}

func MapKeyExtractor(keyField string) func(obj interface{}) (string, error) {
	return func(obj interface{}) (string, error) {
		mapped, ok := obj.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("can't extract key from something else than a `map[string]interface{}` entry")
		}

		val, ok := mapped[keyField]
		if !ok {
			return "", fmt.Errorf("missing value for field `%s` in entry", keyField)
		}

		key, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("value for field `%s` is not a string", keyField)
		}

		return key, nil
	}
}

func DefaultStoreOptsForMap(keyField string) *iface.CreateDocumentDBOptions {
	return &iface.CreateDocumentDBOptions{
		Marshal:      json.Marshal,
		Unmarshal:    json.Unmarshal,
		KeyExtractor: MapKeyExtractor(keyField),
		ItemFactory:  func() interface{} { return map[string]interface{}{} },
	}
}

// NewOrbitDBDocumentStore Instantiates a new DocumentStore
func NewOrbitDBDocumentStore(ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) (iface.Store, error) {
	if options.StoreSpecificOpts == nil {
		options.StoreSpecificOpts = DefaultStoreOptsForMap("_id")
	}

	docOpts, ok := options.StoreSpecificOpts.(*iface.CreateDocumentDBOptions)
	if !ok {
		return nil, fmt.Errorf("invalid type supplied for opts.StoreSpecificOpts")
	}

	if docOpts.Marshal == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.Marshal")
	}

	if docOpts.Unmarshal == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.Unmarshal")
	}

	if docOpts.ItemFactory == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.ItemFactory")
	}

	if docOpts.KeyExtractor == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.ExtractKey")
	}

	store := &orbitDBDocumentStore{docOpts: docOpts}
	options.Index = func(_ []byte) iface.StoreIndex { return newDocumentIndex(docOpts) }

	err := store.InitBaseStore(ipfs, identity, addr, options)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize document store: %w", err)
	}

	return store, nil
}

var _ iface.DocumentStore = &orbitDBDocumentStore{}
