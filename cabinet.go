package cabinet;

import (
	"base64"
	"net/url"
	"fmt"
	"strings"
	"encoding/json"
	"encoding/binary"
	"encoding/hex"
	"github.com/boltdb/bolt"
	)

// Types (except errors)

type Cabinet struct {
	db *bolt.DB
}

type Key struct {
	kind string
	key string
}

type IndexTransaction struct {
	boltTx *bolt.Tx
	unindexing bool
	entityKind string
	entityKey string
}

type Indexer interface {
	Index(*IndexTransaction) error
}

type ViewTransaction struct {
	boltTx *bolt.Tx
}
type UpdateTransaction struct {
	ViewTransaction
}

type QueryHandler interface {
	QueryHandle(Key) (bool, error)
}

// Top-level Cabinet operations

type ErrBoltFailure struct {
	operation string
	base error
}
func (e ErrBoltFailure) Error() string {
	return fmt.Sprint("cabinet: ", e.operation, ": ", e.base.Error())
}

func (cabinet *Cabinet) RegisterKind(string kind) error {
	return cabinet.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprint("e", kind)))
		if err != nil {
			return ErrBoltFailure{"create entity bucket", err}
		}
		return nil
	})
}
// indexName cannot include slashes!
func (cabinet *Cabinet) RegisterIndex(string kind, string indexName) error {
	return cabinet.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprint("i", kind, "/", indexName)))
		if err != nil {
			return ErrBoltFailure{"create index bucket", err}
		}
		return nil
	})
}

type Options {
	mode int
	boltOptions bolt.Options
}

func Open(filename string, options *Options) (*Cabinet, error) {
	if options.mode == 0 {
		options.mode = 0600
	}
	db, err := bolt.Open(filename, options.mode, options.boltOptions)
	if err != nil {
		return nil, ErrBoltFailure{"open database", err}
	}
	return &Cabinet{db}, nil
}

// Transactions

func (cabinet *Cabinet) View(main func(*ViewTransaction) error) error {
	return cabinet.db.View(func(tx *bolt.Tx) error {
		return main(ViewTransaction{tx})
	})
}
func (cabinet *Cabinet) Update(main func(*UpdateTransaction) error) error {
	return cabinet.db.Update(func(tx *bolt.Tx) error {
		return main(UpdateTransaction{ViewTransaction{tx}})
	})
}

// Indexing

// example index b:k:v
//    iExampleKind/indexName:HEXindexValue/entityKey:entityKey
func (tx *IndexTransaction) IndexRaw(indexName string, indexValue []byte) error {
	b := tx.boltTx.Bucket([]byte(fmt.Sprint("i", tx.entityKind, "/", indexName))
	realKey := fmt.Sprint(hex.EncodeToString(indexValue), "/", tx.entityKey)
	if tx.unindexing {
		return b.Delete(realKey)
	} else {
		return b.Put(realKey, []byte(tx.entityKey))
	}
}
func (tx *IndexTransaction) IndexString(indexName string, indexValue string) error {
	return tx.IndexRaw(indexName, byte[](indexValue))
}
func (tx *IndexTransaction) IndexInteger(indexName string, indexValue int64) error {
	var output [8]byte
	binary.PutVarint(output, indexValue)
	return tx.IndexRaw(indexName, output)
}

// Querying (indexes)

func (cabinet *Cabinet) QueryRawExact(indexName string, indexValue []byte, ) error {
	keyBase := fmt.Sprint(hex.EncodeToString(indexValue), "/")
	
	WORKING HERE...

func (cabinet *Cabinet) QueryRawPrefix(indexName string, indexValuePrefix []byte) {
	keyBase := hex.EncodeToString(indexValuePrefix)
	WORKING HERE...

// Key access

type ErrJSONFailure struct {
	operation string
	base error
}
func (e ErrJSONFailure) Error() string {
	return fmt.Sprint("cabinet: ", e.operation, ": ", e.base.Error())
}
type ErrIndexFailure struct {
	operation string
	base error
}
func (e ErrIndexFailure) Error() string {
	return fmt.Sprint("cabinet: ", e.operation, ": ", e.base.Error())
}

func (cabinet *Cabinet) Get(key Key, v Indexer) (bool, error) {
	bucketName := []byte(fmt.Sprint("e", key.kind))
	found := false
	err := cabinet.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		data := bucket.Get([]byte(key.key))
		if data != nil {
			found = true
			err := json.Unmarshal(data, v)
			if err != nil {
				return ErrJSONFailure{"unmarshal entity", err}
			}
		}
		return nil
	})
	if err != nil {
		return false, ErrBoltFailure{"get key", err}
	}
	return found, nil
}

func (cabinet *Cabinet) Put(key Key, v Indexer, old Indexer) error {
	encoded, err := json.Marshal(v)
	if err != nil {
		return ErrJSONFailure{"marshal entity", v}
	}
	bucketName := []byte(fmt.Sprint("e", key.kind))
	err := cabinet.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		data := bucket.Get([]byte(key.key))
		if data != nil {
			err := json.Unmarshal(data, old)
			if err != nil {
				return ErrJSONFailure{"unmarshal old entity", err}
			}
			err := old.Index(IndexTransaction{tx, true, key.kind, key.key})
			if err != nil {
				return ErrIndexFailure{"unindex old entity", err}
			}
		}
		err := v.Index(IndexTransaction{tx, false, key.kind, key.key})
		if err != nil {
			return ErrIndexFailure{"index new entity", err}
		}
		return bucket.Put([]byte(key.key), encoded)
	})
	if err != nil {
		return ErrBoltFailure{"put key", err}
	}
	return nil
}

func (cabinet *Cabinet) Delete(key Key, v Indexer) error {
	bucketName := []byte(fmt.Sprint("e", key.kind))
	err := cabinet.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		data := bucket.Get([]byte(key.key))
		if data != nil {
			err := json.Unmarshal(data, v)
			if err != nil {
				return ErrJSONFailure{"unmarshal old entity", err}
			}
			err := v.Index(IndexTransaction{tx, true, key.kind, key.key})
			if err != nil {
				return ErrIndexFailure{"unindex old entity", err}
			}
		}
		return bucket.Delete([]byte(key.key))
	})
	if err != nil {
		return ErrBoltFailure{"delete key", err}
	}
	return nil
}

// URLSafing for keys

type ErrInvalidURLSafeKey struct {
	urldata string
	issue string
}
func (e ErrInvalidURLSafeKey) Error() string {
	return fmt.Sprint("cabinet: invalid URLSafe key: ", e.urldata, ": " e.issue)
}

func (key Key) URLSafe() string {
	kind := base64.URLEncoding.EncodeToString([]byte(key.kind))
	key := base64.URLEncoding.EncodeToString([]byte(key.key))
	return url.QueryEscape(fmt.Sprint(kind, ".", key))
}

func URLSafeKey(string urldata) (Key, err) {
	split := strings.Split(url.QueryUnescape(urldata), ".")
	if len(split) != 2 {
		return nil, ErrInvalidURLSafeKey{urldata, "incorrect number of separators"}
	}
	kind, err := base64.URLEncoding.DecodeString(split[0])
	if err != nil {
		return nil, ErrInvalidURLSafeKey{urldata, err.Error()}
	}
	key, err := base64.URLEncoding.DecodeString(split[1])
	if err != nil {
		return nil, ErrInvalidURLSafeKey{urldata, err.Error()}
	}
	return Key{kind, key}, err
}

