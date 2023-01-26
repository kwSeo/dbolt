package kvstorage

import (
	"os"

	"github.com/boltdb/bolt"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type BoltKV struct {
	db     *bolt.DB
	logger log.Logger
}

func New(db *bolt.DB, logger log.Logger) *BoltKV {
	return &BoltKV{
		db:     db,
		logger: logger,
	}
}

func Open(path string, mode os.FileMode, options *bolt.Options, logger log.Logger) (*BoltKV, error) {
	boltdb, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create BoltDB")
	}
	return New(boltdb, logger), nil
}

func (b *BoltKV) Get(bucketName, key []byte) ([]byte, error) {
	var value []byte
	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return errors.Wrapf(err, "failed to create or get bucket in view : bucketName=%s", string(bucketName))
		}
		value = bucket.Get(key)
		return nil

	}); err != nil {
		return nil, err
	}
	return value, nil
}

func (b *BoltKV) Put(bucketName, key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return errors.Wrapf(err, "failed to create or get bucket in update : bucketName=%s", string(bucketName))
		}
		if err := bucket.Put(key, value); err != nil {
			return errors.Wrapf(err, "failed to put key-value : key=%s value=%s", string(key), string(value))
		}
		return nil
	})
}

func (b *BoltKV) Close() error {
	return b.db.Close()
}
