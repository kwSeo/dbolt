package main

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

func StartBoltdb(logger log.Logger, dbFile string) error {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	// Read-write transcation
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return errors.Wrap(err, "failed to create or get a bucket")
		}
		return bucket.Put([]byte("key"), []byte("value"))
	})
	if err != nil {
		return errors.Wrap(err, "failed to create a bucket and put key-value pair")
	}

	// Read-only transcation
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("test"))
		if bucket == nil {
			return errors.Wrap(err, "bucket not existed")
		}
		value := bucket.Get([]byte("key"))
		level.Info(logger).Log("value", value)
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to get a bucket and value")
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("test"))
		if bucket == nil {
			return errors.Wrap(err, "bucket not existed")
		}
		return bucket.Delete([]byte("key"))
	})
	if err != nil {
		return errors.Wrap(err, "failed to delete a key-value pair")
	}
	return nil
}
