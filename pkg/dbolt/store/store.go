package store

import (
	"bytes"
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

type LocalStore struct {
	db     *bolt.DB
	logger *zap.Logger
}

func NewLocalStore(db *bolt.DB, logger *zap.Logger) *LocalStore {
	return &LocalStore{
		db:     db,
		logger: logger,
	}
}

func Open(path string, mode os.FileMode, options *bolt.Options, logger *zap.Logger) (*LocalStore, error) {
	boltdb, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create BoltDB")
	}
	return NewLocalStore(boltdb, logger), nil
}

func (ls *LocalStore) Get(ctx context.Context, bucketName, key []byte) ([]byte, error) {
	var value []byte
	if err := ls.db.View(func(tx *bolt.Tx) error {
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

func (ls *LocalStore) Put(ctx context.Context, bucketName, key, value []byte) error {
	return ls.db.Update(func(tx *bolt.Tx) error {
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

const contentType = "application/json"

type HTTPStore struct {
	client  *http.Client
	baseUrl string
}

func NewHTTPStore(cfg *HttpStoreConfig, baseUrl string) *HTTPStore {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}
	return &HTTPStore{
		client:  client,
		baseUrl: baseUrl,
	}
}

func (hs *HTTPStore) Get(ctx context.Context, bucketName, key []byte) ([]byte, error) {
	reqBody := &GetReq{
		BucketName: bucketName,
		Key:        key,
	}
	marshaled, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	content := bytes.NewBuffer(marshaled)
	resp, err := hs.client.Post(hs.baseUrl+"/v1/internal/get", contentType, content)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (hs *HTTPStore) Put(ctx context.Context, bucketName, key, value []byte) error {
	reqBody := &PutReq{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
	}
	marshaled, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	content := bytes.NewBuffer(marshaled)
	resp, err := http.Post(hs.baseUrl+"/v1/internal/put", contentType, content)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

type HttpStoreConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

type GetReq struct {
	BucketName []byte `json:"bucketName"`
	Key        []byte `json:"key"`
}

type PutReq struct {
	BucketName []byte `json:"bucketName"`
	Key        []byte `json:"key"`
	Value      []byte `json:"value"`
}
