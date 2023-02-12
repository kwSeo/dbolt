package store

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

var ErrNoSuchStore = errors.New("no such store")

type Store interface {
	io.Closer
	Get(ctx context.Context, bucketName, key []byte) (value []byte, err error)
	Put(ctx context.Context, bucketName, key, value []byte) error
}

type Provider interface {
	Find(key string) (Store, error)
	Register(key string, store Store) error
}

type SimpleProvider struct {
	stores map[string]Store
}

func NewSimpleProvider() *SimpleProvider {
	return &SimpleProvider{}
}

func (sp *SimpleProvider) Find(key string) (Store, error) {
	store, ok := sp.stores[key]
	if !ok {
		return nil, ErrNoSuchStore
	}
	return store, nil
}

func (sp *SimpleProvider) Register(key string, store Store) error {
	sp.stores[key] = store
	return nil
}
