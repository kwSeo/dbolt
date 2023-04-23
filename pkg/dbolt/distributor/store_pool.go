package distributor

import "context"

type Store interface {
	Get(ctx context.Context, bucket, key []byte) ([]byte, error)
	Put(ctx context.Context, bucket, key, value []byte) error
}

type SimpleStorePool struct {
	m map[string]Store
}

func NewSimpleStorePool() *SimpleStorePool {
	return &SimpleStorePool{
		m: make(map[string]Store),
	}
}

func (sp *SimpleStorePool) Get(key string) Store {
	return sp.m[key]
}

func (sp *SimpleStorePool) Register(key string, store Store) {
	sp.m[key] = store
}
