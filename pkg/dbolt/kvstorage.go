package dbolt

import (
	"context"
	"io"
)

type KVStorage interface {
	io.Closer
	Get(bucketName, key []byte) (value []byte, err error)
	Put(bucketName, key, value []byte) error
}

type RemoteKVStorage interface {
	Get(ctx context.Context, addr string, bucketName, key []byte) ([]byte, error)
	Put(ctx context.Context, addr string, bucketName, key, value []byte) error
}
