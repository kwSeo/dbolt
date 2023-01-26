package dbolt

import "io"

type KVStorage interface {
	io.Closer
	Get(bucketName, key []byte) (value []byte, err error)
	Put(bucketName, key, value []byte) error
}
