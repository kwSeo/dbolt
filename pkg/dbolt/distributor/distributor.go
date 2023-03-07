package distributor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
)

const (
	RingName = "dbolt"
	RingKey  = "dbolt-key"
)

var ErrKeyValueNotFound = errors.New("key-value not found")

type Distributor struct {
	lifecycler *ring.Lifecycler
	ring       *ring.Ring
	storePool  *StorePool
	logger     log.Logger
}

func New(lifecycler *ring.Lifecycler, ring *ring.Ring, storePool *StorePool, logger log.Logger) *Distributor {
	return &Distributor{
		lifecycler: lifecycler,
		ring:       ring,
		storePool:  storePool,
		logger:     logger,
	}
}

func (d *Distributor) tokenFromBytes(bytesArr ...[]byte) uint32 {
	var token uint32 = 0
	for _, bytes := range bytesArr {
		for _, b := range bytes {
			token += uint32(b)
		}
	}
	return token
}

func (d *Distributor) Get(ctx context.Context, bucketName, key []byte) ([]byte, error) {
	token := []uint32{d.tokenFromBytes(bucketName, key)}
	var versionedValues []*VersionedValue

	if err := ring.DoBatch(ctx, ring.Read, d.ring, token, func(id ring.InstanceDesc, _ []int) error {
		level.Debug(d.logger).Log("instanceAddr", id.Addr)
		store := d.storePool.Get(id.Addr)
		value, err := store.Get(ctx, bucketName, key)

		versionedValue, err := unmarshalVersionedValue(value)
		if err != nil {
			return err
		}
		versionedValues = append(versionedValues, versionedValue)
		return nil
	}, doNothing); err != nil {
		return nil, errors.Wrapf(err, "failed to get value by key: key=%s", string(key))
	}
	if len(versionedValues) == 0 {
		return nil, ErrKeyValueNotFound
	}

	var lastUpdated *VersionedValue
	for _, versioned := range versionedValues {
		if lastUpdated == nil || lastUpdated.UpdatedAt.Before(versioned.UpdatedAt) {
			lastUpdated = versioned
		}
	}
	return lastUpdated.Value, nil
}

func (d *Distributor) Put(ctx context.Context, bucketName, key, value []byte) error {
	token := []uint32{d.tokenFromBytes(bucketName, key)}
	versionedValue := newVersionedValueNow(value)
	marshaledVersionedValue, err := marshalVersionedValue(versionedValue)
	if err != nil {
		return err
	}

	if err := ring.DoBatch(ctx, ring.WriteNoExtend, d.ring, token, func(id ring.InstanceDesc, _ []int) error {
		level.Debug(d.logger).Log("instanceAddr", id.Addr)
		store := d.storePool.Get(id.Addr)
		return store.Put(ctx, bucketName, key, marshaledVersionedValue)
	}, doNothing); err != nil {
		return errors.Wrap(err, "failed to put key-value : key="+string(key))
	}
	return nil
}

type Store interface {
	Get(ctx context.Context, bucket, key []byte) ([]byte, error)
	Put(ctx context.Context, bucket, key, value []byte) error
}

type StorePool struct {
	m map[string]Store
}

func NewStorePool() *StorePool {
	return &StorePool{
		m: make(map[string]Store),
	}
}

func (sp *StorePool) Get(key string) Store {
	return sp.m[key]
}

func (sp *StorePool) Register(key string, store Store) {
	sp.m[key] = store
}
