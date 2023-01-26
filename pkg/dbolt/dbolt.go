package dbolt

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RingName = "dbolt"
	RingKey  = "dbolt-key"
)

var ErrKeyValueNotFound = errors.New("key-value not found")

type DB struct {
	kv         KVStorage
	lifecycler *ring.Lifecycler
	ring       *ring.Ring
	logger     log.Logger
}

func Open(options *Options, kv KVStorage, logger log.Logger, reg prometheus.Registerer) (*DB, error) {
	lifecycler, err := ring.NewLifecycler(options.Lifecycler, ring.NewNoopFlushTransferer(), RingName, RingKey, true, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Lifecycler")
	}
	dboltRing, err := ring.NewWithStoreClientAndStrategy(options.Lifecycler.RingConfig, RingName, RingKey, lifecycler.KVStore, ring.NewDefaultReplicationStrategy(), reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ring of dbolt")
	}
	return &DB{
		kv:         kv,
		lifecycler: lifecycler,
		ring:       dboltRing,
		logger:     logger,
	}, nil
}

func (db *DB) StartAsync(ctx context.Context) error {
	if err := db.lifecycler.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "failed to start Lifecycler")
	}
	if err := db.ring.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "failed to start Ring")
	}
	return nil
}

func (db *DB) StopAsync() {
	db.lifecycler.StopAsync()
	db.ring.StopAsync()
}

func (db *DB) AwaitingTerminated(ctx context.Context) error {
	if err := db.lifecycler.AwaitTerminated(ctx); err != nil {
		return errors.Wrap(err, "failed to await Lifecycler terminated")
	}
	if err := db.ring.AwaitTerminated(ctx); err != nil {
		return errors.Wrap(err, "failed to await Ring terminated")
	}
	return nil
}

func (db *DB) Close() error {
	db.StopAsync()
	db.AwaitingTerminated(context.Background())
	return db.kv.Close()
}

func (db *DB) Get(ctx context.Context, bucketName, key []byte) ([]byte, error) {
	token := []uint32{TokenFromBytes(bucketName, key)}
	var versionedValues []*VersionedValue
	selfAddr := db.lifecycler.Addr
	
	if err := ring.DoBatch(ctx, ring.Read, db.ring, token, func(id ring.InstanceDesc, i []int) error {
		level.Debug(db.logger).Log("instanceAddr", id.Addr)
		if selfAddr == id.Addr {
			value, err := db.get(bucketName, key)
			if err != nil {
				return errors.Wrap(err, "failed to get value : key="+string(key))
			}
			versionedValues = append(versionedValues, value)

		} else {
			// TODO
		}
		return nil
	}, func() {
		// TODO
	}); err != nil {
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

func (db *DB) get(bucketName, key []byte) (*VersionedValue, error) {
	value, err := db.kv.Get(bucketName, key)
	if err != nil {
		return nil, err
	}
	versionedValue := new(VersionedValue)
	if err := json.Unmarshal(value, versionedValue); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the value")
	}
	return versionedValue, nil
}

func (db *DB) put(bucketName, key, value []byte) error {
	versionedValue := newVersionedValueNow(value)
	jsonValue, err := json.Marshal(versionedValue)
	if err != nil {
		return errors.Wrap(err, "failed to marshal the value")
	}
	return db.kv.Put(bucketName, key, jsonValue)
}

func TokenFromBytes(bytesArr ...[]byte) uint32 {
	var token uint32 = 0
	for _, bytes := range bytesArr {
		for _, b := range bytes {
			token += uint32(b)
		}
	}
	return token
}

type VersionedValue struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Value     []byte
}

func newVersionedValueNow(value []byte) *VersionedValue {
	now := time.Now()
	return &VersionedValue{
		CreatedAt: now,
		UpdatedAt: now,
		Value:     value,
	}
}
