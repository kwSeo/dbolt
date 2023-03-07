package dbolt

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/kwSeo/dbolt/pkg/dbolt/store"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RingName = "dbolt"
	RingKey  = "dbolt-key"
)

var ErrKeyValueNotFound = errors.New("key-value not found")

type DB struct {
	selfKV     store.Store
	memberKV   store.Provider
	lifecycler *ring.Lifecycler
	ring       *ring.Ring
	logger     log.Logger
}

func Open(options *Options, selfKV store.Store, memberKV store.Provider, logger log.Logger, reg prometheus.Registerer) (*DB, error) {
	lifecycler, err := ring.NewLifecycler(options.Lifecycler, ring.NewNoopFlushTransferer(), RingName, RingKey, true, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Lifecycler")
	}
	dboltRing, err := ring.NewWithStoreClientAndStrategy(options.Lifecycler.RingConfig, RingName, RingKey, lifecycler.KVStore, ring.NewDefaultReplicationStrategy(), reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ring of dbolt")
	}
	return &DB{
		selfKV:     selfKV,
		memberKV:   memberKV,
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
	return db.selfKV.Close()
}

func (db *DB) Get(ctx context.Context, bucketName, key []byte) ([]byte, error) {
	token := []uint32{tokenFromBytes(bucketName, key)}
	var versionedValues []*VersionedValue
	selfAddr := db.lifecycler.Addr

	if err := ring.DoBatch(ctx, ring.Read, db.ring, token, func(id ring.InstanceDesc, _ []int) error {
		level.Debug(db.logger).Log("instanceAddr", id.Addr)
		var value []byte
		var err error
		if selfAddr == id.Addr {
			value, err = db.selfKV.Get(ctx, bucketName, key)
			if err != nil {
				return err
			}
		} else {
			foundStore, err := db.memberKV.Find(id.Addr)
			if err != nil {
				return err
			}
			value, err = foundStore.Get(ctx, bucketName, key)
			if err != nil {
				return err
			}
		}
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

func (db *DB) Put(ctx context.Context, bucketName, key, value []byte) error {
	token := []uint32{tokenFromBytes(bucketName, key)}
	selfAddr := db.lifecycler.Addr
	versionedValue := newVersionedValueNow(value)
	marshaledVersionedValue, err := marshalVersionedValue(versionedValue)
	if err != nil {
		return err
	}

	if err := ring.DoBatch(ctx, ring.WriteNoExtend, db.ring, token, func(id ring.InstanceDesc, _ []int) error {
		level.Debug(db.logger).Log("instanceAddr", id.Addr)
		if selfAddr == id.Addr {
			return db.selfKV.Put(ctx, bucketName, key, marshaledVersionedValue)
		} else {
			foundStore, err := db.memberKV.Find(id.Addr)
			if err != nil {
				return err
			}
			return foundStore.Put(ctx, bucketName, key, marshaledVersionedValue)
		}
	}, doNothing); err != nil {
		return errors.Wrap(err, "failed to put key-value : key="+string(key))
	}
	return nil
}

func tokenFromBytes(bytesArr ...[]byte) uint32 {
	var token uint32 = 0
	for _, bytes := range bytesArr {
		for _, b := range bytes {
			token += uint32(b)
		}
	}
	return token
}

func unmarshalVersionedValue(value []byte) (*VersionedValue, error) {
	versionedValue := new(VersionedValue)
	if err := json.Unmarshal(value, versionedValue); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the value")
	}
	return versionedValue, nil
}

func marshalVersionedValue(versionedValue *VersionedValue) ([]byte, error) {
	marshaled, err := json.Marshal(versionedValue)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal the value")
	}
	return marshaled, nil
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

func doNothing() {}
