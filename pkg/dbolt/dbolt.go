package dbolt

import (
	"context"
	"os"

	"github.com/boltdb/bolt"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RingName = "dbolt"
	RingKey  = "dbolt-key"
)

type DB struct {
	boltdb     *bolt.DB
	lifecycler *ring.Lifecycler
	ring       *ring.Ring
}

func Open(path string, mode os.FileMode, options *Options, logger log.Logger, reg prometheus.Registerer) (*DB, error) {
	lifecycler, err := ring.NewLifecycler(options.Lifecycler, ring.NewNoopFlushTransferer(), RingName, RingKey, true, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Lifecycler")
	}
	dboltRing, err := ring.NewWithStoreClientAndStrategy(options.Lifecycler.RingConfig, RingName, RingKey, lifecycler.KVStore, ring.NewDefaultReplicationStrategy(), reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ring of dbolt")
	}
	boltdb, err := bolt.Open(path, mode, &options.Bolt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create boltDB")
	}
	return &DB{
		boltdb:     boltdb,
		lifecycler: lifecycler,
		ring:       dboltRing,
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
	return db.boltdb.Close()
}

func (db *DB) Get(ctx context.Context, key []byte) ([]byte, error) {
	token := []uint32{TokenFromBytes(key)}
	var value []byte
	err := ring.DoBatch(ctx, ring.Read, db.ring, token, func(id ring.InstanceDesc, i []int) error {
		// TODO:
		return nil
	}, func() {

	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get value by key: key=%s", string(key))
	}
	return value, nil
}

func TokenFromBytes(bytes []byte) uint32 {
	var token uint32 = 0
	for _, b := range bytes {
		token += uint32(b)
	}
	return token
}
