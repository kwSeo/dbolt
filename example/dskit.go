package example

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func StartDsKit(ctx context.Context, logger log.Logger) error {
	reg := prometheus.WrapRegistererWithPrefix("dskit_", prometheus.DefaultRegisterer)

	kvCfg := kv.Config{
		Store: "inmemory",
	}
	kvClient, err := kv.NewClient(kvCfg, ring.GetCodec(), reg, logger)
	if err != nil {
		return errors.Wrap(err, "failed to create KV client")
	}

	lCfg := ring.BasicLifecyclerConfig{
		NumTokens: 3,
	}
	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(1*time.Minute, delegate, logger)
	l, err := ring.NewBasicLifecycler(lCfg, "example", "key", kvClient, delegate, logger, reg)
	if err != nil {
		return errors.Wrap(err, "failed to create Lifecycler")
	}
	if err := l.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "failed to start asnyc lifecycler")
	}
	lAwaitTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := l.AwaitRunning(lAwaitTimeout); err != nil {
		return errors.Wrap(err, "failed to await lifecycler")
	}

	cfg := NewInMemoryConfig(logger)
	r, err := ring.NewWithStoreClientAndStrategy(cfg, "example", "key", kvClient, ring.NewDefaultReplicationStrategy(), reg, logger)
	if err != nil {
		return errors.Wrap(err, "failed to create Ring")
	}
	if err = r.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "failed to start async ring")
	}
	rAwaitTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.AwaitRunning(rAwaitTimeout); err != nil {
		return errors.Wrap(err, "failed to await ring")
	}

	keys := []uint32{4, 5}
	err = ring.DoBatch(ctx, ring.Read, r, keys, func(id ring.InstanceDesc, i []int) error {
		return level.Info(logger).Log("id", id.String(), "i", intsToString(i))
	}, func() { level.Info(logger).Log("msg", "Completed") })
	if err != nil {
		return errors.Wrap(err, "failed to do batch on the ring")
	}
	return nil
}

type TestFlush struct {
	logger log.Logger
}

// Flush implements ring.FlushTransferer
func (tf *TestFlush) Flush() {
	level.Info(tf.logger).Log("msg", "Called Flush function.")
}

// TransferOut implements ring.FlushTransferer
func (tf *TestFlush) TransferOut(ctx context.Context) error {
	return level.Info(tf.logger).Log("msg", "Called TransferOut function.")
}

func NewMemberlistConfig(logger log.Logger) ring.Config {
	return ring.Config{
		ReplicationFactor: 1,
		KVStore: kv.Config{
			Store: "memberlist",
			StoreConfig: kv.StoreConfig{
				MemberlistKV: func() (*memberlist.KV, error) {
					dnsProvider := dns.NewProvider(logger, prometheus.DefaultRegisterer, dns.GolangResolverType)
					memCfg := memberlist.KVConfig{
						AdvertiseAddr:     "127.0.0.1",
						AdvertisePort:     7123,
						RandomizeNodeName: true,
						JoinMembers:       flagext.StringSlice{"127.0.0.1:7123"},
						Codecs:            []codec.Codec{ring.GetCodec()},
					}
					return memberlist.NewKV(memCfg, logger, dnsProvider, prometheus.DefaultRegisterer), nil
				},
			},
		},
		HeartbeatTimeout: 500 * time.Millisecond,
	}
}

func NewInMemoryConfig(_ log.Logger) ring.Config {
	return ring.Config{
		ReplicationFactor: 1,
		KVStore: kv.Config{
			Store: "inmemory",
		},
	}
}

func intsToString(nums []int) string {
	result := ""
	for _, num := range nums {
		result += strconv.Itoa(num)
	}
	return result
}
