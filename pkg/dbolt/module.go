package dbolt

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/kwSeo/dbolt/pkg/dbolt/distributor"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

type Config struct {
	RingConfig       ring.Config
	LifecyclerConfig ring.BasicLifecyclerConfig
	KvConfig         kv.Config
}

type App struct {
	fxApp *fx.App
	cfg   Config
}

func NewApp(cfg Config) *App {
	fxApp := fx.New(
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),
		fx.Provide(func() Config {
			return cfg
		}),
		fx.Provide(
			zap.NewExample,
			initGoKitLogger,
			initPrometheusRegistry,
			initKvClient,
			initRing,
			initBasicLifecycler,
			initStorePool,
			initDistributor,
		),
	)
	return &App{
		fxApp: fxApp,
	}
}

func (a *App) Run() {
	a.fxApp.Run()
}

func initGoKitLogger(logger *zap.Logger) log.Logger {
	return &ZapGoKitLogger{logger: logger}
}

func initPrometheusRegistry() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}

func initKvClient(cfg Config, reg prometheus.Registerer, logger log.Logger) (kv.Client, error) {
	kvClient, err := kv.NewClient(cfg.KvConfig, ring.GetCodec(), reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create KV client")
	}
	return kvClient, nil
}

func initRing(fl fx.Lifecycle, cfg Config, kvClient kv.Client, reg prometheus.Registerer, logger log.Logger) (*ring.Ring, error) {
	r, err := ring.NewWithStoreClientAndStrategy(cfg.RingConfig, distributor.RingName, distributor.RingKey, kvClient, ring.NewDefaultReplicationStrategy(), reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ring")
	}

	fl.Append(fx.StartStopHook(
		func(ctx context.Context) error {
			err := r.StartAsync(ctx)
			if err != nil {
				return err
			}
			return r.AwaitRunning(ctx)
		},
		func() {
			r.StopAsync()
		},
	))

	return r, nil
}

func initBasicLifecycler(fl fx.Lifecycle, r *ring.Ring, cfg Config, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, cfg.LifecyclerConfig.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(1*time.Minute, delegate, logger)
	basicLifecycler, err := ring.NewBasicLifecycler(cfg.LifecyclerConfig, "example", "key", r.KVClient, delegate, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Lifecycler")
	}

	fl.Append(fx.StartStopHook(
		func(ctx context.Context) error {
			err := basicLifecycler.StartAsync(ctx)
			if err != nil {
				return err
			}
			return basicLifecycler.AwaitRunning(ctx)
		},
		func() {
			basicLifecycler.StopAsync()
		},
	))

	return basicLifecycler, nil
}

func initStorePool() *distributor.SimpleStorePool {
	return distributor.NewSimpleStorePool()
}

func initDistributor(lc *ring.Lifecycler, r *ring.Ring, sp *distributor.SimpleStorePool, logger *zap.Logger) *distributor.Distributor {
	return distributor.New(lc, r, sp, logger)
}
