package dbolt

import (
	"context"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/kwSeo/dbolt/pkg/dbolt/store"
	"github.com/kwSeo/dbolt/pkg/util"
	"gopkg.in/yaml.v2"
	"os"
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
	BoltConfig       BoltConfig            `yaml:"bolt"`
	ServerConfig     ServerConfig          `yaml:"server"`
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler"`
}

func (c *Config) Validate() error {
	return util.And(
		c.BoltConfig.Validate,
		c.ServerConfig.Validate,
	)
}

type BoltConfig struct {
	DB struct {
		Path string `yaml:"path"`
	} `yaml:"db"`
}

func (bc *BoltConfig) Validate() error {
	if bc.DB.Path == "" {
		return errors.New("bolt 'db.path' required")
	}
	return nil
}

type ServerConfig struct {
	BindIP         string `yaml:"bind_ip"`
	HTTPListenPort uint16 `yaml:"http_listen_port"`
	GRPCListenPort uint16 `yaml:"grpc_listen_port"`
}

func (sc *ServerConfig) Validate() error {
	if sc.BindIP == "" {
		return errors.New("BindIP required")
	}
	return nil
}

type App struct {
	fxApp *fx.App
}

func NewApp(configPath string) *App {
	fxApp := fx.New(
		fx.Provide(
			zap.NewDevelopment,
			fx.Annotate(initGoKitLogger, fx.As(new(log.Logger))),
			initPrometheusRegistry,
			initConfigLoader(configPath),
			initKvClient,
			fx.Annotate(initRing, fx.As(new(ring.ReadRing))),
			initBasicLifecycler,
			initBoltDB,
			initStorePool,
			initDistributor,
		),
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),
		fx.Invoke(func(dist *distributor.Distributor) {
			// TODO: Implement logic to start distributor.
		}),
	)
	return &App{
		fxApp: fxApp,
	}
}

func (a *App) Run() {
	a.fxApp.Run()
}

func initConfigLoader(path string) func() (*Config, error) {
	return func() (*Config, error) {
		file, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		config := new(Config)
		err = yaml.Unmarshal(file, config)
		if err != nil {
			return nil, err
		}

		err = config.Validate()
		if err != nil {
			return nil, err
		}
		return config, nil
	}
}

func initGoKitLogger(logger *zap.Logger) *ZapGoKitLogger {
	return &ZapGoKitLogger{logger: logger}
}

func initPrometheusRegistry() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}

func initKvClient(cfg *Config, reg prometheus.Registerer, logger *zap.Logger, goKitLogger log.Logger) (kv.Client, error) {
	logger.Info("Initializing KV Client.", zap.String("type", cfg.LifecyclerConfig.RingConfig.KVStore.Store))
	goKitLogger = log.With(goKitLogger, "service", "dskit-kv-client")

	kvClient, err := kv.NewClient(cfg.LifecyclerConfig.RingConfig.KVStore, ring.GetCodec(), reg, goKitLogger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create KV client")
	}
	return kvClient, nil
}

func initRing(fl fx.Lifecycle, lc *ring.BasicLifecycler, cfg *Config, kvClient kv.Client, reg prometheus.Registerer, logger log.Logger) (*ring.Ring, error) {
	logger = log.With(logger, "service", "dskit-ring")

	r, err := ring.NewWithStoreClientAndStrategy(
		cfg.LifecyclerConfig.RingConfig,
		distributor.RingName,
		distributor.RingKey,
		kvClient,
		ring.NewDefaultReplicationStrategy(),
		reg,
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ring")
	}

	fl.Append(fx.StartStopHook(
		func(ctx context.Context) error {
			err := lc.AwaitRunning(ctx)
			if err != nil {
				return err
			}
			err = r.StartAsync(ctx)
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

func initBasicLifecycler(fxLc fx.Lifecycle, kvClient kv.Client, cfg *Config, logger *zap.Logger, goKitLogger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	goKitLogger = log.With(goKitLogger, "service", "dskit-lifecycler")

	hostname, err := os.Hostname()
	if err != nil {
		logger.Error("Failed to read hostname.", zap.Error(err))
		return nil, err
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, cfg.LifecyclerConfig.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, goKitLogger)
	delegate = ring.NewAutoForgetDelegate(1*time.Minute, delegate, goKitLogger)

	basicCfg := ring.BasicLifecyclerConfig{
		ID:                              hostname,
		Addr:                            fmt.Sprintf("%s:%s", cfg.ServerConfig.BindIP, cfg.ServerConfig.GRPCListenPort),
		Zone:                            cfg.LifecyclerConfig.Zone,
		HeartbeatPeriod:                 cfg.LifecyclerConfig.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.LifecyclerConfig.HeartbeatTimeout,
		TokensObservePeriod:             cfg.LifecyclerConfig.ObservePeriod,
		NumTokens:                       cfg.LifecyclerConfig.NumTokens,
		KeepInstanceInTheRingOnShutdown: !cfg.LifecyclerConfig.UnregisterOnShutdown,
	}
	basicLifecycler, err := ring.NewBasicLifecycler(basicCfg, distributor.RingName, distributor.RingKey, kvClient, delegate, goKitLogger, reg)
	if err != nil {
		logger.Error("Failed to create BasicLifecycler.", zap.Error(err))
		return nil, err
	}

	fxLc.Append(fx.StartStopHook(
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

func initBoltDB(fxLc fx.Lifecycle, cfg *Config, logger *zap.Logger) (*bolt.DB, error) {
	// TODO: bolt.DefaultOptions 뿐만이 아니라 다른 옵션들도 사용할 수 있도록 개설 필요.
	db, err := bolt.Open(cfg.BoltConfig.DB.Path, os.ModePerm, bolt.DefaultOptions)
	if err != nil {
		logger.Error("Failed to create bolt DB.", zap.Error(err))
		return nil, err
	}
	return db, nil
}

func initStorePool(fxLc fx.Lifecycle, cfg *Config, lc *ring.BasicLifecycler, r ring.ReadRing, boltdb *bolt.DB, logger *zap.Logger) *distributor.SimpleStorePool {
	storePool := distributor.NewSimpleStorePool()
	registerMe := func() {
		localStore := store.NewLocalStore(boltdb, logger)
		storePool.Register(lc.GetInstanceAddr(), localStore)
	}

	fxLc.Append(fx.StartHook(func(ctx context.Context) error {
		replicationSet, err := r.GetAllHealthy(ring.Reporting)
		if errors.Is(err, ring.ErrEmptyRing) {
			registerMe()
			return nil

		} else if err != nil {
			return errors.Wrap(err, "failed to read all healthy instances")
		}

		for _, addr := range replicationSet.GetAddresses() {
			if addr == lc.GetInstanceAddr() {
				registerMe()
			} else {
				baseUrl := fmt.Sprintf("http://%s:%d", addr, cfg.ServerConfig.HTTPListenPort)
				httpStore := store.NewHTTPStoreWithDefault(baseUrl)
				storePool.Register(addr, httpStore)
			}
		}
		return nil
	}))
	return storePool
}

func initDistributor(lc *ring.BasicLifecycler, r ring.ReadRing, sp *distributor.SimpleStorePool, logger *zap.Logger) *distributor.Distributor {
	return distributor.New(lc, r, sp, logger)
}
