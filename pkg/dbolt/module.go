package dbolt

import (
	"context"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/kwSeo/dbolt/pkg/dbolt/httpserver"
	"github.com/kwSeo/dbolt/pkg/dbolt/store"
	"gopkg.in/yaml.v2"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/kwSeo/dbolt/pkg/dbolt/distributor"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

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
			initMemberlistService,
			fx.Annotate(initRing, fx.As(new(ring.ReadRing))),
			initLifecycler,
			initBoltDB,
			initStorePool,
			initDistributor,
			initHTTPServer,
		),
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),
		fx.Invoke(func(s *httpserver.Server) {
			// 애플리케이션을 트리거하기 위한 빈 함수
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

func initMemberlistService(cfg *Config, goKitLogger log.Logger, reg prometheus.Registerer) *memberlist.KVInitService {
	memberlistConfig := cfg.MemberlistConfig
	dnsProvider := dns.NewProvider(log.With(goKitLogger, "component", "dnsProvider"), reg, dns.GolangResolverType)
	return memberlist.NewKVInitService(&memberlistConfig, goKitLogger, dnsProvider, reg)
}

func initLifecycler(fxLc fx.Lifecycle, memberlistKVInitService *memberlist.KVInitService, cfg *Config, logger *zap.Logger, goKitLogger log.Logger, reg prometheus.Registerer) (*ring.Lifecycler, error) {
	goKitLogger = log.With(goKitLogger, "service", "dskit-lifecycler")

	cfg.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = memberlistKVInitService.GetMemberlistKV

	lifecycler, err := ring.NewLifecycler(cfg.LifecyclerConfig, ring.NewNoopFlushTransferer(), distributor.RingName, distributor.RingKey, false, goKitLogger, reg)
	if err != nil {
		logger.Error("Failed to create Lifecycler.", zap.Error(err))
		return nil, err
	}

	fxLc.Append(fx.StartStopHook(
		func(ctx context.Context) error {
			err := lifecycler.StartAsync(ctx)
			if err != nil {
				return err
			}
			return lifecycler.AwaitRunning(ctx)
		},
		func() {
			lifecycler.StopAsync()
		},
	))

	return lifecycler, nil
}

func initRing(fl fx.Lifecycle, cfg *Config, reg prometheus.Registerer, logger log.Logger) (*ring.Ring, error) {
	logger = log.With(logger, "service", "dskit-ring")

	r, err := ring.New(cfg.LifecyclerConfig.RingConfig, distributor.RingName, distributor.RingKey, logger, reg)
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

func initBoltDB(fxLc fx.Lifecycle, cfg *Config, logger *zap.Logger) (*bolt.DB, error) {
	// TODO: bolt.DefaultOptions 뿐만이 아니라 다른 옵션들도 사용할 수 있도록 개선 필요.
	db, err := bolt.Open(cfg.BoltConfig.DB.Path, os.ModePerm, bolt.DefaultOptions)
	if err != nil {
		logger.Error("Failed to create bolt DB.", zap.Error(err))
		return nil, err
	}
	return db, nil
}

func initStorePool(fxLc fx.Lifecycle, cfg *Config, lc *ring.Lifecycler, r ring.ReadRing, boltdb *bolt.DB, logger *zap.Logger) *distributor.SimpleStorePool {
	storePool := distributor.NewSimpleStorePool()

	fxLc.Append(fx.StartHook(func(ctx context.Context) error {
		go func() {
			for range time.Tick(5 * time.Second) {
				logger.Debug("Syncing store pool...")

				replicationSet, err := r.GetAllHealthy(ring.Reporting)
				if err != nil {
					logger.Error("Failed to read all healthy instances.", zap.Error(err))
					continue
				}

				myAddr := lc.Addr
				for _, addr := range replicationSet.GetAddresses() {
					if addr == myAddr {
						logger.Debug("Registering me.")
						localStore := store.NewLocalStore(boltdb, logger)
						storePool.Register(myAddr, localStore)

					} else if !storePool.Contains(addr) {
						logger.Debug("Registering store of member instance.", zap.String("addr", addr))
						baseUrl := fmt.Sprintf("http://%s:%d", addr, cfg.ServerConfig.HTTPListenPort)
						httpStore := store.NewHTTPStoreWithDefault(baseUrl)
						storePool.Register(addr, httpStore)
					}
				}
			}
		}()
		return nil
	}))

	return storePool
}

func initDistributor(r ring.ReadRing, sp *distributor.SimpleStorePool, logger *zap.Logger) *distributor.Distributor {
	return distributor.New(r, sp, logger)
}

func initHTTPServer(fxLc fx.Lifecycle, cfg *Config, dist *distributor.Distributor, logger *zap.Logger) *httpserver.Server {
	server := httpserver.New(&cfg.ServerConfig, dist, logger)
	fxLc.Append(fx.StartStopHook(server.Start, server.Stop))
	return server
}
