package main

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	ringName = "test-ring"
	ringKey  = "test-ring-key"
)

func main() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Println("Closing...", sig)
		os.Exit(1)
	}()

	logger := log.With(log.NewLogfmtLogger(os.Stdout), "ts", log.DefaultTimestamp, "caller", log.DefaultCaller)
	promReg := prometheus.DefaultRegisterer
	ctx := context.Background()

	level.Info(logger).Log("msg", "Starting server")

	dnsProvider := dns.NewProvider(log.With(logger, "component", "dnsProvider"), promReg, dns.GolangResolverType)
	memberlistKV := memberlist.NewKV(memberlistConfig, log.With(logger, "component", "memberlist"), dnsProvider, promReg)
	level.Info(logger).Log("msg", "Initialized Memberlist for Gossip")

	if err := memberlistKV.StartAsync(ctx); err != nil {
		fatal(logger, err)
	}
	if err := memberlistKV.AwaitRunning(ctx); err != nil {
		fatal(logger, err)
	}
	level.Info(logger).Log("msg", "Started Memberlist KV")

	config.Lifecycler.RingConfig.KVStore.MemberlistKV = func() (*memberlist.KV, error) {
		level.Info(logger).Log("msg", "MemberlistKV used.")
		return memberlistKV, nil
	}

	r, err := ring.New(config.Lifecycler.RingConfig, ringName, ringKey, log.With(logger, "component", "ring"), promReg)
	if err != nil {
		fatal(logger, err)
	}
	level.Info(logger).Log("msg", "Initialized Ring.")

	lc, err := ring.NewLifecycler(config.Lifecycler, ring.NewNoopFlushTransferer(), ringName, ringKey, true, log.With(logger, "component", "lifecycler"), promReg)
	if err != nil {
		fatal(logger, err)
	}
	level.Info(logger).Log("msg", "Initialized Lifecycler.")

	if err := r.StartAsync(ctx); err != nil {
		fatal(logger, err)
	}
	if err := r.AwaitRunning(ctx); err != nil {
		fatal(logger, err)
	}
	level.Info(logger).Log("msg", "Started Ring.")

	if err := lc.StartAsync(ctx); err != nil {
		fatal(logger, err)
	}
	if err := lc.AwaitRunning(ctx); err != nil {
		fatal(logger, err)
	}
	level.Info(logger).Log("msg", "Started Lifecycler.")

	for {
		if err := lc.CheckReady(ctx); err != nil {
			level.Info(logger).Log("msg", "Waiting for ready...")
			time.Sleep(1 * time.Second)
			continue
		}

		rs, err := r.GetAllHealthy(ring.Reporting)
		if err != nil {
			fatal(logger, err)
		}
		for _, addr := range rs.GetAddresses() {
			level.Info(logger).Log("msg", "Found a instance.", "addr", addr)
		}
		break
	}
}

type Config struct {
	Lifecycler ring.LifecyclerConfig
	Memberlist memberlist.KVConfig
}

var memberlistConfig = memberlist.KVConfig{
	JoinMembers:      []string{"memberlist"},
	AbortIfJoinFails: true,
	RejoinInterval:   5 * time.Second,
	Codecs:           []codec.Codec{ring.GetCodec()},
}

var config = Config{
	Lifecycler: ring.LifecyclerConfig{
		RingConfig: ring.Config{
			KVStore: kv.Config{
				Store: "memberlist",
				StoreConfig: kv.StoreConfig{
					MemberlistKV: nil, // lazy init
				},
			},
			HeartbeatTimeout:     1 * time.Second,
			ReplicationFactor:    1,
			ZoneAwarenessEnabled: false,
			ExcludedZones:        nil,
			SubringCacheDisabled: true,
		},
		NumTokens:        128,
		HeartbeatPeriod:  1 * time.Second,
		HeartbeatTimeout: 500 * time.Millisecond,
		ObservePeriod:    3 * time.Second,
		JoinAfter:        1 * time.Second,
		MinReadyDuration: 1 * time.Second,
		InfNames:         []string{"en0"},
		FinalSleep:       3 * time.Second,
		TokensFilePath:   "test-tokens",
		//Zone:                     "", Not used
		UnregisterOnShutdown:     true,
		ReadinessCheckRingHealth: true,
		// Below are properties for testing.
		//Addr:                     "",
		Port: 8080,
		//ID:                       "",

		// Injected internally
		//ListenPort:               0,
	},
}

func fatal(logger log.Logger, err error) {
	level.Error(logger).Log("err", err)
	os.Exit(1)
}
