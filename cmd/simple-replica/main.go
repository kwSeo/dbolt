package main

import (
	"context"
	"flag"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	ringName = "test-ring"
	ringKey  = "test-ring-key"
)

func main() {
	logger := log.With(log.NewLogfmtLogger(os.Stdout), "ts", log.DefaultTimestamp, "caller", log.DefaultCaller)
	promReg := prometheus.DefaultRegisterer
	ctx := context.Background()

	inf := flag.String("network-interface", "eth0", "")
	join := flag.String("join", "memberlist", "")
	listenAddr := flag.String("listen-addr", ":8080", "")
	flag.Parse()

	level.Info(logger).Log("msg", "Starting server")

	memberlistConfig := getMemberlistConfig([]string{*join}, 7946)
	config, err := getConfig([]string{*inf})
	if err != nil {
		fatal(logger, err)
	}

	dnsProvider := dns.NewProvider(log.With(logger, "component", "dnsProvider"), promReg, dns.GolangResolverType)
	memberlistService := memberlist.NewKVInitService(&memberlistConfig, log.With(logger, "component", "memberlist"), dnsProvider, promReg)
	level.Info(logger).Log("msg", "Initialized Memberlist for Gossip")

	if err := memberlistService.StartAsync(ctx); err != nil {
		fatal(logger, err)
	}
	//if err := memberlistService.AwaitRunning(ctx); err != nil {
	//	fatal(logger, err)
	//}
	level.Info(logger).Log("msg", "Started Memberlist KV")

	config.Lifecycler.RingConfig.KVStore.MemberlistKV = memberlistService.GetMemberlistKV

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
	//if err := r.AwaitRunning(ctx); err != nil {
	//	fatal(logger, err)
	//}
	level.Info(logger).Log("msg", "Started Ring.")

	if err := lc.StartAsync(ctx); err != nil {
		fatal(logger, err)
	}
	//if err := lc.AwaitRunning(ctx); err != nil {
	//	fatal(logger, err)
	//}
	level.Info(logger).Log("msg", "Started Lifecycler.")

	//for {
	//	if err := lc.CheckReady(ctx); err != nil {
	//		level.Info(logger).Log("msg", "Waiting for ready...")
	//		time.Sleep(1 * time.Second)
	//		continue
	//	}
	//
	//	rs, err := r.GetAllHealthy(ring.Reporting)
	//	if err != nil {
	//		fatal(logger, err)
	//	}
	//	for _, addr := range rs.GetAddresses() {
	//		level.Info(logger).Log("msg", "Found a instance.", "addr", addr)
	//	}
	//	break
	//}

	http.HandleFunc("/do-batch", func(w http.ResponseWriter, req *http.Request) {
		timeout, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		err = ring.DoBatch(timeout, ring.Read, r, []uint32{1}, func(desc ring.InstanceDesc, k []int) error {
			level.Info(logger).Log("msg", "Do batch on ring.", "desc", desc.String(), "key", intsToString(k))
			return nil
		}, func() {
			level.Info(logger).Log("msg", "Called cleanup function of ring. Do nothing.")
		})
		if err != nil {
			level.Error(logger).Log("msg", "Failed to doBatch.", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.Handle("/memberlist", memberlistService)
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		level.Info(logger).Log("msg", "Received unexpected request.")
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(*listenAddr, nil); err != nil {
		fatal(logger, err)
	}
}

type Config struct {
	Lifecycler ring.LifecyclerConfig
	Memberlist memberlist.KVConfig
}

func getMemberlistConfig(joinMembers []string, advertisePort int) memberlist.KVConfig {
	return memberlist.KVConfig{
		NodeName:                         "", // Use default. Default is hostname.
		RandomizeNodeName:                true,
		StreamTimeout:                    10 * time.Second,
		RetransmitMult:                   4,
		JoinMembers:                      joinMembers,
		MinJoinBackoff:                   3 * time.Second,
		MaxJoinBackoff:                   5 * time.Second,
		MaxJoinRetries:                   10,
		AbortIfJoinFails:                 false,
		RejoinInterval:                   5 * time.Second,
		LeftIngestersTimeout:             5 * time.Minute,
		LeaveTimeout:                     20 * time.Second,
		GossipInterval:                   200 * time.Millisecond,
		GossipNodes:                      3,
		PushPullInterval:                 30 * time.Second,
		GossipToTheDeadTime:              30 * time.Second,
		DeadNodeReclaimTime:              0,
		MessageHistoryBufferBytes:        0,
		EnableCompression:                false,
		AdvertiseAddr:                    "", // Use default. Default is 0.0.0.0.
		ClusterLabel:                     "", // Not used in simple-replica.
		ClusterLabelVerificationDisabled: true,
		Codecs:                           []codec.Codec{ring.GetCodec()},
		AdvertisePort:                    advertisePort,
		TCPTransport: memberlist.TCPTransportConfig{
			BindAddrs:          []string{"0.0.0.0"},
			BindPort:           advertisePort,
			PacketDialTimeout:  2 * time.Second,
			PacketWriteTimeout: 5 * time.Second,
			TransportDebug:     false,
			TLSEnabled:         false,
		},
	}
}

func getConfig(infNames []string) (Config, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return Config{}, errors.Wrap(err, "failed to get hostname")
	}
	return Config{
		Lifecycler: ring.LifecyclerConfig{
			RingConfig: ring.Config{
				KVStore: kv.Config{
					Store:  "memberlist",
					Prefix: "dbolt/",
					StoreConfig: kv.StoreConfig{
						MemberlistKV: nil, // lazy init
					},
					Mock: nil,
				},
				HeartbeatTimeout:     time.Minute,
				ReplicationFactor:    3,
				ZoneAwarenessEnabled: false,
				ExcludedZones:        nil,
				SubringCacheDisabled: true,
			},
			NumTokens:        128,
			HeartbeatPeriod:  15 * time.Second,
			HeartbeatTimeout: time.Minute,
			ObservePeriod:    0,
			JoinAfter:        0,
			MinReadyDuration: 5 * time.Second,
			InfNames:         infNames,
			EnableInet6:      false,
			FinalSleep:       0,
			TokensFilePath:   "test-tokens",
			//Zone:                     "", Not used
			UnregisterOnShutdown:     true,
			ReadinessCheckRingHealth: false,
			// Below are properties for testing.
			//Addr:                     "",
			Port: 8080,
			ID:   hostname,
			//ListenPort:         0,
			//RingTokenGenerator: nil,
		},
	}, nil
}

func fatal(logger log.Logger, err error) {
	level.Error(logger).Log("err", err)
	os.Exit(1)
}

func intsToString(ints []int) string {
	result := ""
	for _, v := range ints {
		result += strconv.Itoa(v) + " "
	}
	return result
}

func logIfErr(logger log.Logger, err error) {
	if err != nil {
		level.Error(logger).Log("err", err)
	}
}
