package dbolt

import (
	"github.com/boltdb/bolt"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
)

type Options struct {
	Name       string
	Bolt       bolt.Options
	Lifecycler ring.LifecyclerConfig
	Memberlist memberlist.KVConfig
}
