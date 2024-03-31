package dbolt

import (
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/kwSeo/dbolt/pkg/dbolt/httpserver"
	"github.com/kwSeo/dbolt/pkg/util"
	"github.com/pkg/errors"
)

type Config struct {
	BoltConfig       BoltConfig            `yaml:"bolt"`
	ServerConfig     httpserver.Config     `yaml:"server"`
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler"`
	MemberlistConfig memberlist.KVConfig   `yaml:"memberlist"`
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
