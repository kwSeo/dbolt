package dbolt

import (
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/kwSeo/dbolt/pkg/util"
	"github.com/pkg/errors"
)

type Config struct {
	BoltConfig       BoltConfig            `yaml:"bolt"`
	ServerConfig     ServerConfig          `yaml:"server"`
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
