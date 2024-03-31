package httpserver

import (
	"context"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/kwSeo/dbolt/pkg/dbolt/distributor"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"net/http"
)

type Config struct {
	BindIP         string `yaml:"bind_ip"`
	HTTPListenPort uint16 `yaml:"http_listen_port"`
	GRPCListenPort uint16 `yaml:"grpc_listen_port"`
}

func (sc *Config) Validate() error {
	if sc.BindIP == "" {
		return errors.New("BindIP required")
	}
	return nil
}

type Server struct {
	cfg    *Config
	app    *fiber.App
	dist   *distributor.Distributor
	logger *zap.Logger
}

func New(cfg *Config, dist *distributor.Distributor, logger *zap.Logger) *Server {
	app := fiber.New(
		fiber.Config{
			ErrorHandler: nil,
			AppName:      "dbolt",
		},
	)
	return &Server{
		cfg:    cfg,
		dist:   dist,
		app:    app,
		logger: logger,
	}
}

func (s *Server) Start() error {
	s.logger.Info("Initializing HTTP server.")
	s.app.Use(logger.New())
	s.app.Use(healthcheck.New())
	s.app.Get("/api/v1/buckets/:bucket/:key", s.getValueByKey)
	s.app.Post("/api/v1/buckets/:bucket/:key", s.postValueByKey)

	addr := fmt.Sprintf("%v:%v", s.cfg.BindIP, s.cfg.HTTPListenPort)
	s.logger.Info("Starting HTTP server.", zap.String("bindAddress", addr))
	return s.app.Listen(addr)
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping HTTP server.")
	return s.app.ShutdownWithContext(ctx)
}

func (s *Server) getValueByKey(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")
	value, err := s.dist.Get(c.UserContext(), []byte(bucket), []byte(key))
	if err != nil {
		return errors.Wrapf(err, "failed to find a value by key, bucket=%v, key=%v", bucket, key)
	}

	if c.Accepts("application/json") != "" {
		return c.JSON(&GetValueResponse{Value: value})
	}
	return c.Send(value)
}

func (s *Server) postValueByKey(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")
	var req PostValueByKeyRequest
	if err := c.BodyParser(&req); err != nil {
		return errors.Wrap(err, "failed to parse the request body")
	}
	if err := s.dist.Put(c.UserContext(), []byte(bucket), []byte(key), []byte(req.Value)); err != nil {
		return errors.Wrapf(err, "failed to put the value by key, bucket=%v, key=%v, value=%v", bucket, key, req.Value)
	}
	return c.SendStatus(http.StatusOK)
}

type GetValueResponse struct {
	Value []byte
}

type PostValueByKeyRequest struct {
	Value string
}
