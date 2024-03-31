package main

import (
	"context"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	accesslogger "github.com/gofiber/fiber/v2/middleware/logger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"time"
)

func main() {
	fxApp := fx.New(
		fx.Provide(
			zap.NewDevelopment,
			initTester,
			initFiber,
		),
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),
		fx.Invoke(func(_ *fiber.App) {
			// do nothing
		}),
		fx.StartTimeout(5*time.Second),
	)

	fxApp.Run()
}

func initTester(fxLc fx.Lifecycle, logger *zap.Logger) *Tester {
	tester := &Tester{}
	fxLc.Append(fx.StartHook(func() error {
		logger.Info("Called tester.Sleep.")
		tester.Sleep()
		return nil
	}))
	return tester
}

func initFiber(fxLc fx.Lifecycle, _ *Tester, logger *zap.Logger) *fiber.App {
	logger.Info("Initializing Go Fiber.")
	app := fiber.New()
	app.Use(accesslogger.New())
	app.Use(healthcheck.New())

	app.Get("/echo", func(c *fiber.Ctx) error {
		msg := c.Params("message")
		logger.Info("Received message.", zap.String("message", msg))
		return c.SendString(msg)
	})

	fxLc.Append(fx.StartStopHook(
		func() error {
			logger.Info("Starting Go Fiber.")
			go func() {
				app.Listen(":8080")
			}()
			return nil
		},
		func(ctx context.Context) error {
			logger.Info("Stopping Go Fiber.")
			return app.ShutdownWithContext(ctx)
		},
	))
	return app
}

type Tester struct {
}

func (*Tester) Sleep() {
	n := 0
	for range time.Tick(1 * time.Second) {
		fmt.Println("...tick...")
		n++
		if n == 3 {
			return
		}
	}
}
