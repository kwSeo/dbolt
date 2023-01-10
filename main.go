package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/kwSeo/dbolt/example"
)

func main() {
	logger := log.With(log.NewLogfmtLogger(os.Stdout), "ts", log.DefaultTimestamp, "caller", log.DefaultCaller)

	var dbFile string
	flag.StringVar(&dbFile, "db.file", "main.db", "")
	flag.Parse()

	level.Info(logger).Log("msg", "Starting test app")

	if err := example.StartBoltdb(logger, dbFile); err != nil {
		fatalWithError(logger, err)
	}
	if err := example.StartDsKit(context.Background(), logger); err != nil {
		fatalWithError(logger, err)
	}
}

func fatalWithError(logger log.Logger, err error) {
	level.Error(logger).Log("err", err)
	os.Exit(1)
}
