package main

import (
	"flag"
	"github.com/kwSeo/dbolt/pkg/dbolt"
)

func main() {
	configPath := flag.String("config-path", "./config.yml", "Configuration file path.")
	flag.Parse()

	dbolt.NewApp(*configPath).Run()
}
