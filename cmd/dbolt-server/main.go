package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-kit/log"
)

func main() {
	logger := log.With(log.NewLogfmtLogger(os.Stdout), "ts", log.DefaultCaller, "caller", log.DefaultCaller)

	var bindAddr string
	flag.StringVar(&bindAddr, "server.http-port", "8090", "Server port")
	flag.Parse()

	http.HandleFunc("/v1/db", handleDB)
}

func handleDB(w http.ResponseWriter, r *http.Request) {
	// TODO:
}
