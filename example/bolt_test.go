package example

import (
	"os"

	"github.com/go-kit/log"
)

func ExampleStartBoltdb() {
	var logger = log.NewLogfmtLogger(os.Stdout)
	if err := StartBoltdb(logger, "main.db"); err != nil {
		panic(err)
	}
}
