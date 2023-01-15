package example

import (
	"context"
	"os"

	"github.com/go-kit/log"
)

func ExampleStartDsKit() {
	var logger = log.NewLogfmtLogger(os.Stdout)
	if err := StartDsKit(context.Background(), logger); err != nil {
		panic(err)
	}
}
