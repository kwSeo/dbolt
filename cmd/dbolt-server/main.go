package main

import "github.com/kwSeo/dbolt/pkg/dbolt"

func main() {
	dbolt.NewApp(dbolt.Config{}).Run()
}
