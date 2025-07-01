package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	exampleprovider "github.com/spilliams/schema-terraform-provider/example/provider"
)

var (
	version string = "dev"
	commit  string = "unknown"
)

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	opts := providerserver.ServeOpts{
		// for development only
		Address: "demo.leuco.net/terraform-registry/schema",
		Debug:   debug,
	}

	err := providerserver.Serve(context.Background(), exampleprovider.New(version, commit), opts)
	if err != nil {
		log.Fatal(err.Error())
	}
}
