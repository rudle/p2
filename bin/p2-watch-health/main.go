package main

import (
	"log"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/version"
)

var (
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	hooks        = kingpin.Flag("hook", "Watch hooks.").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	hc := checker.NewConsulHealthChecker(client)

	results := make(chan []*health.Result)
	errCh := make(chan error)
	quitCh := make(<-chan struct{})

	go hc.WatchHealth(results, errCh, quitCh)

	for {
		select {
		case result := <-results:
			log.Printf("health:\n\n %+v\n\n", result)
		case err := <-errCh:
			log.Fatalf("Error occurred while watchcing health: %s", err)
		}
	}
}
