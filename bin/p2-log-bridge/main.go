package main

import (
	"io"
	"os"
	"os/exec"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/logbridge"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/version"
)

var (
	durableLogger = kingpin.Arg("exec", "An executable that logbridge will log to with durability").Required().String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	loggerCmd := exec.Command(*durableLogger)
	durablePipe, err := loggerCmd.StdinPipe()
	if err != nil {
		logging.DefaultLogger.WithError(err).Errorln("failed during logbridge setup")
		os.Exit(1)
	}

	go func(r io.Reader, durableWriter io.Writer, nonDurableWriter io.Writer, logger logging.Logger) {
		logbridge.Tee(r, durableWriter, nonDurableWriter, logger)
	}(os.Stdin, durablePipe, os.Stderr, logging.DefaultLogger)

	loggerCmd.Start()
	err = loggerCmd.Wait()
	if err != nil {
		logging.DefaultLogger.WithError(err)
		os.Exit(1)
	}
}
