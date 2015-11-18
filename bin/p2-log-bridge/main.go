package main

import (
	"os"

	"github.com/square/p2/pkg/logbridge"
	"github.com/square/p2/pkg/logging"
)

func main() {
	logbridge.LossyCopy(os.Stdout, os.Stdin, 1024, logging.DefaultLogger)
}
