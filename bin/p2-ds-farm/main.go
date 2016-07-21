package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"

	ds_farm "github.com/square/p2/pkg/ds"
)

func main() {
	_, consulOpts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(consulOpts)
	logger := logging.NewLogger(logrus.Fields{})
	dsStore := dsstore.NewConsul(client, 3, &logger)
	kpStore := kp.NewConsulStore(client)
	applicator := labels.NewConsulApplicator(client, 3)

	dsf := ds_farm.NewFarm(kpStore, dsStore, applicator, logger, nil)

	// WARNING: Uncommenting this block will allow you to replicate
	// I'm just using the replication package out of the box, I currently do not
	// know enough about what it does
	//
	// Please see pkg/ds/daemon_set.go's publishToReplication() function for more details
	//
	healthChecker := checker.NewConsulHealthChecker(client)
	dsf.SetFarmHealthChecker(&healthChecker)

	quitCh := make(chan struct{})
	dsf.Start(quitCh)
}
