package ds

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/types"
)

type Status int

const (
	StatusNotStarted Status = iota
	StatusInProgress
	StatusCompleted
)

func statusToString(s Status) string {
	switch s {
	case StatusNotStarted:
		return "created"
	case StatusInProgress:
		return "started"
	default:
		return "unknown"
	}
}

func (ds *daemonSet) updateStatus(s Status, failedNodes []types.NodeName) error {
	statusDocument := map[string]string{
		"status":      statusToString(s),
		"failedNodes": fmt.Sprintf("%v", failedNodes),
	}
	jsonBytes, err := json.Marshal(statusDocument)
	if err != nil {
		return err
	}
	if err != nil {
		ds.logger.WithErrorAndFields(err, logrus.Fields{
			"dsID": ds.ID(),
			"pod":  ds.PodID(),
		}).Errorln("Unable to persist status for this daemon set.")
	}

	return ds.statusStore.SetStatus(statusstore.DS,
		statusstore.ResourceID(ds.ID()),
		StatusNamespace,
		jsonBytes)
}
