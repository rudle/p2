package ds

import (
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/ds/fields"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	kpStore    kp.Store
	dsStore    dsstore.Store
	scheduler  scheduler.Scheduler
	applicator labels.Applicator

	children map[fields.ID]*childDS
	childMu  sync.Mutex

	logger  logging.Logger
	alerter alerting.Alerter
}

type childDS struct {
	ds        DaemonSet
	quitCh    chan<- struct{}
	updatedCh chan<- *ds_fields.DaemonSet
	deletedCh chan<- *ds_fields.DaemonSet
	errCh     <-chan error
}

func NewFarm(
	kpStore kp.Store,
	dsStore dsstore.Store,
	applicator labels.Applicator,
	logger logging.Logger,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		kpStore:    kpStore,
		dsStore:    dsStore,
		scheduler:  scheduler.NewApplicatorScheduler(applicator),
		applicator: applicator,
		children:   make(map[fields.ID]*childDS),
		logger:     logger,
		alerter:    alerter,
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	dsf.cleanupDaemonSetPods(quitCh)
	dsf.mainLoop(quitCh)
}

const cleanupInterval = 60 * time.Second

// This function removes all pods with a DSIDLabel where the daemon set id does
// not exist in the store at every interval specified because it is possible
// that the farm will unexpectedly crash or someone deletes or modifies a node
func (dsf *Farm) cleanupDaemonSetPods(quitCh <-chan struct{}) {
	go func() {
		timer := time.NewTimer(time.Duration(0))

		for {
			select {
			case <-quitCh:
				return
			case <-timer.C:
			}
			timer.Reset(cleanupInterval)

			allDaemonSets, err := dsf.dsStore.List()
			if err != nil {
				dsf.logger.NoFields().Errorf("Unable to get daemon sets from intent tree: %v", err)
				continue
			}

			dsIDMap := make(map[fields.ID]ds_fields.DaemonSet)
			for _, dsFields := range allDaemonSets {
				dsIDMap[dsFields.ID] = dsFields
			}

			dsIDLabelSelector := klabels.Everything().
				Add(DSIDLabel, klabels.ExistsOperator, []string{})

			allPods, err := dsf.applicator.GetMatches(dsIDLabelSelector, labels.POD)
			if err != nil {
				dsf.logger.NoFields().Error(err)
				continue
			}

			for _, podLabels := range allPods {
				// Only check if it is a pod scheduled by a daemon set
				dsID := podLabels.Labels.Get(DSIDLabel)

				// Check if the daemon set exists, if it doesn't unschedule the pod
				if _, ok := dsIDMap[fields.ID(dsID)]; !ok {
					nodeName, podID, err := labels.NodeAndPodIDFromPodLabel(podLabels)
					if err != nil {
						dsf.logger.NoFields().Error(err)
						continue
					}

					// TODO: Since this mirrors the unschedule function in daemon_set.go,
					// We should find a nice way to couple them together
					dsf.logger.NoFields().Infof("Unscheduling from '%s' with dangling daemon set uuid '%s'", nodeName, dsID)

					_, err = dsf.kpStore.DeletePod(kp.INTENT_TREE, nodeName, podID)
					if err != nil {
						dsf.logger.NoFields().Errorf("Unable to delete pod id '%v' from intent tree: %v", dsID, err)

						if alertErr := dsf.alerter.Alert(alerting.AlertInfo{
							Description: fmt.Sprintf("Unable to delete the following pod from store: %v", podID),
							IncidentKey: "pod_store_delete_error",
						}); alertErr != nil {
							dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
						}
						continue
					}

					id := labels.MakePodLabelKey(nodeName, podID)
					err = dsf.applicator.RemoveLabel(labels.POD, id, DSIDLabel)
					if err != nil {
						dsf.logger.NoFields().Errorf("Error removing ds pod id label '%v': %v", id, err)
					}
				}
			}
		}
	}()
}

func (dsf *Farm) mainLoop(quitCh <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	dsWatch := dsf.dsStore.Watch(subQuit)

	defer dsf.closeAllChildren()

	var changes dsstore.WatchedDaemonSets
	var err error
	var ok bool

	for {
		select {
		case <-quitCh:
			return
		case changes, ok = <-dsWatch:
			if !ok {
				return
			}
		}

		// This loop will check all the error channels of the children spawn by this
		// farm, if any child outputs an error, close the child.
		// The quitCh here will also return if the caller to mainLoop closes it
		for dsID, child := range dsf.children {
			select {
			case <-quitCh:
				return
			case err, ok = <-child.errCh:
				if !ok {
					// child error channel closed
					dsf.closeChild(dsID)
					continue
				}
				dsf.logger.Errorf("An error has occurred in spawned ds '%v':, %v", child.ds, err)
				if alertErr := dsf.alerter.Alert(alerting.AlertInfo{
					Description: fmt.Sprintf("An error has occurred in spawned ds '%v':, %v", child.ds, err),
					IncidentKey: "child_ds_error",
				}); alertErr != nil {
					dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
				}
				continue
			default:
			}
		}
		dsf.handleDSChanges(changes)
	}
}

func (dsf *Farm) closeAllChildren() {
	for dsID := range dsf.children {
		dsf.closeChild(dsID)
	}
}

func (dsf *Farm) closeChild(dsID fields.ID) {
	if child, ok := dsf.children[dsID]; ok {
		close(child.quitCh)
		close(child.updatedCh)
		close(child.deletedCh)
		delete(dsf.children, dsID)
	}
}

func (dsf *Farm) handleDSChanges(changes dsstore.WatchedDaemonSets) {
	if changes.Err != nil {
		dsf.logger.Infof("An error has occurred while watching daemon sets: %v", changes.Err)
		return
	}

	if len(changes.Created) > 0 {
		dsf.logger.Infof("The following daemon sets have been created:")
		for _, dsFields := range changes.Created {
			dsf.logger.Infof("%v", *dsFields)

			// If the daemon set contends with another daemon set, disable it
			// if that fails, delete it, if that fails too, raise an alert
			dsIDContended, isContended, err := dsf.dsContends(dsFields)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
				continue
			}

			if isContended {
				dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
				newDS, err := dsf.dsStore.Disable(dsFields.ID)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to delete daemon set: %v", err)
					continue
				}
				dsf.children[newDS.ID] = dsf.spawnDaemonSet(&newDS)
			} else {
				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields)
			}
		}
	}

	if len(changes.Updated) > 0 {
		dsf.logger.Infof("The following daemon sets have been updated:")
		for _, dsFields := range changes.Updated {
			dsf.logger.Infof("%v", *dsFields)

			// If the daemon set contends with another daemon set, disable it
			// if that fails, delete it, if that fails too, raise an alert
			if _, ok := dsf.children[dsFields.ID]; ok {
				dsIDContended, isContended, err := dsf.dsContends(dsFields)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
					continue
				}

				if isContended {
					dsf.logger.Errorf("Updated daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
					newDS, err := dsf.dsStore.Disable(dsFields.ID)
					if err != nil {
						dsf.logger.Errorf("Error occurred when trying to delete daemon set: %v", err)
						continue
					}
					dsf.children[newDS.ID].updatedCh <- &newDS
				} else {
					dsf.children[dsFields.ID].updatedCh <- dsFields
				}
			}
		}
	}

	if len(changes.Deleted) > 0 {
		dsf.logger.Infof("The following daemon sets have been deleted:")
		for _, dsFields := range changes.Deleted {
			dsf.logger.Infof("%v", *dsFields)
			if child, ok := dsf.children[dsFields.ID]; ok {
				child.deletedCh <- dsFields
				select {
				case err := <-child.errCh:
					if err != nil {
						dsf.logger.Errorf("Error occurred when deleting spawned daemon set: %v", err)
					}
					dsf.closeChild(dsFields.ID)
				}
			}
		}
	}
}

// Naive implementation of a guard, this checks if any of the scheduled nodes
// are used by two daemon sets, does not pre-emptively catch overlaps by selector
// because of how kubernetes selectors work
//
// Also naively checks the selectors to see if there are any selector overlap
// if two label selectors are labels.Everything()
//
// Returns [ daemon set contended, contention exists, error ]
func (dsf *Farm) dsContends(dsFields *ds_fields.DaemonSet) (ds_fields.ID, bool, error) {
	// This daemon set does not contend if it is disabled
	if dsFields.Disabled {
		return "", false, nil
	}
	// Get all eligible nodes for this daemon set by looking at the labes.NODE tree
	eligibleNodes, err := dsf.scheduler.EligibleNodes(dsFields.Manifest, dsFields.NodeSelector)
	if err != nil {
		return "", false, util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	// If this daemon set has a node selector set to Everything, check the labels
	// of other daemon sets
	for _, child := range dsf.children {
		everythingSelector := klabels.Everything().String()
		if !child.ds.IsDisabled() && child.ds.PodID() == dsFields.PodID && child.ds.ID() != dsFields.ID {
			// Naively check if both selectors are the Everything selector plus
			// something else or if both selectors are the same
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and {} (the everything selector)
			// even though the first selector doesn't select anything
			//
			// If either the child or the current daemon set has the everything selector
			// then they contend
			if dsFields.NodeSelector.String() == everythingSelector ||
				child.ds.GetNodeSelector().String() == everythingSelector {
				dsf.raiseContentionAlert(child.ds.ID(), dsFields.ID)
				return child.ds.ID(), true, nil
			}

			// If both daemon sets have the same selector, then they contend
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and { az = zone_one, az != zone_one }
			// even though they don't select anything
			if dsFields.NodeSelector.String() == child.ds.GetNodeSelector().String() {
				dsf.raiseContentionAlert(child.ds.ID(), dsFields.ID)
				return child.ds.ID(), true, nil
			}

			// Check the child's eligibleNodes, then intersect it to see if there
			// are any overlaps
			//
			// NOTE: This is naive, it does not account for new nodes, so any alerts
			// we get will be caused by adding new nodes by both human, machine error,
			// are starting up a daemon set farm where contention already exists
			scheduledNodes, err := child.ds.EligibleNodes()
			if err != nil {
				return "", false, util.Errorf("Error getting scheduled nodes: %v", err)
			}
			intersectedNodes := types.NewNodeSet(eligibleNodes...).Intersection(types.NewNodeSet(scheduledNodes...))
			if intersectedNodes.Len() > 0 {
				dsf.raiseContentionAlert(child.ds.ID(), dsFields.ID)
				return child.ds.ID(), true, nil
			}
		}
	}

	return "", false, nil
}

func (dsf *Farm) raiseContentionAlert(oldDSID ds_fields.ID, newDSID ds_fields.ID) {
	if alertErr := dsf.alerter.Alert(alerting.AlertInfo{
		Description: fmt.Sprintf("New ds '%v', contends with '%v'", newDSID, oldDSID),
		IncidentKey: "preemptive_ds_contention",
	}); alertErr != nil {
		dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
	}
}

// Creates a functioning daemon set that will watch and write to the pod tree
func (dsf *Farm) spawnDaemonSet(dsFields *ds_fields.DaemonSet) *childDS {
	dsLogger := dsf.logger.SubLogger(logrus.Fields{
		"ds":  dsFields.ID,
		"pod": dsFields.Manifest.ID(),
	})

	ds := New(
		*dsFields,
		dsf.dsStore,
		dsf.kpStore,
		dsf.applicator,
		dsLogger,
	)

	quitSpawnCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)

	desiresCh := ds.WatchDesires(quitSpawnCh, updatedCh, deletedCh)

	return &childDS{
		ds:        ds,
		quitCh:    quitSpawnCh,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
	}
}
