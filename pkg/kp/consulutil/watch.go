package consulutil

import (
	"bytes"
	"time"
	"unsafe"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	"github.com/square/p2/pkg/logging"
)

// WatchPrefix watches a Consul prefix for changes to any keys that have the prefix. When
// anything changes, all Key/Value pairs having that prefix will be written to the
// provided channel.
//
// Errors will sent on the given output channel but do not otherwise affect execution. The
// given output stream will become owned by this function call, and this call will close
// it when the function ends. This function will run until explicitly canceled by closing
// the "done" channel. Data is written to the output channel synchronously, so readers
// must consume the data or this method will block.
func WatchPrefix(
	prefix string,
	clientKV ConsulLister,
	outPairs chan<- api.KVPairs,
	done <-chan struct{},
	outErrors chan<- error,
	pause time.Duration,
	metricsRegistry metrics.Registry,
	logger logging.Logger,
) {
	defer close(outPairs)
	var currentIndex uint64

	// Pause signifies the amount of time to wait after a result is
	// returned by the watch. Some use cases may want to respond quickly to
	// a change after a period of stagnation, but are able to tolerate a
	// degree of staleness in order to reduce QPS on the data store
	if pause < time.Second {
		pause = time.Second
	}
	timer := time.NewTimer(time.Duration(pause))

	if metricsRegistry == nil {
		// in-memory metrics are better than nothing. We'll log these periodically
		metricsRegistry = metrics.NewRegistry()
	}
	listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, outputPairsHistogram := watchHistograms("prefix", metricsRegistry)
	go metricsLog(metricsRegistry, 5*time.Minute, logger.WithField("WatchType", "Prefix"))

	var (
		safeListStart    time.Time
		outputPairsStart time.Time
	)
	for {
		select {
		case <-done:
			return
		default:
		}
		safeListStart = time.Now()
		pairs, queryMeta, err := SafeList(clientKV, done, prefix, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		listLatencyHistogram.Update(int64(time.Since(safeListStart) / time.Millisecond))
		if queryMeta != nil {
			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
		}
		<-timer.C
		timer.Reset(pause)
		switch err {
		case CanceledError:
			logger.WithErrorAndFields(err, logrus.Fields{
				"prefix": prefix,
			}).Errorln("SafeList was canceled")
			return
		case nil:
			currentIndex = queryMeta.LastIndex
			outputPairsStart = time.Now()
			select {
			case <-done:
			case outPairs <- pairs:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairsStart) / time.Millisecond))
			outputPairsHistogram.Update(int64(unsafe.Sizeof(pairs)))
		default:
			logger.WithErrorAndFields(err, logrus.Fields{
				"prefix": prefix,
			}).Errorln("Encountered error during SafeList")
			select {
			case <-done:
			case outErrors <- err:
			}
		}
	}
}

func watchHistograms(watchType string, metricsRegistry metrics.Registry) (listLatency metrics.Histogram, consulLatency metrics.Histogram, outputPairsWait metrics.Histogram, outputPairsBytes metrics.Histogram) {
	listLatency = metrics.GetOrRegisterHistogram("list_latency", metricsRegistry, metrics.NewExpDecaySample(1028, 0.015))
	consulLatency = metrics.GetOrRegisterHistogram("consul_latency", metricsRegistry, metrics.NewExpDecaySample(1028, 0.015))
	outputPairsWait = metrics.GetOrRegisterHistogram("output_pairs_wait", metricsRegistry, metrics.NewExpDecaySample(1028, 0.015))
	outputPairsBytes = metrics.GetOrRegisterHistogram("output_pairs_bytes", metricsRegistry, metrics.NewExpDecaySample(1028, 0.015))

	return
}

func metricsLog(r metrics.Registry, freq time.Duration, l logging.Logger) {
	logScaled(r, freq, time.Millisecond, l)
}

// Output each metric in the given registry periodically using the given
// logger. Print timings in `scale` units (eg time.Millisecond) rather than nanos.
func logScaled(r metrics.Registry, freq time.Duration, scale time.Duration, l logging.Logger) {
	du := float64(scale)
	duSuffix := scale.String()[1:]

	for _ = range time.Tick(freq) {
		r.Each(func(name string, i interface{}) {
			switch metric := i.(type) {
			case metrics.Counter:
				l.Printf("counter %s\n", name)
				l.Printf("  count:       %9d\n", metric.Count())
			case metrics.Gauge:
				l.Printf("gauge %s\n", name)
				l.Printf("  value:       %9d\n", metric.Value())
			case metrics.GaugeFloat64:
				l.Printf("gauge %s\n", name)
				l.Printf("  value:       %f\n", metric.Value())
			case metrics.Healthcheck:
				metric.Check()
				l.Printf("healthcheck %s\n", name)
				l.Printf("  error:       %v\n", metric.Error())
			case metrics.Histogram:
				h := metric.Snapshot()
				ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				l.Printf("histogram %s\n", name)
				l.Printf("  count:       %9d\n", h.Count())
				l.Printf("  min:         %9d\n", h.Min())
				l.Printf("  max:         %9d\n", h.Max())
				l.Printf("  mean:        %12.2f\n", h.Mean())
				l.Printf("  stddev:      %12.2f\n", h.StdDev())
				l.Printf("  median:      %12.2f\n", ps[0])
				l.Printf("  75%%:         %12.2f\n", ps[1])
				l.Printf("  95%%:         %12.2f\n", ps[2])
				l.Printf("  99%%:         %12.2f\n", ps[3])
				l.Printf("  99.9%%:       %12.2f\n", ps[4])
			case metrics.Meter:
				m := metric.Snapshot()
				l.Printf("meter %s\n", name)
				l.Printf("  count:       %9d\n", m.Count())
				l.Printf("  1-min rate:  %12.2f\n", m.Rate1())
				l.Printf("  5-min rate:  %12.2f\n", m.Rate5())
				l.Printf("  15-min rate: %12.2f\n", m.Rate15())
				l.Printf("  mean rate:   %12.2f\n", m.RateMean())
			case metrics.Timer:
				t := metric.Snapshot()
				ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				l.Printf("timer %s\n", name)
				l.Printf("  count:       %9d\n", t.Count())
				l.Printf("  min:         %12.2f%s\n", float64(t.Min())/du, duSuffix)
				l.Printf("  max:         %12.2f%s\n", float64(t.Max())/du, duSuffix)
				l.Printf("  mean:        %12.2f%s\n", t.Mean()/du, duSuffix)
				l.Printf("  stddev:      %12.2f%s\n", t.StdDev()/du, duSuffix)
				l.Printf("  median:      %12.2f%s\n", ps[0]/du, duSuffix)
				l.Printf("  75%%:         %12.2f%s\n", ps[1]/du, duSuffix)
				l.Printf("  95%%:         %12.2f%s\n", ps[2]/du, duSuffix)
				l.Printf("  99%%:         %12.2f%s\n", ps[3]/du, duSuffix)
				l.Printf("  99.9%%:       %12.2f%s\n", ps[4]/du, duSuffix)
				l.Printf("  1-min rate:  %12.2f\n", t.Rate1())
				l.Printf("  5-min rate:  %12.2f\n", t.Rate5())
				l.Printf("  15-min rate: %12.2f\n", t.Rate15())
				l.Printf("  mean rate:   %12.2f\n", t.RateMean())
			}
		})
	}
}

// WatchSingle has the same semantics as WatchPrefix, but for a single key in
// Consul. If the key is deleted, a nil will be sent on the output channel, but
// the watch will not be terminated. In addition, if updates happen in rapid
// succession, intervening updates may be missed. If these semantics are
// undesirable, consider WatchNewKeys instead.
func WatchSingle(
	key string,
	clientKV ConsulGetter,
	outKVP chan<- *api.KVPair,
	done <-chan struct{},
	outErrors chan<- error,
	metricsRegistry metrics.Registry,
	logger logging.Logger,
) {
	defer close(outKVP)
	var currentIndex uint64
	timer := time.NewTimer(time.Duration(0))

	if metricsRegistry == nil {
		// in-memory metrics are better than nothing. We'll log these periodically
		metricsRegistry = metrics.NewRegistry()
	}
	listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, _ := watchHistograms("single", metricsRegistry)
	go metricsLog(metricsRegistry, 5*time.Minute, logger.WithField("WatchType", "Single"))

	var (
		safeGetStart    time.Time
		outputPairStart time.Time
	)

	for {
		select {
		case <-done:
			return
		case <-timer.C:
		}
		safeGetStart = time.Now()
		timer.Reset(250 * time.Millisecond) // upper bound on request rate
		kvp, queryMeta, err := SafeGet(clientKV, done, key, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		listLatencyHistogram.Update(int64(time.Since(safeGetStart) / time.Millisecond))
		consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
		switch err {
		case CanceledError:
			return
		case nil:
			outputPairStart = time.Now()
			currentIndex = queryMeta.LastIndex
			select {
			case <-done:
			case outKVP <- kvp:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairStart) / time.Millisecond))
		default:
			select {
			case <-done:
			case outErrors <- err:
			}
			timer.Reset(2 * time.Second) // backoff
		}
	}
}

type NewKeyHandler func(key string) chan<- *api.KVPair

type keyMeta struct {
	created    uint64
	modified   uint64
	subscriber chan<- *api.KVPair
}

// WatchNewKeys watches for changes to a list of Key/Value pairs and lets each key be
// handled individually though a subscription-like interface.
//
// This function models a key's lifetime in the following way. When a key is first seen,
// the given NewKeyHandler function will be run, which may return a channel. When the
// key's value changes, new K/V updates are sent to the key's notification channel. When
// the key is deleted, `nil` is sent. After being deleted or if the watcher is asked to
// exit, a key's channel will be closed, to notify the receiver that no further updates
// are coming.
//
// WatchNewKeys doesn't watch a prefix itself--the caller should arrange a suitable input
// stream of K/V pairs, probably from WatchPrefix(). This function runs until the input
// stream closes. Closing "done" will asynchronously cancel the watch and cause it to
// eventually exit.
func WatchNewKeys(pairsChan <-chan api.KVPairs, onNewKey NewKeyHandler, done <-chan struct{}) {
	keys := make(map[string]*keyMeta)

	defer func() {
		for _, keyMeta := range keys {
			if keyMeta.subscriber != nil {
				close(keyMeta.subscriber)
			}
		}
	}()

	for {
		var pairs api.KVPairs
		var ok bool
		select {
		case <-done:
			return
		case pairs, ok = <-pairsChan:
			if !ok {
				return
			}
		}

		visited := make(map[string]bool)

		// Scan for new and changed keys
		for _, pair := range pairs {
			visited[pair.Key] = true
			if keyMeta, ok := keys[pair.Key]; ok {
				if keyMeta.created == pair.CreateIndex {
					// Existing key that was seen before
					if keyMeta.subscriber != nil && keyMeta.modified != pair.ModifyIndex {
						// It's changed!
						keyMeta.modified = pair.ModifyIndex
						select {
						case <-done:
							return
						case keyMeta.subscriber <- pair:
						}
					}
					continue
				} else {
					// This key was deleted and recreated between queries
					if keyMeta.subscriber != nil {
						select {
						case <-done:
							return
						case keyMeta.subscriber <- nil:
						}
						close(keyMeta.subscriber)
					}
					// Fall through to re-create this key
				}
			}
			// Found a new key
			keyMeta := &keyMeta{
				created:    pair.CreateIndex,
				modified:   pair.ModifyIndex,
				subscriber: onNewKey(pair.Key),
			}
			keys[pair.Key] = keyMeta
			if keyMeta.subscriber != nil {
				select {
				case <-done:
					return
				case keyMeta.subscriber <- pair:
				}
			}
		}

		// Scan for deleted keys
		for key, keyMeta := range keys {
			if !visited[key] {
				if keyMeta.subscriber != nil {
					select {
					case <-done:
						return
					case keyMeta.subscriber <- nil:
					}
					close(keyMeta.subscriber)
				}
				delete(keys, key)
			}
		}
	}
}

type WatchedChanges struct {
	Created api.KVPairs
	Updated api.KVPairs
	Deleted api.KVPairs
}

// WatchDiff watches a Consul prefix for changes and categorizes them
// into create, update, and delete, please note that if a kvPair was
// create and modified before this starts watching, this watch will
// treat it as a create
func WatchDiff(
	prefix string,
	clientKV ConsulLister,
	quitCh <-chan struct{},
	outErrors chan<- error,
	metricsRegistry metrics.Registry,
	logger logging.Logger,
) <-chan *WatchedChanges {
	outCh := make(chan *WatchedChanges)

	go func() {
		defer close(outCh)

		// Keep track of what we have seen so that we know when something was changed
		keys := make(map[string]*api.KVPair)

		var currentIndex uint64
		timer := time.NewTimer(time.Duration(0))

		if metricsRegistry == nil {
			// in-memory metrics are better than nothing. We'll log these periodically
			metricsRegistry = metrics.NewRegistry()
		}
		listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, outputPairsHistogram := watchHistograms("diff", metricsRegistry)
		go metricsLog(metricsRegistry, 5*time.Minute, logger.WithField("WatchType", "Diff"))

		var (
			safeListStart    time.Time
			outputPairsStart time.Time
		)
		for {
			select {
			case <-quitCh:
				return
			case <-timer.C:
			}
			timer.Reset(250 * time.Millisecond) // upper bound on request rate

			safeListStart = time.Now()
			pairs, queryMeta, err := SafeList(clientKV, quitCh, prefix, &api.QueryOptions{
				WaitIndex: currentIndex,
			})
			listLatencyHistogram.Update(int64(time.Since(safeListStart) / time.Millisecond))
			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))

			if err == CanceledError {
				select {
				case <-quitCh:
				case outErrors <- err:
				}
				return
			} else if err != nil {
				select {
				case <-quitCh:
				case outErrors <- err:
				}
				timer.Reset(2 * time.Second) // backoff
				continue
			}

			currentIndex = queryMeta.LastIndex
			// A copy used to keep track of what was deleted
			mapCopy := make(map[string]*api.KVPair)
			for key, val := range keys {
				mapCopy[key] = val
			}

			outgoingChanges := &WatchedChanges{}
			for _, val := range pairs {
				if _, ok := keys[val.Key]; !ok {
					// If it is not in the map, then it was a create
					outgoingChanges.Created = append(outgoingChanges.Created, val)
					keys[val.Key] = val

				} else if !bytes.Equal(keys[val.Key].Value, val.Value) {
					// If is in the map and the values are the not same, then it was an update
					// TODO: Should use something else other than comparing values
					outgoingChanges.Updated = append(outgoingChanges.Updated, val)
					if _, ok := mapCopy[val.Key]; ok {
						delete(mapCopy, val.Key)
					}
					keys[val.Key] = val

				} else {
					// Otherwise it is in the map and the values are equal, so it was not an update
					if _, ok := mapCopy[val.Key]; ok {
						delete(mapCopy, val.Key)
					}
				}
			}
			// If it was not observed, then it was a delete
			for key, val := range mapCopy {
				outgoingChanges.Deleted = append(outgoingChanges.Deleted, val)
				if _, ok := keys[key]; ok {
					delete(keys, key)
				}
			}

			outputPairsStart = time.Now()
			select {
			case <-quitCh:
			case outCh <- outgoingChanges:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairsStart) / time.Millisecond))
			outputPairsHistogram.Update(int64(unsafe.Sizeof(pairs)))
		}
	}()

	return outCh
}
