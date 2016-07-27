package replication

import "time"

type Throttle struct {
	minHealth int
	podHealth podHealth
	throttle  chan struct{}
	quit      chan struct{}
}

func NewThrottle(minHealth int, podHealth podHealth) *Throttle {
	return &Throttle{
		minHealth: minHealth,
		podHealth: podHealth,
		throttle:  make(chan struct{}),
		quit:      make(chan struct{}),
	}
}

// runs forever, call in a goroutine
func (t *Throttle) Engage() {
	ticker := time.NewTicker(1 * time.Second)
	select {
	case <-t.quit:
		ticker.Stop()
		return
	case <-ticker.C:
		t.throttle <- struct{}{}
	}
}
