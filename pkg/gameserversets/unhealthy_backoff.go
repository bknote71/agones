// Copyright Contributors to Agones a Series of LF Projects, LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gameserversets

import (
	"sync"
	"time"

	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
)

const (
	// unhealthyBackoffBase is the first delay before replacing a GameServer that became Unhealthy before Ready.
	unhealthyBackoffBase = 10 * time.Second
	// unhealthyBackoffMax is the maximum delay.
	unhealthyBackoffMax = 5 * time.Minute
)

// unhealthyBackoff tracks, per GameServerSet, how long to keep a GameServer that became Unhealthy before Ready.
// It is not persisted: a controller restart or leader change starts the delay again from the base.
type unhealthyBackoff struct {
	entries map[string]*unhealthyBackoffEntry
	mu      sync.Mutex
	max     time.Duration
}

type unhealthyBackoffEntry struct {
	// usableAtLastFailure is the names of the GameServers that were already usable at lastFailure.
	usableAtLastFailure map[string]struct{}
	// lastStep is when delay was last increased.
	lastStep time.Time
	// lastFailure is the latest before-Ready failure observed.
	lastFailure time.Time
	delay       time.Duration
}

func newUnhealthyBackoff(maxDelay time.Duration) *unhealthyBackoff {
	return &unhealthyBackoff{max: maxDelay, entries: map[string]*unhealthyBackoffEntry{}}
}

// observe updates the backoff state of the GameServerSet and returns the current delay.
// The delay doubles at most once per delay period, and is cleared when a GameServer becomes usable
// after the last failure or no failure has been seen for twice the maximum delay.
func (b *unhealthyBackoff) observe(gsSet *agonesv1.GameServerSet, list []*agonesv1.GameServer, now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := gsSet.ObjectMeta.Namespace + "/" + gsSet.ObjectMeta.Name
	entry := b.entries[key]

	usable := map[string]struct{}{}
	var latestFailure time.Time
	for _, gs := range list {
		if gs.IsBeingDeleted() {
			continue
		}
		if gs.IsActive() {
			usable[gs.ObjectMeta.Name] = struct{}{}
		}
		if failedAt, ok := unhealthyBeforeReadyAt(gs); ok && failedAt.After(latestFailure) {
			latestFailure = failedAt
		}
	}

	// Checked before recording a new failure, so the result does not depend on which is observed first.
	if entry != nil {
		for name := range usable {
			if _, ok := entry.usableAtLastFailure[name]; !ok {
				delete(b.entries, key)
				return 0
			}
		}
	}

	if latestFailure.IsZero() {
		if entry != nil && now.Sub(entry.lastFailure) > 2*b.max {
			delete(b.entries, key)
			entry = nil
		}
	} else {
		if entry == nil {
			entry = &unhealthyBackoffEntry{}
			b.entries[key] = entry
		}
		if latestFailure.After(entry.lastFailure) {
			switch {
			case entry.delay == 0:
				entry.delay = unhealthyBackoffBase
				entry.lastStep = now
			case now.Sub(entry.lastStep) >= entry.delay:
				entry.delay = min(entry.delay*2, b.max)
				entry.lastStep = now
			}
			entry.lastFailure = latestFailure
			entry.usableAtLastFailure = usable
		}
	}

	if entry == nil {
		return 0
	}
	return entry.delay
}

func (b *unhealthyBackoff) deleteGameServerSet(gsSet *agonesv1.GameServerSet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, gsSet.ObjectMeta.Namespace+"/"+gsSet.ObjectMeta.Name)
}

// unhealthyBeforeReadyAt returns when the GameServer became Unhealthy before Ready, if it did.
func unhealthyBeforeReadyAt(gs *agonesv1.GameServer) (time.Time, bool) {
	if gs.Status.State != agonesv1.GameServerStateUnhealthy {
		return time.Time{}, false
	}
	failedAtStr := gs.ObjectMeta.Annotations[agonesv1.GameServerUnhealthyBeforeReadyAtAnnotation]
	if failedAtStr == "" {
		return time.Time{}, false
	}
	failedAt, err := time.Parse(time.RFC3339, failedAtStr)
	if err != nil {
		return time.Time{}, false
	}
	return failedAt, true
}

// unhealthyDelayRemaining returns how much longer to keep the GameServer. Zero or less means replace it now.
func unhealthyDelayRemaining(gs *agonesv1.GameServer, delay time.Duration, now time.Time) time.Duration {
	if delay <= 0 {
		return 0
	}
	failedAt, ok := unhealthyBeforeReadyAt(gs)
	if !ok {
		return 0
	}
	return failedAt.Add(delay).Sub(now)
}

// nextUnhealthyDelayEnd returns the time until the first kept GameServer is due to be replaced.
func nextUnhealthyDelayEnd(list []*agonesv1.GameServer, delay time.Duration, now time.Time) (time.Duration, bool) {
	var next time.Duration
	found := false
	for _, gs := range list {
		if gs.IsBeingDeleted() {
			continue
		}
		if remaining := unhealthyDelayRemaining(gs, delay, now); remaining > 0 && (!found || remaining < next) {
			next = remaining
			found = true
		}
	}
	return next, found
}
