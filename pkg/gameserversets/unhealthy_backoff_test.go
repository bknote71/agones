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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"agones.dev/agones/pkg/apis"
	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
	"agones.dev/agones/pkg/gameservers"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var testGameServerCount atomic.Int64

func uniqueGameServerName() string {
	return fmt.Sprintf("gs-%d", testGameServerCount.Add(1))
}

func unhealthyBeforeReadyGameServer(createdAt, failedAt time.Time) *agonesv1.GameServer {
	return &agonesv1.GameServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              uniqueGameServerName(),
			CreationTimestamp: metav1.NewTime(createdAt),
			Annotations: map[string]string{
				agonesv1.GameServerUnhealthyBeforeReadyAtAnnotation: failedAt.UTC().Format(time.RFC3339),
			},
		},
		Status: agonesv1.GameServerStatus{State: agonesv1.GameServerStateUnhealthy},
	}
}

func gameServerCreatedAt(state agonesv1.GameServerState, createdAt time.Time) *agonesv1.GameServer {
	return &agonesv1.GameServer{
		ObjectMeta: metav1.ObjectMeta{Name: uniqueGameServerName(), CreationTimestamp: metav1.NewTime(createdAt)},
		Status:     agonesv1.GameServerStatus{State: state},
	}
}

func TestUnhealthyBackoffObserve(t *testing.T) {
	t.Parallel()

	gsSet := &agonesv1.GameServerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gsSet"}}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }

	t.Run("no failures, no delay", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)
		list := []*agonesv1.GameServer{
			gameServerCreatedAt(agonesv1.GameServerStateReady, at(0)),
			gameServerCreatedAt(agonesv1.GameServerStateUnhealthy, at(0)),
		}
		assert.Equal(t, time.Duration(0), b.observe(gsSet, list, at(5)))
		assert.Empty(t, b.entries)
	})

	t.Run("delay doubles on consecutive failures up to the max", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(0), at(5))}
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(5)))
		// the same failure observed again does not increase the delay
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(14)))

		list = []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(15), at(20))}
		assert.Equal(t, 20*time.Second, b.observe(gsSet, list, at(20)))

		list = []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(40), at(45))}
		assert.Equal(t, 40*time.Second, b.observe(gsSet, list, at(45)))

		list = []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(85), at(90))}
		assert.Equal(t, 40*time.Second, b.observe(gsSet, list, at(90)))
	})

	t.Run("failures within the same delay period count once", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(0), at(5))}
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(5)))

		list = append(list, unhealthyBeforeReadyGameServer(at(0), at(7)))
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(7)))

		list = append(list, unhealthyBeforeReadyGameServer(at(0), at(9)))
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(9)))
	})

	t.Run("a GameServer created with the failed one clears the delay when it becomes usable", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		failed := unhealthyBeforeReadyGameServer(at(0), at(5))
		sibling := gameServerCreatedAt(agonesv1.GameServerStateScheduled, at(0))
		assert.Equal(t, 10*time.Second, b.observe(gsSet, []*agonesv1.GameServer{failed, sibling}, at(5)))

		sibling.Status.State = agonesv1.GameServerStateReady
		assert.Equal(t, time.Duration(0), b.observe(gsSet, []*agonesv1.GameServer{failed, sibling}, at(12)))
		assert.Empty(t, b.entries)
	})

	t.Run("becoming usable and a new failure seen together clears the delay", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(0), at(5))}
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(5)))

		list = []*agonesv1.GameServer{
			unhealthyBeforeReadyGameServer(at(15), at(20)),
			gameServerCreatedAt(agonesv1.GameServerStateReady, at(15)),
		}
		assert.Equal(t, time.Duration(0), b.observe(gsSet, list, at(20)))
		assert.Empty(t, b.entries)
	})

	t.Run("a GameServer that becomes usable after the last failure clears the delay", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		failed := unhealthyBeforeReadyGameServer(at(0), at(5))
		// was already Ready before the failure, so says nothing about whether the failure is over
		older := gameServerCreatedAt(agonesv1.GameServerStateReady, at(0))
		assert.Equal(t, 10*time.Second, b.observe(gsSet, []*agonesv1.GameServer{failed, older}, at(5)))

		starting := gameServerCreatedAt(agonesv1.GameServerStateScheduled, at(15))
		assert.Equal(t, 10*time.Second, b.observe(gsSet, []*agonesv1.GameServer{failed, older, starting}, at(16)))

		starting.Status.State = agonesv1.GameServerStateReady
		assert.Equal(t, time.Duration(0), b.observe(gsSet, []*agonesv1.GameServer{failed, older, starting}, at(18)))
		assert.Empty(t, b.entries)
	})

	t.Run("GameServers being deleted are ignored", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		failed := unhealthyBeforeReadyGameServer(at(0), at(5))
		deletedAt := metav1.NewTime(at(6))
		failed.ObjectMeta.DeletionTimestamp = &deletedAt
		assert.Equal(t, time.Duration(0), b.observe(gsSet, []*agonesv1.GameServer{failed}, at(6)))
	})

	t.Run("state expires when there have been no failures for a long time", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(0), at(5))}
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(5)))

		assert.Equal(t, 10*time.Second, b.observe(gsSet, nil, at(60)))
		assert.Equal(t, time.Duration(0), b.observe(gsSet, nil, at(86)))
		assert.Empty(t, b.entries)
	})

	t.Run("deleting the GameServerSet clears its state", func(t *testing.T) {
		b := newUnhealthyBackoff(40 * time.Second)

		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(at(0), at(5))}
		assert.Equal(t, 10*time.Second, b.observe(gsSet, list, at(5)))
		b.deleteGameServerSet(gsSet)
		assert.Empty(t, b.entries)
	})
}

func TestNextUnhealthyDelayEnd(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }

	list := []*agonesv1.GameServer{
		gameServerCreatedAt(agonesv1.GameServerStateReady, at(0)),
		gameServerCreatedAt(agonesv1.GameServerStateUnhealthy, at(0)),
		unhealthyBeforeReadyGameServer(at(0), at(8)),
		unhealthyBeforeReadyGameServer(at(0), at(5)),
		unhealthyBeforeReadyGameServer(at(0), at(1)),
	}

	next, ok := nextUnhealthyDelayEnd(list, 10*time.Second, at(12))
	assert.True(t, ok)
	assert.Equal(t, 3*time.Second, next)

	_, ok = nextUnhealthyDelayEnd(list, 10*time.Second, at(18))
	assert.False(t, ok)

	_, ok = nextUnhealthyDelayEnd(list, 0, at(12))
	assert.False(t, ok)
}

func TestComputeReconciliationActionUnhealthyBeforeReady(t *testing.T) {
	t.Parallel()

	now := time.Now()
	compute := func(list []*agonesv1.GameServer, replicas int, delay time.Duration) (int, []*agonesv1.GameServer) {
		toAdd, toDelete, _ := computeReconciliationAction(apis.Distributed, list, map[string]gameservers.NodeCount{},
			replicas, 1000, 1000, 1000, nil, delay)
		return toAdd, toDelete
	}

	t.Run("kept while within the delay", func(t *testing.T) {
		list := []*agonesv1.GameServer{
			unhealthyBeforeReadyGameServer(now, now.Add(-5*time.Second)),
			gameServerCreatedAt(agonesv1.GameServerStateReady, now),
		}
		toAdd, toDelete := compute(list, 2, 30*time.Second)
		assert.Equal(t, 0, toAdd)
		assert.Empty(t, toDelete)
	})

	t.Run("replaced once the delay has passed", func(t *testing.T) {
		list := []*agonesv1.GameServer{
			unhealthyBeforeReadyGameServer(now, now.Add(-45*time.Second)),
			gameServerCreatedAt(agonesv1.GameServerStateReady, now),
		}
		toAdd, toDelete := compute(list, 2, 30*time.Second)
		assert.Equal(t, 1, toAdd)
		assert.Equal(t, []*agonesv1.GameServer{list[0]}, toDelete)
	})

	t.Run("replaced straight away when there is no delay", func(t *testing.T) {
		list := []*agonesv1.GameServer{unhealthyBeforeReadyGameServer(now, now)}
		toAdd, toDelete := compute(list, 1, 0)
		assert.Equal(t, 1, toAdd)
		assert.Len(t, toDelete, 1)
	})

	t.Run("Unhealthy after Ready is replaced straight away", func(t *testing.T) {
		list := []*agonesv1.GameServer{gameServerCreatedAt(agonesv1.GameServerStateUnhealthy, now)}
		toAdd, toDelete := compute(list, 1, 30*time.Second)
		assert.Equal(t, 1, toAdd)
		assert.Len(t, toDelete, 1)
	})

	t.Run("kept GameServers are removed on scale down", func(t *testing.T) {
		list := []*agonesv1.GameServer{
			unhealthyBeforeReadyGameServer(now, now),
			unhealthyBeforeReadyGameServer(now, now),
		}
		toAdd, toDelete := compute(list, 0, 30*time.Second)
		assert.Equal(t, 0, toAdd)
		assert.Len(t, toDelete, 2)
	})
}
