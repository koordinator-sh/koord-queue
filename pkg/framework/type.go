/*
 Copyright 2021 The Kube-Queue Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package framework

import (
	"sync"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// QueueUnitInfo is a Queue wrapper with additional information related to
// the QueueUnit
type QueueUnitInfo struct {
	// Name is namespace + "/" + name
	Name string
	Unit *v1alpha1.QueueUnit
	// The time QueueUnit added to the scheduling queue.
	Timestamp time.Time
	// Number of schedule attempts before successfully scheduled.
	Attempts int
	// The time when the QueueUnit is added to the queue for the first time.
	InitialAttemptTimestamp time.Time
	//
	Queue string
}

var lock sync.RWMutex
var queueUnitInfoCache = map[types.UID]*QueueUnitInfo{}

func ResetQueueUnitCache() {
	lock.Lock()
	defer lock.Unlock()
	queueUnitInfoCache = map[types.UID]*QueueUnitInfo{}
}

// NewQueueUnitInfo constructs QueueUnitInfo
func NewQueueUnitInfo(unit *v1alpha1.QueueUnit) *QueueUnitInfo {
	lock.RLock()
	if existing, ok := queueUnitInfoCache[unit.UID]; ok {
		lock.RUnlock()
		return &QueueUnitInfo{
			Name:                    unit.Namespace + "/" + unit.Name,
			Unit:                    unit.DeepCopy(),
			Timestamp:               existing.Timestamp,
			Attempts:                0,
			InitialAttemptTimestamp: existing.InitialAttemptTimestamp,
		}
	}
	lock.RUnlock()
	info := &QueueUnitInfo{
		Name:                    unit.Namespace + "/" + unit.Name,
		Unit:                    unit,
		Timestamp:               time.Now(),
		Attempts:                0,
		InitialAttemptTimestamp: unit.CreationTimestamp.Time,
	}
	lock.Lock()
	queueUnitInfoCache[unit.UID] = info
	lock.Unlock()
	return info
}

func ForgetQueueUnitInfo(c cache.Indexer) {
	lock.Lock()
	for uid := range queueUnitInfoCache {
		if _, e, _ := c.GetByKey(string(uid)); !e {
			delete(queueUnitInfoCache, uid)
		}
	}
	lock.Unlock()
}

func (qu *QueueUnitInfo) Copy() *QueueUnitInfo {
	return &QueueUnitInfo{
		Name:                    qu.Name,
		Timestamp:               qu.Timestamp,
		Attempts:                qu.Attempts,
		InitialAttemptTimestamp: qu.InitialAttemptTimestamp,
		Unit:                    qu.Unit.DeepCopy(),
	}
}
