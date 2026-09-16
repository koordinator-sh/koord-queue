/*
 Copyright 2021 The Koord-Queue Authors.

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

package utils

import (
	"math/rand"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

const (
	// DefaultBackoffBase is the first backoff delay, used when the caller has no backoff time
	// of its own to start from.
	DefaultBackoffBase = 10 * time.Second
	// MaxBackoffDuration caps the exponential growth so a queue unit is always retried
	// eventually instead of drifting into an effectively infinite backoff.
	MaxBackoffDuration = 15 * time.Minute
	// backoffJitterFraction spreads the retries of queue units that back off at the same
	// moment, which is the common case when a whole queue is starved.
	backoffJitterFraction = 0.1
)

// backoffDelay returns the delay for the given attempt using capped exponential growth with
// jitter. Attempt numbering starts at 1, the value recorded for the first backoff.
func backoffDelay(base time.Duration, attempt int32) time.Duration {
	if base <= 0 {
		base = DefaultBackoffBase
	}
	delay := base
	for i := int32(1); i < attempt; i++ {
		delay *= 2
		if delay >= MaxBackoffDuration {
			break
		}
	}
	if delay > MaxBackoffDuration {
		delay = MaxBackoffDuration
	}
	// Symmetric jitter around the computed delay.
	jitter := float64(delay) * backoffJitterFraction
	delay += time.Duration((rand.Float64()*2 - 1) * jitter)
	if delay <= 0 {
		delay = base
	}
	return delay
}

// RecordQueueUnitRequeueState advances the backoff bookkeeping as a queue unit enters Backoff
// and returns the delay after which it becomes eligible for scheduling again. A zero base makes
// it fall back to DefaultBackoffBase. It returns 0 when the feature is disabled, which tells the
// caller to keep using its own flat backoff time.
func RecordQueueUnitRequeueState(status *v1alpha1.QueueUnitStatus, base time.Duration, now time.Time) time.Duration {
	if !features.Enabled(features.QueueUnitRequeueState) {
		return 0
	}

	attempt := int32(1)
	if status.RequeueState != nil {
		attempt = status.RequeueState.Count + 1
	}
	delay := backoffDelay(base, attempt)
	status.RequeueState = &v1alpha1.RequeueState{
		Count:     attempt,
		RequeueAt: &metav1.Time{Time: now.Add(delay)},
	}
	return delay
}

// QueueUnitRequeueBackoffElapsed reports whether the recorded backoff has expired, along with
// the remaining wait when it has not. Queue units without recorded state fall back to the flat
// backoff time, so units that backed off before the feature was enabled still recover.
func QueueUnitRequeueBackoffElapsed(status *v1alpha1.QueueUnitStatus, flatBackoff time.Duration, now time.Time) (elapsed bool, remaining time.Duration) {
	if !features.Enabled(features.QueueUnitRequeueState) || status.RequeueState == nil || status.RequeueState.RequeueAt == nil {
		var last time.Time
		if status.LastUpdateTime != nil {
			last = status.LastUpdateTime.Time
		}
		waited := now.Sub(last)
		if waited >= flatBackoff {
			return true, 0
		}
		return false, flatBackoff - waited
	}

	requeueAt := status.RequeueState.RequeueAt.Time
	if now.Before(requeueAt) {
		return false, requeueAt.Sub(now)
	}
	return true, 0
}

// ClearQueueUnitRequeueState drops the backoff bookkeeping once the queue unit runs
// successfully, so a later failure starts counting from the first attempt again.
func ClearQueueUnitRequeueState(status *v1alpha1.QueueUnitStatus) bool {
	if status.RequeueState == nil {
		return false
	}
	status.RequeueState = nil
	return true
}
