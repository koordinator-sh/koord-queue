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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

func TestBackoffDelayGrowsAndIsCapped(t *testing.T) {
	const base = 10 * time.Second

	tests := []struct {
		name    string
		attempt int32
		wantMin time.Duration
		wantMax time.Duration
	}{
		// Bounds allow for the +-10% jitter around the nominal delay.
		{name: "first attempt uses the base", attempt: 1, wantMin: 9 * time.Second, wantMax: 11 * time.Second},
		{name: "second attempt doubles", attempt: 2, wantMin: 18 * time.Second, wantMax: 22 * time.Second},
		{name: "third attempt doubles again", attempt: 3, wantMin: 36 * time.Second, wantMax: 44 * time.Second},
		{name: "large attempt is capped", attempt: 30, wantMin: MaxBackoffDuration - MaxBackoffDuration/10, wantMax: MaxBackoffDuration + MaxBackoffDuration/10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Repeat so the jitter cannot make a flaky pass.
			for i := 0; i < 50; i++ {
				got := backoffDelay(base, tt.attempt)
				if got < tt.wantMin || got > tt.wantMax {
					t.Fatalf("backoffDelay(%v, %d) = %v, want within [%v, %v]", base, tt.attempt, got, tt.wantMin, tt.wantMax)
				}
			}
		})
	}
}

func TestBackoffDelayZeroBaseFallsBackToDefault(t *testing.T) {
	got := backoffDelay(0, 1)
	if got < DefaultBackoffBase-DefaultBackoffBase/10 || got > DefaultBackoffBase+DefaultBackoffBase/10 {
		t.Errorf("backoffDelay(0, 1) = %v, want roughly %v", got, DefaultBackoffBase)
	}
}

func TestRecordQueueUnitRequeueState(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitRequeueState, true)

	now := time.Now()

	tests := []struct {
		name      string
		initial   *v1alpha1.RequeueState
		wantCount int32
	}{
		{name: "first backoff starts at one", initial: nil, wantCount: 1},
		{name: "second backoff increments", initial: &v1alpha1.RequeueState{Count: 1}, wantCount: 2},
		{name: "counting continues from the recorded attempt", initial: &v1alpha1.RequeueState{Count: 7}, wantCount: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &v1alpha1.QueueUnitStatus{Phase: v1alpha1.Backoff, RequeueState: tt.initial}
			delay := RecordQueueUnitRequeueState(status, 10*time.Second, now)

			if delay <= 0 {
				t.Fatalf("RecordQueueUnitRequeueState() delay = %v, want a positive delay", delay)
			}
			if status.RequeueState == nil {
				t.Fatalf("requeueState was not recorded")
			}
			if status.RequeueState.Count != tt.wantCount {
				t.Errorf("count = %d, want %d", status.RequeueState.Count, tt.wantCount)
			}
			if status.RequeueState.RequeueAt == nil {
				t.Fatalf("requeueAt was not recorded")
			}
			if got := status.RequeueState.RequeueAt.Time; !got.Equal(now.Add(delay)) {
				t.Errorf("requeueAt = %v, want %v", got, now.Add(delay))
			}
		})
	}
}

func TestRecordQueueUnitRequeueStateFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitRequeueState, false)

	status := &v1alpha1.QueueUnitStatus{Phase: v1alpha1.Backoff}
	if delay := RecordQueueUnitRequeueState(status, 10*time.Second, time.Now()); delay != 0 {
		t.Errorf("delay = %v, want 0 so the caller keeps its flat backoff", delay)
	}
	if status.RequeueState != nil {
		t.Errorf("requeueState = %+v, want nil while the gate is disabled", status.RequeueState)
	}
}

func TestQueueUnitRequeueBackoffElapsed(t *testing.T) {
	now := time.Now()
	const flat = 30 * time.Second

	tests := []struct {
		name         string
		gateEnabled  bool
		status       v1alpha1.QueueUnitStatus
		wantElapsed  bool
		wantRemAtMin time.Duration
	}{
		{
			name:        "structured state not yet due",
			gateEnabled: true,
			status: v1alpha1.QueueUnitStatus{RequeueState: &v1alpha1.RequeueState{
				Count: 1, RequeueAt: &metav1.Time{Time: now.Add(20 * time.Second)},
			}},
			wantElapsed:  false,
			wantRemAtMin: 19 * time.Second,
		},
		{
			name:        "structured state already due",
			gateEnabled: true,
			status: v1alpha1.QueueUnitStatus{RequeueState: &v1alpha1.RequeueState{
				Count: 3, RequeueAt: &metav1.Time{Time: now.Add(-time.Second)},
			}},
			wantElapsed: true,
		},
		{
			// Units that backed off before the feature was enabled must still recover.
			name:        "no structured state falls back to the flat backoff, still waiting",
			gateEnabled: true,
			status:      v1alpha1.QueueUnitStatus{LastUpdateTime: &metav1.Time{Time: now.Add(-5 * time.Second)}},
			wantElapsed: false,
		},
		{
			name:        "no structured state falls back to the flat backoff, elapsed",
			gateEnabled: true,
			status:      v1alpha1.QueueUnitStatus{LastUpdateTime: &metav1.Time{Time: now.Add(-time.Minute)}},
			wantElapsed: true,
		},
		{
			// With the gate off the recorded state must be ignored entirely.
			name:        "gate disabled ignores the structured state",
			gateEnabled: false,
			status: v1alpha1.QueueUnitStatus{
				LastUpdateTime: &metav1.Time{Time: now.Add(-time.Minute)},
				RequeueState: &v1alpha1.RequeueState{
					Count: 1, RequeueAt: &metav1.Time{Time: now.Add(time.Hour)},
				},
			},
			wantElapsed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitRequeueState, tt.gateEnabled)

			elapsed, remaining := QueueUnitRequeueBackoffElapsed(&tt.status, flat, now)
			if elapsed != tt.wantElapsed {
				t.Fatalf("elapsed = %v, want %v (remaining %v)", elapsed, tt.wantElapsed, remaining)
			}
			if !elapsed && remaining <= 0 {
				t.Errorf("remaining = %v, want a positive wait while backing off", remaining)
			}
			if tt.wantRemAtMin > 0 && remaining < tt.wantRemAtMin {
				t.Errorf("remaining = %v, want at least %v", remaining, tt.wantRemAtMin)
			}
		})
	}
}

func TestClearQueueUnitRequeueStateOnRunning(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitRequeueState, true)

	status := &v1alpha1.QueueUnitStatus{
		Phase: v1alpha1.Running,
		RequeueState: &v1alpha1.RequeueState{
			Count: 4, RequeueAt: &metav1.Time{Time: time.Now()},
		},
	}
	// Reaching Running is what resets the sequence, and it happens through the conditions sync.
	if !SyncQueueUnitConditions(status) {
		t.Fatalf("SyncQueueUnitConditions() reported no change")
	}
	if status.RequeueState != nil {
		t.Errorf("requeueState = %+v, want nil once the job runs", status.RequeueState)
	}
}
