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

// conditionSnapshot is the (type, status, reason) triple asserted by the tests, keeping the
// expectations readable without pinning timestamps.
type conditionSnapshot struct {
	condType string
	status   metav1.ConditionStatus
	reason   string
}

func snapshot(conditions []metav1.Condition) []conditionSnapshot {
	out := make([]conditionSnapshot, 0, len(conditions))
	for _, c := range conditions {
		out = append(out, conditionSnapshot{condType: c.Type, status: c.Status, reason: c.Reason})
	}
	return out
}

func containsCondition(conditions []metav1.Condition, want conditionSnapshot) bool {
	for _, got := range snapshot(conditions) {
		if got == want {
			return true
		}
	}
	return false
}

func TestSyncQueueUnitConditions(t *testing.T) {
	tests := []struct {
		name           string
		status         v1alpha1.QueueUnitStatus
		wantChanged    bool
		wantConditions []conditionSnapshot
		wantAbsent     []string
	}{
		{
			name:        "enqueued reports pending quota and admission",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Enqueued},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitQuotaReserved, metav1.ConditionFalse, reasonPending},
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionFalse, reasonPending},
			},
			// A queue unit that was never evicted must not gain an Evicted condition.
			wantAbsent: []string{v1alpha1.QueueUnitEvicted},
		},
		{
			name:        "reserved reports quota reserved but not admitted",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Reserved},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitQuotaReserved, metav1.ConditionTrue, reasonQuotaReserved},
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionFalse, reasonWaitingForAdmissionCheck},
			},
		},
		{
			name:        "dequeued reports admitted",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Dequeued},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitQuotaReserved, metav1.ConditionTrue, reasonQuotaReserved},
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionTrue, reasonAdmitted},
			},
		},
		{
			name:        "schedReady reports admitted in strict dequeue mode",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.SchedReady},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionTrue, reasonAdmitted},
			},
		},
		{
			name:        "running reports pods ready",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Running},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionTrue, reasonAdmitted},
				{v1alpha1.QueueUnitPodsReady, metav1.ConditionTrue, reasonPodsRunning},
			},
		},
		{
			name:        "succeed reports finished",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Succeed},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitFinished, metav1.ConditionTrue, reasonSucceeded},
			},
		},
		{
			name:        "failed reports finished with failure reason",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Failed},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitFinished, metav1.ConditionTrue, reasonFailed},
			},
		},
		{
			name:        "backoff reports eviction and drops quota reservation",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.Backoff, Message: "running timeout"},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitQuotaReserved, metav1.ConditionFalse, v1alpha1.QueueUnitEvictedByBackoffTimeout},
				{v1alpha1.QueueUnitEvicted, metav1.ConditionTrue, v1alpha1.QueueUnitEvictedByBackoffTimeout},
			},
		},
		{
			name:        "schedFailed reports admission failure",
			status:      v1alpha1.QueueUnitStatus{Phase: v1alpha1.SchedFailed, Message: "no node"},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitAdmitted, metav1.ConditionFalse, reasonSchedulingFailed},
			},
		},
		{
			name: "requeue after eviction clears the evicted condition",
			status: v1alpha1.QueueUnitStatus{
				Phase: v1alpha1.Enqueued,
				Conditions: []metav1.Condition{{
					Type:               v1alpha1.QueueUnitEvicted,
					Status:             metav1.ConditionTrue,
					Reason:             v1alpha1.QueueUnitEvictedByPreemption,
					Message:            "preempted",
					LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute)),
				}},
			},
			wantChanged: true,
			wantConditions: []conditionSnapshot{
				{v1alpha1.QueueUnitEvicted, metav1.ConditionFalse, reasonRequeued},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := *tt.status.DeepCopy()
			if got := SyncQueueUnitConditions(&status); got != tt.wantChanged {
				t.Errorf("SyncQueueUnitConditions() changed = %v, want %v", got, tt.wantChanged)
			}
			for _, want := range tt.wantConditions {
				if !containsCondition(status.Conditions, want) {
					t.Errorf("missing condition %+v, got %+v", want, snapshot(status.Conditions))
				}
			}
			for _, absent := range tt.wantAbsent {
				if FindQueueUnitCondition(&status, absent) != nil {
					t.Errorf("condition %q should be absent, got %+v", absent, snapshot(status.Conditions))
				}
			}

			// A second sync of an unchanged phase must be a no-op so status updates are not
			// rewritten on every reconcile.
			if SyncQueueUnitConditions(&status) {
				t.Errorf("SyncQueueUnitConditions() reported a change on a repeated call")
			}
		})
	}
}

func TestSyncQueueUnitConditionsFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitConditions, false)

	status := v1alpha1.QueueUnitStatus{Phase: v1alpha1.Dequeued}
	if SyncQueueUnitConditions(&status) {
		t.Errorf("SyncQueueUnitConditions() must not report changes while the gate is disabled")
	}
	if len(status.Conditions) != 0 {
		t.Errorf("conditions must stay empty while the gate is disabled, got %+v", status.Conditions)
	}
}

func TestSetQueueUnitEvictedCondition(t *testing.T) {
	tests := []struct {
		name    string
		initial []metav1.Condition
		reason  string
		message string
	}{
		{
			name:    "deactivation",
			reason:  v1alpha1.QueueUnitEvictedByDeactivation,
			message: "the queue unit is deactivated",
		},
		{
			name:    "maximum execution time exceeded",
			reason:  v1alpha1.QueueUnitEvictedByMaximumExecutionTimeExceeded,
			message: "exceeded the maximum execution time",
		},
		{
			name: "overwrites an earlier eviction reason",
			initial: []metav1.Condition{{
				Type:               v1alpha1.QueueUnitEvicted,
				Status:             metav1.ConditionTrue,
				Reason:             v1alpha1.QueueUnitEvictedByBackoffTimeout,
				Message:            "backoff",
				LastTransitionTime: metav1.Now(),
			}},
			reason:  v1alpha1.QueueUnitEvictedByPreemption,
			message: "preempted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := v1alpha1.QueueUnitStatus{Phase: v1alpha1.Dequeued, Conditions: tt.initial}
			if !SetQueueUnitEvictedCondition(&status, tt.reason, tt.message) {
				t.Fatalf("SetQueueUnitEvictedCondition() reported no change")
			}

			evicted := FindQueueUnitCondition(&status, v1alpha1.QueueUnitEvicted)
			if evicted == nil {
				t.Fatalf("Evicted condition was not set")
			}
			if evicted.Status != metav1.ConditionTrue || evicted.Reason != tt.reason || evicted.Message != tt.message {
				t.Errorf("Evicted condition = %+v, want status True reason %q message %q", evicted, tt.reason, tt.message)
			}
			if !IsQueueUnitConditionTrue(&status, v1alpha1.QueueUnitEvicted) {
				t.Errorf("IsQueueUnitConditionTrue(Evicted) = false, want true")
			}
			// Losing the admission must be visible on the Admitted condition as well.
			admitted := FindQueueUnitCondition(&status, v1alpha1.QueueUnitAdmitted)
			if admitted == nil || admitted.Status != metav1.ConditionFalse {
				t.Errorf("Admitted condition = %+v, want status False", admitted)
			}
		})
	}
}

func TestSetConditionKeepsLastTransitionTimeStable(t *testing.T) {
	status := v1alpha1.QueueUnitStatus{Phase: v1alpha1.Dequeued}
	SyncQueueUnitConditions(&status)

	admitted := FindQueueUnitCondition(&status, v1alpha1.QueueUnitAdmitted)
	if admitted == nil {
		t.Fatalf("Admitted condition was not set")
	}
	firstTransition := admitted.LastTransitionTime

	// Only the message changes: the transition time must be preserved.
	setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitAdmitted, reasonAdmitted, "another message"))
	admitted = FindQueueUnitCondition(&status, v1alpha1.QueueUnitAdmitted)
	if !admitted.LastTransitionTime.Equal(&firstTransition) {
		t.Errorf("LastTransitionTime changed on a message-only update: %v -> %v", firstTransition, admitted.LastTransitionTime)
	}

	// Flipping the status must refresh the transition time.
	setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, reasonPending, "evicted"))
	admitted = FindQueueUnitCondition(&status, v1alpha1.QueueUnitAdmitted)
	if admitted.LastTransitionTime.Before(&firstTransition) {
		t.Errorf("LastTransitionTime went backwards on a status flip: %v -> %v", firstTransition, admitted.LastTransitionTime)
	}
}
