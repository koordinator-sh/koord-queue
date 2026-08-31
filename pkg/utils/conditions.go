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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

// Reasons reported on the QuotaReserved, Admitted, PodsReady and Finished conditions.
const (
	reasonPending                  = "Pending"
	reasonQuotaReserved            = "QuotaReserved"
	reasonWaitingForAdmissionCheck = "WaitingForAdmissionChecks"
	reasonAdmitted                 = "Admitted"
	reasonPodsRunning              = "PodsRunning"
	reasonSucceeded                = "Succeeded"
	reasonFailed                   = "Failed"
	reasonSchedulingFailed         = "SchedulingFailed"
	reasonRequeued                 = "Requeued"
)

// setCondition inserts or updates cond in conditions and reports whether anything changed.
// LastTransitionTime is only refreshed when the status flips, which mirrors the semantics of
// apimachinery's meta.SetStatusCondition (that package is not vendored here).
func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) bool {
	if cond.LastTransitionTime.IsZero() {
		cond.LastTransitionTime = metav1.Now()
	}

	for i := range *conditions {
		existing := &(*conditions)[i]
		if existing.Type != cond.Type {
			continue
		}
		if existing.Status == cond.Status && existing.Reason == cond.Reason && existing.Message == cond.Message {
			return false
		}
		if existing.Status != cond.Status {
			existing.LastTransitionTime = cond.LastTransitionTime
		}
		existing.Status = cond.Status
		existing.Reason = cond.Reason
		existing.Message = cond.Message
		return true
	}

	*conditions = append(*conditions, cond)
	return true
}

// setConditionIfPresent behaves like setCondition but never creates a missing condition. It is
// used to flip a condition back to False without adding it to queue units that never had it.
func setConditionIfPresent(conditions *[]metav1.Condition, cond metav1.Condition) bool {
	for i := range *conditions {
		if (*conditions)[i].Type == cond.Type {
			return setCondition(conditions, cond)
		}
	}
	return false
}

func trueCondition(condType, reason, message string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: metav1.ConditionTrue, Reason: reason, Message: message}
}

func falseCondition(condType, reason, message string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: metav1.ConditionFalse, Reason: reason, Message: message}
}

// SyncQueueUnitConditions mirrors the authoritative Phase into status.conditions and reports
// whether the conditions changed. Conditions are an observation window only: no scheduling
// decision reads them, so a stale or missing condition can never change queueing behaviour.
func SyncQueueUnitConditions(status *v1alpha1.QueueUnitStatus) bool {
	if !features.Enabled(features.QueueUnitConditions) {
		return false
	}

	changed := false
	switch status.Phase {
	case v1alpha1.Enqueued:
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitQuotaReserved, reasonPending, "The queue unit is waiting in its queue")) || changed
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, reasonPending, "The queue unit is not admitted yet")) || changed
		// Stop the execution clock: PodsReady doubles as the start marker for the maximum
		// execution time, so leaving it True here would count the same interval twice.
		changed = setConditionIfPresent(&status.Conditions, falseCondition(v1alpha1.QueueUnitPodsReady, reasonPending, "The pods of the job are not running")) || changed
		// Only clear a previous eviction, never introduce one on a fresh queue unit.
		changed = setConditionIfPresent(&status.Conditions, falseCondition(v1alpha1.QueueUnitEvicted, reasonRequeued, "The queue unit is queued again")) || changed
	case v1alpha1.Reserved:
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitQuotaReserved, reasonQuotaReserved, "Quota is reserved in the queue")) || changed
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, reasonWaitingForAdmissionCheck, "Waiting for all admission checks to be ready")) || changed
	case v1alpha1.Dequeued, v1alpha1.SchedReady, v1alpha1.SchedSucceed:
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitQuotaReserved, reasonQuotaReserved, "Quota is reserved in the queue")) || changed
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitAdmitted, reasonAdmitted, "The queue unit is admitted and the job is released")) || changed
	case v1alpha1.Running:
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitQuotaReserved, reasonQuotaReserved, "Quota is reserved in the queue")) || changed
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitAdmitted, reasonAdmitted, "The queue unit is admitted and the job is released")) || changed
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitPodsReady, reasonPodsRunning, "The pods of the job are running")) || changed
		// The job made it to running, so the next failure starts a fresh backoff sequence.
		changed = ClearQueueUnitRequeueState(status) || changed
	case v1alpha1.Succeed:
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitFinished, reasonSucceeded, "The job succeeded")) || changed
	case v1alpha1.Failed:
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitFinished, reasonFailed, "The job failed")) || changed
	case v1alpha1.Backoff:
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitQuotaReserved, v1alpha1.QueueUnitEvictedByBackoffTimeout, "The reservation is released while backing off")) || changed
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, v1alpha1.QueueUnitEvictedByBackoffTimeout, "The queue unit is no longer admitted")) || changed
		changed = setConditionIfPresent(&status.Conditions, falseCondition(v1alpha1.QueueUnitPodsReady, v1alpha1.QueueUnitEvictedByBackoffTimeout, "The pods of the job are not running")) || changed
		changed = setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitEvicted, v1alpha1.QueueUnitEvictedByBackoffTimeout, status.Message)) || changed
	case v1alpha1.SchedFailed:
		changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, reasonSchedulingFailed, status.Message)) || changed
	}
	return changed
}

// SetQueueUnitEvictedCondition records why the queue unit lost its admission. Callers that know
// the precise cause (deactivation, preemption, maximum execution time) use it instead of relying
// on the reason SyncQueueUnitConditions infers from the Phase.
func SetQueueUnitEvictedCondition(status *v1alpha1.QueueUnitStatus, reason, message string) bool {
	if !features.Enabled(features.QueueUnitConditions) {
		return false
	}

	changed := setCondition(&status.Conditions, trueCondition(v1alpha1.QueueUnitEvicted, reason, message))
	changed = setCondition(&status.Conditions, falseCondition(v1alpha1.QueueUnitAdmitted, reason, message)) || changed
	return changed
}

// StopQueueUnitExecutionClock marks the pods as no longer running so the maximum execution time
// stops accruing. Callers use it when they reset the accumulated execution time, to make sure the
// interval that was just discarded is not accumulated again by a later eviction.
func StopQueueUnitExecutionClock(status *v1alpha1.QueueUnitStatus) bool {
	return setConditionIfPresent(&status.Conditions,
		falseCondition(v1alpha1.QueueUnitPodsReady, reasonPending, "The pods of the job are not running"))
}

// FindQueueUnitCondition returns the condition of the given type, or nil when absent.
func FindQueueUnitCondition(status *v1alpha1.QueueUnitStatus, condType string) *metav1.Condition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == condType {
			return &status.Conditions[i]
		}
	}
	return nil
}

// IsQueueUnitConditionTrue reports whether the given condition is present and True.
func IsQueueUnitConditionTrue(status *v1alpha1.QueueUnitStatus, condType string) bool {
	cond := FindQueueUnitCondition(status, condType)
	return cond != nil && cond.Status == metav1.ConditionTrue
}
