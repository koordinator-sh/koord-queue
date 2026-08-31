package framework

import (
	"context"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	queueutils "github.com/koordinator-sh/koord-queue/pkg/utils"
)

// ActiveAnnotationKey lets a job stop or resume queueing without deleting anything. Setting it
// to "false" is the user-facing entry point for spec.active: it is parsed when the QueueUnit is
// created, so a job can be born inactive without ever being admitted, and it is kept in sync
// afterwards. Any other value, or the absence of the annotation, leaves spec.active untouched so
// an automatic deactivation (for instance on exceeding the maximum execution time) is not
// flipped back to active by the sync.
const ActiveAnnotationKey = "scheduling.x-k8s.io/active"

// MaxExecTimeSecondsAnnotationKey caps how long the job may execute, counted from the moment its
// pods start running. It mirrors the upstream kueue.x-k8s.io/max-exec-time-seconds label.
const MaxExecTimeSecondsAnnotationKey = "scheduling.x-k8s.io/max-exec-time-seconds"

// activeFromAnnotation returns the value of the active annotation, or nil when it is absent or
// not a valid boolean.
func activeFromAnnotation(object client.Object) *bool {
	ann := object.GetAnnotations()
	if ann == nil {
		return nil
	}
	raw, ok := ann[ActiveAnnotationKey]
	if !ok || raw == "" {
		return nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		klog.Warningf("ignoring invalid %s annotation %q on %s/%s: %v",
			ActiveAnnotationKey, raw, object.GetNamespace(), object.GetName(), err)
		return nil
	}
	return ptr.To(v)
}

// maxExecutionTimeFromAnnotation returns the maximum execution time declared on the job, or nil
// when the annotation is absent or is not a positive int32.
func maxExecutionTimeFromAnnotation(object client.Object) *int32 {
	ann := object.GetAnnotations()
	if ann == nil {
		return nil
	}
	raw, ok := ann[MaxExecTimeSecondsAnnotationKey]
	if !ok || raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v <= 0 {
		klog.Warningf("ignoring invalid %s annotation %q on %s/%s",
			MaxExecTimeSecondsAnnotationKey, raw, object.GetNamespace(), object.GetName())
		return nil
	}
	return ptr.To(int32(v))
}

// syncQueueUnitActivation propagates the active and maximum-execution-time annotations from the
// job onto the queue unit spec. This keeps the job -> QueueUnit direction of the data flow: the
// job is the user-facing object, the queue unit is what the scheduler reads.
func (d *GenericJobReconciler) syncQueueUnitActivation(ctx context.Context, object client.Object, qu *v1alpha1.QueueUnit) (updated bool, err error) {
	needUpdate := false
	newQu := qu

	if features.Enabled(features.QueueUnitActive) {
		// Only an explicit annotation drives the field: when it is missing we must not reset
		// spec.active, otherwise a controller-driven deactivation would be undone here.
		if want := activeFromAnnotation(object); want != nil {
			if qu.Spec.Active == nil || *qu.Spec.Active != *want {
				newQu.Spec.Active = want
				needUpdate = true
			}
		}
	}

	if features.Enabled(features.MaximumExecutionTime) {
		want := maxExecutionTimeFromAnnotation(object)
		if !int32PtrEqual(qu.Spec.MaximumExecutionTimeSeconds, want) {
			newQu.Spec.MaximumExecutionTimeSeconds = want
			needUpdate = true
		}
	}

	if !needUpdate {
		return false, nil
	}
	return true, d.client.Update(ctx, newQu)
}

func int32PtrEqual(a, b *int32) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// isBeingReclaimed reports whether a preemption is still reclaiming resources from this queue
// unit. Deactivation waits for that to finish so the two eviction paths never race.
func isBeingReclaimed(qu *v1alpha1.QueueUnit) bool {
	for _, ad := range qu.Status.Admissions {
		if ad.ReclaimState != nil && ad.ReclaimState.Replicas > 0 {
			return true
		}
	}
	return false
}

// reconcileDeactivation evicts a queue unit whose spec.active was set to false. It reuses the
// preemption eviction path: the job extension suspends the job and deletes its resources, and
// the quota is only returned once the pods are actually gone and the resource reporter shrinks
// the admitted replicas. Nothing here releases quota directly.
//
// It reports handled=true when the caller should stop reconciling this round.
func (d *GenericJobReconciler) reconcileDeactivation(ctx context.Context, log logr.Logger, handle JobHandle,
	object client.Object, queueUnit *v1alpha1.QueueUnit) (handled bool, err error) {
	if !features.Enabled(features.QueueUnitActive) {
		return false, nil
	}
	if v1alpha1.IsQueueUnitActive(queueUnit) {
		return false, nil
	}
	// A job type without RequeueJobExtension cannot complete the suspend/re-admit lifecycle,
	// so spec.active does not apply to it.
	if handle.requeueJobExtension == nil {
		return false, nil
	}
	// A finished job has nothing left to reclaim.
	if queueUnit.Status.Phase == v1alpha1.Succeed || queueUnit.Status.Phase == v1alpha1.Failed {
		return false, nil
	}
	// Let an in-flight preemption finish its own reclaim first; the next reconcile will pick
	// the deactivation up again.
	if isBeingReclaimed(queueUnit) {
		log.V(2).Info("queue unit is inactive but still being reclaimed by preemption, waiting")
		return true, nil
	}

	// Already evicted and parked: stay put until the queue unit is activated again.
	if !queueutils.IsQueueUnitReservedAnyResource(queueUnit) && queueUnit.Status.Phase == v1alpha1.Enqueued {
		log.V(5).Info("queue unit is inactive and holds no resources, waiting for activation")
		return true, nil
	}

	log.V(0).Info("queue unit is deactivated, suspending the job and reclaiming its resources")
	if err := handle.genericJobExtension.Suspend(ctx, object, d.client); err != nil {
		return true, err
	}

	newQu := queueUnit.DeepCopy()
	newQu.Status.Phase = v1alpha1.Enqueued
	newQu.Status.Message = "Evicted because the queue unit is deactivated"
	newQu.Status.LastUpdateTime = &v1.Time{Time: time.Now()}
	// Bank the execution time spent so far before the clock is stopped, so a stop/resume cycle
	// cannot be used to dodge the execution budget.
	accumulateExecutionTime(&newQu.Status)
	// Sync first so the phase mapping runs, then stamp the precise eviction reason on top.
	queueutils.SyncQueueUnitConditions(&newQu.Status)
	queueutils.SetQueueUnitEvictedCondition(&newQu.Status, v1alpha1.QueueUnitEvictedByDeactivation, newQu.Status.Message)
	if err := d.client.Status().Update(ctx, newQu); err != nil {
		return true, err
	}
	d.eventRecorder.Event(object, corev1.EventTypeNormal, "Deactivated",
		"Job is suspended and its resources are reclaimed because the queue unit is deactivated")
	return true, nil
}

// reconcileMaxExecutionTime deactivates a queue unit that ran longer than
// spec.maximumExecutionTimeSeconds. The budget is consumed only while the job is actually
// executing, so time spent waiting for admission or for pods to start is free; jobs that are
// admitted but never start running are the job of the running timeout instead.
//
// It reports the delay after which the remaining budget expires, so the caller can requeue.
func (d *GenericJobReconciler) reconcileMaxExecutionTime(ctx context.Context, log logr.Logger,
	object client.Object, queueUnit *v1alpha1.QueueUnit) (requeueAfter time.Duration, err error) {
	if !features.Enabled(features.MaximumExecutionTime) {
		return 0, nil
	}
	if queueUnit.Spec.MaximumExecutionTimeSeconds == nil || !v1alpha1.IsQueueUnitActive(queueUnit) {
		return 0, nil
	}
	// The clock starts when the pods report running, which is what PodsReady records.
	podsReady := queueutils.FindQueueUnitCondition(&queueUnit.Status, v1alpha1.QueueUnitPodsReady)
	if podsReady == nil || podsReady.Status != v1.ConditionTrue {
		return 0, nil
	}

	budget := time.Duration(*queueUnit.Spec.MaximumExecutionTimeSeconds) * time.Second
	spent := time.Duration(ptr.Deref(queueUnit.Status.AccumulatedPastExecutionTimeSeconds, 0)) * time.Second
	remaining := budget - spent - time.Since(podsReady.LastTransitionTime.Time)
	if remaining > 0 {
		return remaining, nil
	}

	log.V(0).Info("queue unit exceeded its maximum execution time, deactivating it",
		"maximumExecutionTimeSeconds", *queueUnit.Spec.MaximumExecutionTimeSeconds)

	newQu := queueUnit.DeepCopy()
	newQu.Spec.Active = ptr.To(false)
	if err := d.client.Update(ctx, newQu); err != nil {
		return 0, err
	}

	// Deactivation resets the accumulated time: reactivating the queue unit is a deliberate
	// decision that grants a fresh budget, and it must not be defeated by a stale total.
	newQu.Status.AccumulatedPastExecutionTimeSeconds = nil
	newQu.Status.Message = "Evicted because the maximum execution time was exceeded"
	newQu.Status.LastUpdateTime = &v1.Time{Time: time.Now()}
	// Stop the clock together with the reset, otherwise the eviction that follows would
	// accumulate the very interval that was just discarded.
	queueutils.StopQueueUnitExecutionClock(&newQu.Status)
	queueutils.SetQueueUnitEvictedCondition(&newQu.Status,
		v1alpha1.QueueUnitEvictedByMaximumExecutionTimeExceeded, newQu.Status.Message)
	if err := d.client.Status().Update(ctx, newQu); err != nil {
		return 0, err
	}
	d.eventRecorder.Eventf(object, corev1.EventTypeWarning, v1alpha1.QueueUnitEvictedByMaximumExecutionTimeExceeded,
		"The maximum execution time (%ds) was exceeded", *queueUnit.Spec.MaximumExecutionTimeSeconds)
	return 0, nil
}

// tightenRequeue shortens a reconcile result's requeue delay so a pending deadline is not
// missed by the surrounding status handling, which requeues on much coarser periods.
func tightenRequeue(res ctrl.Result, after time.Duration) ctrl.Result {
	if res.Requeue {
		return res
	}
	if res.RequeueAfter == 0 || res.RequeueAfter > after {
		res.RequeueAfter = after
	}
	return res
}

// accumulateExecutionTime adds the time the job just spent executing to the accumulated total,
// so a job cannot dodge its execution budget by cycling through evict and re-admit. It is called
// on the eviction paths that keep the queue unit active (preemption, running timeout).
func accumulateExecutionTime(status *v1alpha1.QueueUnitStatus) {
	if !features.Enabled(features.MaximumExecutionTime) {
		return
	}
	podsReady := queueutils.FindQueueUnitCondition(status, v1alpha1.QueueUnitPodsReady)
	if podsReady == nil || podsReady.Status != v1.ConditionTrue {
		return
	}
	elapsed := int32(time.Since(podsReady.LastTransitionTime.Time).Seconds())
	if elapsed <= 0 {
		return
	}
	status.AccumulatedPastExecutionTimeSeconds = ptr.To(ptr.Deref(status.AccumulatedPastExecutionTimeSeconds, 0) + elapsed)
}
