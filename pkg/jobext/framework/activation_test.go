package framework

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	corev1 "k8s.io/api/core/v1"
)

func jobWithAnnotations(ann map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default", Annotations: ann}}
}

func TestActiveFromAnnotation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *bool
	}{
		{name: "no annotations at all", annotations: nil, want: nil},
		{name: "annotation absent", annotations: map[string]string{"other": "x"}, want: nil},
		{name: "empty value is ignored", annotations: map[string]string{ActiveAnnotationKey: ""}, want: nil},
		{name: "false deactivates", annotations: map[string]string{ActiveAnnotationKey: "false"}, want: ptr.To(false)},
		{name: "true activates", annotations: map[string]string{ActiveAnnotationKey: "true"}, want: ptr.To(true)},
		// An unparseable value must not silently deactivate a job.
		{name: "garbage value is ignored", annotations: map[string]string{ActiveAnnotationKey: "nope"}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := activeFromAnnotation(jobWithAnnotations(tt.annotations))
			switch {
			case tt.want == nil && got != nil:
				t.Errorf("activeFromAnnotation() = %v, want nil", *got)
			case tt.want != nil && got == nil:
				t.Errorf("activeFromAnnotation() = nil, want %v", *tt.want)
			case tt.want != nil && *got != *tt.want:
				t.Errorf("activeFromAnnotation() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestMaxExecutionTimeFromAnnotation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *int32
	}{
		{name: "annotation absent", annotations: nil, want: nil},
		{name: "positive value", annotations: map[string]string{MaxExecTimeSecondsAnnotationKey: "3600"}, want: ptr.To(int32(3600))},
		{name: "zero is rejected", annotations: map[string]string{MaxExecTimeSecondsAnnotationKey: "0"}, want: nil},
		{name: "negative is rejected", annotations: map[string]string{MaxExecTimeSecondsAnnotationKey: "-5"}, want: nil},
		{name: "non numeric is rejected", annotations: map[string]string{MaxExecTimeSecondsAnnotationKey: "1h"}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maxExecutionTimeFromAnnotation(jobWithAnnotations(tt.annotations))
			switch {
			case tt.want == nil && got != nil:
				t.Errorf("maxExecutionTimeFromAnnotation() = %v, want nil", *got)
			case tt.want != nil && got == nil:
				t.Errorf("maxExecutionTimeFromAnnotation() = nil, want %v", *tt.want)
			case tt.want != nil && *got != *tt.want:
				t.Errorf("maxExecutionTimeFromAnnotation() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestIsBeingReclaimed(t *testing.T) {
	tests := []struct {
		name       string
		admissions []v1alpha1.Admission
		want       bool
	}{
		{name: "no admissions", want: false},
		{name: "no reclaim state", admissions: []v1alpha1.Admission{{Name: "a", Replicas: 2}}, want: false},
		{
			name:       "reclaim in progress",
			admissions: []v1alpha1.Admission{{Name: "a", Replicas: 2, ReclaimState: &v1alpha1.ReclaimState{Replicas: 1}}},
			want:       true,
		},
		{
			name:       "reclaim state with zero replicas is not in progress",
			admissions: []v1alpha1.Admission{{Name: "a", Replicas: 2, ReclaimState: &v1alpha1.ReclaimState{}}},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qu := &v1alpha1.QueueUnit{Status: v1alpha1.QueueUnitStatus{Admissions: tt.admissions}}
			if got := isBeingReclaimed(qu); got != tt.want {
				t.Errorf("isBeingReclaimed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileDeactivationSkipsJobsWithoutRequeueSupport(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueUnitActive, true)

	queueUnit := &v1alpha1.QueueUnit{
		Spec: v1alpha1.QueueUnitSpec{Active: ptr.To(false)},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Running,
		},
	}

	handled, err := (&GenericJobReconciler{}).reconcileDeactivation(
		context.Background(), logr.Discard(), JobHandle{}, nil, queueUnit,
	)
	if err != nil {
		t.Fatalf("reconcileDeactivation() error = %v", err)
	}
	if handled {
		t.Fatal("reconcileDeactivation() handled = true, want false for a job without RequeueJobExtension")
	}
	if queueUnit.Status.Phase != v1alpha1.Running {
		t.Fatalf("phase = %s, want Running", queueUnit.Status.Phase)
	}
}

func TestAccumulateExecutionTime(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaximumExecutionTime, true)

	podsReadySince := func(d time.Duration, status metav1.ConditionStatus) []metav1.Condition {
		return []metav1.Condition{{
			Type:               v1alpha1.QueueUnitPodsReady,
			Status:             status,
			Reason:             "PodsRunning",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-d)),
		}}
	}

	tests := []struct {
		name        string
		conditions  []metav1.Condition
		accumulated *int32
		wantAtLeast *int32
	}{
		{
			name:        "no PodsReady condition means the clock never started",
			conditions:  nil,
			wantAtLeast: nil,
		},
		{
			name:        "PodsReady false means the clock is stopped",
			conditions:  podsReadySince(time.Minute, metav1.ConditionFalse),
			wantAtLeast: nil,
		},
		{
			name:        "running for a while accumulates from zero",
			conditions:  podsReadySince(10*time.Second, metav1.ConditionTrue),
			wantAtLeast: ptr.To(int32(9)),
		},
		{
			name:        "accumulation adds to the previous total",
			conditions:  podsReadySince(10*time.Second, metav1.ConditionTrue),
			accumulated: ptr.To(int32(100)),
			wantAtLeast: ptr.To(int32(109)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &v1alpha1.QueueUnitStatus{
				Conditions:                          tt.conditions,
				AccumulatedPastExecutionTimeSeconds: tt.accumulated,
			}
			accumulateExecutionTime(status)

			got := status.AccumulatedPastExecutionTimeSeconds
			if tt.wantAtLeast == nil {
				if !int32PtrEqual(got, tt.accumulated) {
					t.Errorf("accumulated = %v, want it left untouched (%v)", got, tt.accumulated)
				}
				return
			}
			if got == nil {
				t.Fatalf("accumulated = nil, want at least %v", *tt.wantAtLeast)
			}
			if *got < *tt.wantAtLeast {
				t.Errorf("accumulated = %v, want at least %v", *got, *tt.wantAtLeast)
			}
		})
	}
}

func TestAccumulateExecutionTimeFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaximumExecutionTime, false)

	status := &v1alpha1.QueueUnitStatus{Conditions: []metav1.Condition{{
		Type:               v1alpha1.QueueUnitPodsReady,
		Status:             metav1.ConditionTrue,
		Reason:             "PodsRunning",
		LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute)),
	}}}
	accumulateExecutionTime(status)
	if status.AccumulatedPastExecutionTimeSeconds != nil {
		t.Errorf("accumulated = %v, want nil while the gate is disabled", *status.AccumulatedPastExecutionTimeSeconds)
	}
}

func TestTightenRequeue(t *testing.T) {
	tests := []struct {
		name string
		res  ctrl.Result
		want time.Duration
	}{
		{name: "empty result takes the deadline", res: ctrl.Result{}, want: 5 * time.Second},
		{name: "longer delay is shortened", res: ctrl.Result{RequeueAfter: time.Minute}, want: 5 * time.Second},
		{name: "shorter delay is kept", res: ctrl.Result{RequeueAfter: time.Second}, want: time.Second},
		// An immediate requeue already wakes up sooner than any deadline.
		{name: "immediate requeue is left alone", res: ctrl.Result{Requeue: true}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tightenRequeue(tt.res, 5*time.Second); got.RequeueAfter != tt.want {
				t.Errorf("tightenRequeue().RequeueAfter = %v, want %v", got.RequeueAfter, tt.want)
			}
		})
	}
}
