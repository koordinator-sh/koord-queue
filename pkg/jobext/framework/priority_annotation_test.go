package framework

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPriorityFromAnnotation(t *testing.T) {
	cases := []struct {
		name string
		ann  map[string]string
		want *int32
	}{
		{"no annotations", nil, nil},
		{"absent key", map[string]string{"other": "1"}, nil},
		{"empty value", map[string]string{PriorityAnnotationKey: ""}, nil},
		{"invalid value", map[string]string{PriorityAnnotationKey: "abc"}, nil},
		{"valid value", map[string]string{PriorityAnnotationKey: "42"}, int32Ptr(42)},
		{"negative value", map[string]string{PriorityAnnotationKey: "-5"}, int32Ptr(-5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := priorityFromAnnotation(jobWithAnnotations(tc.ann))
			if !priorityEqual(got, tc.want) {
				t.Fatalf("priorityFromAnnotation() = %v, want %v", ptrVal(got), ptrVal(tc.want))
			}
		})
	}
}

// TestCreateQueueUnitPriority covers the priority a freshly created QueueUnit gets: the value
// reported by the job extension, unless the job carries the scheduling.x-k8s.io/priority
// annotation, which wins so that create and update paths agree.
func TestCreateQueueUnitPriority(t *testing.T) {
	tests := []struct {
		name           string
		annotations    map[string]string
		jobPriority    *int32
		expectPriority *int32
	}{
		{
			name:           "no annotation uses the job extension priority",
			jobPriority:    int32Ptr(20),
			expectPriority: int32Ptr(20),
		},
		{
			name:           "annotation overrides the job extension priority",
			annotations:    map[string]string{PriorityAnnotationKey: "99"},
			jobPriority:    int32Ptr(20),
			expectPriority: int32Ptr(99),
		},
		{
			name:           "unparseable annotation falls back to the job extension priority",
			annotations:    map[string]string{PriorityAnnotationKey: "not-a-number"},
			jobPriority:    int32Ptr(20),
			expectPriority: int32Ptr(20),
		},
		{
			name:           "annotation is the only priority source",
			annotations:    map[string]string{PriorityAnnotationKey: "7"},
			jobPriority:    nil,
			expectPriority: int32Ptr(7),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(s); err != nil {
				t.Fatalf("failed to add QueueUnit to scheme: %v", err)
			}
			if err := v1.AddToScheme(s); err != nil {
				t.Fatalf("failed to add core/v1 to scheme: %v", err)
			}

			ext := &configurableJobExtension{priorityClassName: "high", priority: tc.jobPriority}
			handle := NewJobHandle(time.Minute, time.Minute, 0, ext, false)
			cl := fake.NewClientBuilder().WithScheme(s).Build()
			reconciler := NewJobReconcilerWithJobExtension(cl, s, handle)

			job := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:        "test-job",
				Namespace:   "default",
				Annotations: tc.annotations,
			}}

			if err := reconciler.createQueueUnit(context.Background(), handle, job, v1alpha1.Enqueued, ""); err != nil {
				t.Fatalf("createQueueUnit() returned error: %v", err)
			}

			created := &v1alpha1.QueueUnit{}
			if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "test-job"}, created); err != nil {
				t.Fatalf("failed to get the created QueueUnit: %v", err)
			}
			if !priorityEqual(created.Spec.Priority, tc.expectPriority) {
				t.Errorf("Spec.Priority = %v, want %v", ptrVal(created.Spec.Priority), ptrVal(tc.expectPriority))
			}
			if created.Spec.PriorityClassName != "high" {
				t.Errorf("Spec.PriorityClassName = %q, want %q", created.Spec.PriorityClassName, "high")
			}
		})
	}
}
