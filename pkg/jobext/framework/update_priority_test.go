package framework

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// configurableJobExtension is a mock GenericJobExtension whose Priority return value
// can be configured per-test to simulate a job spec change.
type configurableJobExtension struct {
	mockGenericJobExtension
	priorityClassName string
	priority          *int32
}

func (m *configurableJobExtension) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	return m.priorityClassName, m.priority
}

func (m *configurableJobExtension) Object() client.Object {
	return &v1.Pod{}
}

func (m *configurableJobExtension) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "test", Version: "v1", Kind: "Pod"}
}

func int32Ptr(v int32) *int32 { return &v }

func TestUpdateQueueUnitPriority(t *testing.T) {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = v1.AddToScheme(s)

	tests := []struct {
		name             string
		quPriority       *int32
		quPriorityClass  string
		jobPriority      *int32
		jobPriorityClass string
		annotations      map[string]string
		expectUpdated    bool
		expectPriority   *int32
		expectPC         string
	}{
		{
			name:             "priority unchanged - no update",
			quPriority:       int32Ptr(10),
			quPriorityClass:  "high",
			jobPriority:      int32Ptr(10),
			jobPriorityClass: "high",
			expectUpdated:    false,
			expectPriority:   int32Ptr(10),
			expectPC:         "high",
		},
		{
			name:             "priority changed from job extension",
			quPriority:       int32Ptr(10),
			quPriorityClass:  "low",
			jobPriority:      int32Ptr(20),
			jobPriorityClass: "high",
			expectUpdated:    true,
			expectPriority:   int32Ptr(20),
			expectPC:         "high",
		},
		{
			name:             "priority overridden by annotation",
			quPriority:       int32Ptr(10),
			quPriorityClass:  "low",
			jobPriority:      int32Ptr(20),
			jobPriorityClass: "high",
			annotations:      map[string]string{PriorityAnnotationKey: "99"},
			expectUpdated:    true,
			expectPriority:   int32Ptr(99),
			expectPC:         "high",
		},
		{
			name:             "both nil priority - no update",
			quPriority:       nil,
			quPriorityClass:  "",
			jobPriority:      nil,
			jobPriorityClass: "",
			expectUpdated:    false,
			expectPriority:   nil,
			expectPC:         "",
		},
		{
			name:             "priority class changed only",
			quPriority:       int32Ptr(10),
			quPriorityClass:  "low",
			jobPriority:      int32Ptr(10),
			jobPriorityClass: "high",
			expectUpdated:    true,
			expectPriority:   int32Ptr(10),
			expectPC:         "high",
		},
		{
			name:             "priority changed from nil to value",
			quPriority:       nil,
			quPriorityClass:  "",
			jobPriority:      int32Ptr(5),
			jobPriorityClass: "test",
			expectUpdated:    true,
			expectPriority:   int32Ptr(5),
			expectPC:         "test",
		},
		{
			name:             "priority changed from value to nil",
			quPriority:       int32Ptr(5),
			quPriorityClass:  "test",
			jobPriority:      nil,
			jobPriorityClass: "",
			expectUpdated:    true,
			expectPriority:   nil,
			expectPC:         "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qu := &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-qu",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{
					Priority:          tc.quPriority,
					PriorityClassName: tc.quPriorityClass,
				},
			}

			obj := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: tc.annotations,
				},
			}

			cl := fake.NewClientBuilder().WithScheme(s).WithObjects(qu).Build()

			ext := &configurableJobExtension{
				priorityClassName: tc.jobPriorityClass,
				priority:          tc.jobPriority,
			}

			handle := NewJobHandle(time.Minute, time.Minute, 0, ext, false)
			reconciler := NewJobReconcilerWithJobExtension(cl, s, handle)

			updated, err := reconciler.updateQueueUnitPriority(context.Background(), handle, obj, qu)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if updated != tc.expectUpdated {
				t.Fatalf("expected updated=%v, got %v", tc.expectUpdated, updated)
			}

			// If updated, re-read the queueunit from the fake client to verify persistence
			updatedQU := &v1alpha1.QueueUnit{}
			if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "test-qu"}, updatedQU); err != nil {
				t.Fatalf("failed to get queueunit: %v", err)
			}

			if !priorityEqual(updatedQU.Spec.Priority, tc.expectPriority) {
				t.Errorf("expected priority %v, got %v", ptrVal(tc.expectPriority), ptrVal(updatedQU.Spec.Priority))
			}
			if updatedQU.Spec.PriorityClassName != tc.expectPC {
				t.Errorf("expected priorityClassName %q, got %q", tc.expectPC, updatedQU.Spec.PriorityClassName)
			}
		})
	}
}

func TestPriorityEqual(t *testing.T) {
	tests := []struct {
		name string
		a    *int32
		b    *int32
		want bool
	}{
		{"both nil", nil, nil, true},
		{"a nil b set", nil, int32Ptr(1), false},
		{"a set b nil", int32Ptr(1), nil, false},
		{"same value", int32Ptr(5), int32Ptr(5), true},
		{"different value", int32Ptr(5), int32Ptr(10), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := priorityEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("priorityEqual() = %v, want %v", got, tc.want)
			}
		})
	}
}

func ptrVal(p *int32) interface{} {
	if p == nil {
		return nil
	}
	return *p
}
