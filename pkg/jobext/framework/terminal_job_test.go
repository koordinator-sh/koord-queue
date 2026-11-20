package framework

import (
	"context"
	"testing"
	"time"

	queuev1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type terminalJobAdapter struct {
	mockGenericJobExtension
	status JobStatus
}

func (a *terminalJobAdapter) Object() client.Object { return &batchv1.Job{} }
func (a *terminalJobAdapter) GVK() schema.GroupVersionKind {
	return batchv1.SchemeGroupVersion.WithKind("Job")
}
func (a *terminalJobAdapter) QueueUnitSuffix() string { return "qu" }
func (a *terminalJobAdapter) GetJobStatus(context.Context, client.Object, client.Client) (JobStatus, time.Time) {
	return a.status, time.Time{}
}

func TestTerminalJobWithRelatedQueueUnitRemainsManaged(t *testing.T) {
	tests := []struct {
		name       string
		status     JobStatus
		jobUID     types.UID
		queueUnit  *queuev1.QueueUnit
		wantManage bool
	}{
		{name: "failed with related unit", status: Failed, queueUnit: terminalTestQueueUnit(), wantManage: true},
		{name: "succeeded with related unit", status: Succeeded, queueUnit: terminalTestQueueUnit(), wantManage: true},
		{name: "running without marker", status: Running, queueUnit: terminalTestQueueUnit()},
		{name: "failed without unit", status: Failed},
		{name: "failed with unrelated unit", status: Failed, queueUnit: func() *queuev1.QueueUnit {
			qu := terminalTestQueueUnit()
			qu.Spec.ConsumerRef.Name = "another-job"
			return qu
		}()},
		{name: "failed with unit from previous job UID", status: Failed, jobUID: "new-job", queueUnit: func() *queuev1.QueueUnit {
			qu := terminalTestQueueUnit()
			qu.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "test-job", UID: "old-job"}}
			return qu
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := queuev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := batchv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.queueUnit != nil {
				builder = builder.WithObjects(tt.queueUnit)
			}
			adapter := &terminalJobAdapter{status: tt.status}
			ext := NewGenericJobExtensionWithJob(adapter, func(context.Context, client.Object) bool { return false })
			handle := NewJobHandle(0, 0, 0, ext, false)
			reconciler := &GenericJobReconciler{client: builder.Build()}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default", UID: tt.jobUID}}
			got, err := reconciler.shouldReconcileJob(context.Background(), handle, job)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.wantManage {
				t.Fatalf("shouldReconcileJob() = %v, want %v", got, tt.wantManage)
			}
		})
	}
}

func terminalTestQueueUnit() *queuev1.QueueUnit {
	return &queuev1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job-qu", Namespace: "default"},
		Spec: queuev1.QueueUnitSpec{ConsumerRef: &corev1.ObjectReference{
			APIVersion: "batch/v1", Kind: "Job", Name: "test-job", Namespace: "default",
		}},
	}
}

func TestTerminalJobReconcileClearsStaleAdmissionsWithoutQueuingMarker(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := queuev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"}}
	qu := terminalTestQueueUnit()
	qu.Status.Phase = queuev1.Failed
	qu.Status.Admissions = []queuev1.Admission{{Name: "master", Replicas: 1}}
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&queuev1.QueueUnit{}).
		WithObjects(job, qu).Build()
	adapter := &terminalJobAdapter{status: Failed}
	ext := NewGenericJobExtensionWithJob(adapter, func(context.Context, client.Object) bool { return false })
	handle := NewJobHandle(0, 0, 0, ext, false)
	reconciler := &GenericJobReconciler{
		client:     cli,
		jobHandles: map[string]JobHandle{handle.typeName: handle},
	}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: job.Name, Namespace: handle.typeName + "|default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	updated := &queuev1.QueueUnit{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: qu.Name, Namespace: qu.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != queuev1.Failed || len(updated.Status.Admissions) != 0 {
		t.Fatalf("terminal QueueUnit still holds admissions: phase=%s admissions=%v", updated.Status.Phase, updated.Status.Admissions)
	}
}
