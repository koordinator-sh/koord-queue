package schedulingqueuev2

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	queueapi "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	queueframework "github.com/koordinator-sh/koord-queue/pkg/framework"
	jobframework "github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	jobutil "github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	testfake "github.com/koordinator-sh/koord-queue/pkg/test/fake"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestIndexedJobFailureDoesNotBlockQueue reproduces the queue stall after an
// admitted Indexed Job loses a Pod permanently. There are no Pending Pods and
// no replacement will be created, so the next job should be allowed to proceed.
// Only the API/cache and Pod state changes are simulated; the Job adapter,
// resource reporter and queue admission logic are the production implementations.
func TestIndexedJobFailureDoesNotBlockQueue(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failIndex bool
	}{
		{name: "all_pods_running"},
		{name: "one_index_permanently_failed", failIndex: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			scheme := runtime.NewScheme()
			must(corev1.AddToScheme(scheme))
			must(batchv1.AddToScheme(scheme))
			must(queueapi.AddToScheme(scheme))

			// 1. Both replicas have already been admitted and are running.
			start := metav1.NewTime(time.Unix(1, 0))
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default", UID: "job-uid"},
				Spec: batchv1.JobSpec{
					Parallelism: ptr.To(int32(2)), Completions: ptr.To(int32(2)), Suspend: ptr.To(false),
					CompletionMode: ptr.To(batchv1.IndexedCompletion), BackoffLimitPerIndex: ptr.To(int32(0)),
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{Name: "worker", Image: "test-image", Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
						}}},
					}},
				},
				Status: batchv1.JobStatus{Active: 2, StartTime: &start},
			}
			pods := make([]*corev1.Pod, 2)
			objects := []client.Object{job}
			for i := range pods {
				pods[i] = &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("pod-%d", i), Namespace: job.Namespace,
						Labels: map[string]string{
							"batch.kubernetes.io/controller-uid":       string(job.UID),
							"batch.kubernetes.io/job-completion-index": fmt.Sprint(i),
						},
						OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}},
					},
					Spec:   corev1.PodSpec{NodeName: "node", Containers: job.Spec.Template.Spec.Containers, RestartPolicy: corev1.RestartPolicyNever},
					Status: corev1.PodStatus{Phase: corev1.PodRunning, StartTime: &start},
				}
				objects = append(objects, pods[i])
			}
			api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
				WithStatusSubresource(&queueapi.QueueUnit{}, &batchv1.Job{}, &corev1.Pod{}).
				WithIndex(&corev1.Pod{}, jobutil.PodsByOwnersCacheFields, func(obj client.Object) []string {
					owner := metav1.GetControllerOf(obj)
					if owner == nil {
						return nil
					}
					return []string{owner.Kind + "/" + owner.Name}
				}).Build()
			cached := nonTerminalPodClient{api}
			handle := handles.NewJobReconciler(cached, nil, scheme, true, "")
			reporter := jobframework.NewResourceReporter(cached, scheme, handle)
			qu := &queueapi.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace, UID: "queue-unit-uid"},
				Spec: queueapi.QueueUnitSpec{
					Priority: ptr.To(int32(10)), PodSets: handle.GetHandle().PodSet(ctx, job),
					ConsumerRef: &corev1.ObjectReference{APIVersion: "batch/v1", Kind: "Job", Namespace: job.Namespace, Name: job.Name},
				},
				Status: queueapi.QueueUnitStatus{Phase: queueapi.Running, Admissions: []queueapi.Admission{{Name: job.Name, Replicas: 2, Running: 2}}},
			}
			must(api.Create(ctx, qu))
			// Initialize synchronously; background queue maintenance is not needed.
			q := &PriorityQueue{
				name: "queue", fw: testfake.NewFakeHandle(nil), lessFunc: less,
				queueCr: &queueapi.Queue{Spec: queueapi.QueueSpec{QueuePolicy: "Block"}},
				assumed: map[string]struct{}{}, updating: map[string]struct{}{},
				queueUnits: map[string]*queueframework.QueueUnitInfo{},
				blocked:    true, waitPodsRunning: true,
			}
			q.cond = sync.NewCond(&q.lock)
			added, err := q.AddQueueUnitInfo(queueframework.NewQueueUnitInfo(qu))
			must(err)
			if !assert.False(t, added, "the fully running job should already be out of the queue") {
				t.FailNow()
			}

			// Report the Job/Pod state and deliver QueueUnit updates to the
			// queue until spec/status stop changing. Bound retries to avoid hangs.
			key := client.ObjectKeyFromObject(qu)
			reportState := func() {
				t.Helper()
				for i := 0; i < 10; i++ {
					before := qu.DeepCopy()
					_, err := reporter.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
						Namespace: "batch/v1/Job|" + job.Namespace, Name: job.Name,
					}})
					must(err)
					must(api.Get(ctx, key, qu))
					must(q.Update(before, qu))
					if equality.Semantic.DeepEqual(before.Spec, qu.Spec) && equality.Semantic.DeepEqual(before.Status, qu.Status) {
						return
					}
				}
				t.Fatal("resource report did not converge after 10 reconciles")
			}

			// 2. The Pod failure is observed before the Job status catches up.
			if tc.failIndex {
				pods[0].Status.Phase = corev1.PodFailed
				must(api.Status().Update(ctx, pods[0]))
			}
			reportState()

			// 3. Reproduce the outstanding reclaim request, then let the Job
			// confirm that index 0 failed permanently and will not be retried.
			if tc.failIndex {
				assert.Equal(t, int64(1), qu.Status.Admissions[0].Running)
				assert.Contains(t, q.assumed, "default/job")
				before := qu.DeepCopy()
				qu.Status.Admissions[0].ReclaimState = &queueapi.ReclaimState{Replicas: 1}
				must(api.Status().Update(ctx, qu))
				must(q.Update(before, qu))
				job.Status.Active, job.Status.Failed = 1, 1
				job.Status.FailedIndexes = ptr.To("0")
				must(api.Status().Update(ctx, job))
				reportState()
			}
			var allPods corev1.PodList
			must(api.List(ctx, &allPods))
			phases := map[corev1.PodPhase]int{}
			for _, pod := range allPods.Items {
				phases[pod.Status.Phase]++
				assert.Equal(t, "node", pod.Spec.NodeName, "reporting must not unbind existing Pods")
			}
			wantFailed := 0
			if tc.failIndex {
				wantFailed = 1
			}
			assert.Equal(t, wantFailed, phases[corev1.PodFailed])
			assert.Equal(t, 2-wantFailed, phases[corev1.PodRunning])
			assert.Zero(t, phases[corev1.PodPending])
			if !assert.Len(t, qu.Status.Admissions, 1) {
				t.FailNow()
			}
			admission := qu.Status.Admissions[0]
			assert.Equal(t, int64(2-wantFailed), admission.Running, "the reporter must have observed the Pod state")
			assert.Nil(t, admission.ReclaimState, "released replicas must also settle the outstanding reclaim request")

			// 4. The next job should proceed in both cases, with no replicas left
			// to schedule. This assertion fails on the unfixed permanent-failure path.
			next := makeTestQueueUnit("default", "next", 5)
			added, err = q.AddQueueUnitInfo(next)
			must(err)
			if !assert.True(t, added) {
				t.FailNow()
			}
			err, _ = q.Reserve(ctx, next)
			t.Logf("Pods: Running=%d Failed=%d Pending=%d; QueueUnit: demand=%d admitted=%d reportedRunning=%d; next job: %v",
				phases[corev1.PodRunning], phases[corev1.PodFailed], phases[corev1.PodPending],
				qu.Spec.PodSets[0].Count, admission.Replicas, admission.Running, err)
			assert.NoError(t, err, "a permanently failed index must not block the next job")
		})
	}
}

// nonTerminalPodClient models the deployed Pod cache selector, which excludes
// Failed/Succeeded Pods. The backing API retains them. Informer delivery is not tested.
type nonTerminalPodClient struct{ client.Client }

func (c nonTerminalPodClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		live := make([]corev1.Pod, 0, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
				live = append(live, pod)
			}
		}
		pods.Items = live
	}
	return nil
}
