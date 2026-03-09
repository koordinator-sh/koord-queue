package framework

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
	"github.com/kube-queue/kube-queue/pkg/jobext/test/util/wrappers"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func TestReconcilePodSets(t *testing.T) {
	tests := []struct {
		name       string
		current    []kueue.PodSet
		expect     []kueue.PodSet
		expectNeed bool
		expectSets []kueue.PodSet
	}{
		{
			name: "No change",
			current: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
			expect: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
			expectNeed: false,
			expectSets: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
		},
		{
			name: "Update count",
			current: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
			expect: []kueue.PodSet{
				{Name: "ps-1", Count: 4},
				{Name: "ps-2", Count: 3},
			},
			expectNeed: true,
			expectSets: []kueue.PodSet{
				{Name: "ps-1", Count: 4},
				{Name: "ps-2", Count: 3},
			},
		},
		{
			name: "Add new podset",
			current: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
			},
			expect: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
			expectNeed: true,
			expectSets: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
		},
		{
			name: "Remove stale podset",
			current: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
				{Name: "ps-2", Count: 3},
			},
			expect: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
			},
			expectNeed: true,
			expectSets: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qu := &v1alpha1.QueueUnit{
				Spec: v1alpha1.QueueUnitSpec{
					PodSets: tt.current,
				},
			}

			needUpdate := reconcilePodSets(qu, tt.expect)

			assert.Equal(t, tt.expectNeed, needUpdate)
			assert.Equal(t, tt.expectSets, qu.Spec.PodSets)
		})
	}
}

func TestReconcileOveradmission(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = kueue.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	tests := []struct {
		name           string
		queueUnit      *v1alpha1.QueueUnit
		pods           []v1.Pod
		expectedAdmits []kueue.PodSet
		expectRequeue  bool
	}{
		{
			name: "No overadmission",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Spec: v1alpha1.QueueUnitSpec{
					PodSets: []kueue.PodSet{
						{Name: "ps-1", Count: 2},
					},
				},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 2},
					},
				},
			},
			pods: []v1.Pod{
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				},
			},
			expectedAdmits: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
			},
			expectRequeue: false,
		},
		{
			name: "Overadmission with running pods",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Spec: v1alpha1.QueueUnitSpec{
					PodSets: []kueue.PodSet{
						{Name: "ps-1", Count: 1},
					},
				},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 3},
					},
				},
			},
			pods: []v1.Pod{
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				},
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				},
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				},
			},
			expectedAdmits: []kueue.PodSet{
				{Name: "ps-1", Count: 3},
			},
			expectRequeue: false,
		},
		{
			name: "Overadmission with pending pods",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Spec: v1alpha1.QueueUnitSpec{
					PodSets: []kueue.PodSet{
						{Name: "ps-1", Count: 2},
					},
				},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 4},
					},
				},
			},
			pods: []v1.Pod{
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				},
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: "default/test-qu",
						util.RelatedPodSetAnnoKey:    "ps-1",
					}},
					Status: v1.PodStatus{Phase: v1.PodPending},
				},
			},
			expectedAdmits: []kueue.PodSet{
				{Name: "ps-1", Count: 2},
			},
			expectRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.queueUnit).WithStatusSubresource(&v1alpha1.QueueUnit{}).
				WithIndex(&v1.Pod{}, util.RelatedQueueUnitPodSetCacheFieldsForTest, func(o client.Object) []string {
					po, ok := o.(*corev1.Pod)
					if !ok {
						return nil
					}
					qu, ps := po.Annotations[util.RelatedQueueUnitAnnoKey], po.Annotations[util.RelatedPodSetAnnoKey]
					if qu == "" || ps == "" {
						return nil
					}
					return []string{qu + "/" + ps}
				}).WithLists(&v1.PodList{Items: tt.pods})
			cli := builder.Build()

			reporter := &ResourceReporter{
				client: cli,
			}

			ctx := context.Background()
			podsByPs := map[string][]*corev1.Pod{}
			for _, ps := range tt.queueUnit.Status.Admissions {
				pods := &corev1.PodList{}
				if err := cli.List(ctx, pods, client.MatchingFields{util.RelatedQueueUnitPodSetCacheFieldsForTest: tt.queueUnit.Namespace + "/" + tt.queueUnit.Name + "/" + ps.Name}); err != nil {
					t.Fatal(err)
				}
				pps := []*corev1.Pod{}
				for _, p := range pods.Items {
					pps = append(pps, &p)
				}
				podsByPs[ps.Name] = pps
			}

			needRequeue, err := reporter.reconcileOveradmission(ctx, klog.FromContext(ctx), tt.queueUnit, podsByPs)
			if err != nil {
				t.Fatal(err)
			}

			newQu := &v1alpha1.QueueUnit{}
			if err := cli.Get(ctx, types.NamespacedName{Namespace: tt.queueUnit.Namespace, Name: tt.queueUnit.Name}, newQu); err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectRequeue, needRequeue)
			assert.Equal(t, len(tt.expectedAdmits), len(newQu.Status.Admissions))
			for i := range tt.expectedAdmits {
				assert.Equal(t, tt.expectedAdmits[i].Name, newQu.Status.Admissions[i].Name)
				assert.Equal(t, tt.expectedAdmits[i].Count, int32(newQu.Status.Admissions[i].Replicas))
			}
		})
	}
}

func TestReconcileReclaim(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = kueue.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	tests := []struct {
		name             string
		queueUnit        *v1alpha1.QueueUnit
		admissions       []v1alpha1.Admission
		podsByPs         map[string][]*corev1.Pod
		expectedReclaims map[string][]*corev1.Pod
	}{
		{
			name: "No reclaim needed",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 2, ReclaimState: nil},
					},
				},
			},
			admissions: []v1alpha1.Admission{
				{Name: "ps-1", Replicas: 2, ReclaimState: nil},
			},
			podsByPs: map[string][]*corev1.Pod{
				"ps-1": {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Spec:   v1.PodSpec{NodeName: "test-node"},
						Status: v1.PodStatus{Phase: v1.PodRunning},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Spec:   v1.PodSpec{NodeName: "test-node"},
						Status: v1.PodStatus{Phase: v1.PodRunning},
					},
				},
			},
			expectedReclaims: map[string][]*corev1.Pod{},
		},
		{
			name: "Reclaim needed due to ReclaimState",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 3, ReclaimState: &v1alpha1.ReclaimState{Replicas: 2}},
					},
				},
			},
			admissions: []v1alpha1.Admission{
				{Name: "ps-1", Replicas: 3, ReclaimState: &v1alpha1.ReclaimState{Replicas: 2}},
			},
			podsByPs: map[string][]*corev1.Pod{
				"ps-1": {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Spec:   v1.PodSpec{NodeName: "test-node"},
						Status: v1.PodStatus{Phase: v1.PodRunning},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
				},
			},
			expectedReclaims: map[string][]*corev1.Pod{
				"ps-1": {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
				},
			},
		},
		{
			name: "Partial reclaim",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "test-qu", Namespace: "default"},
				Status: v1alpha1.QueueUnitStatus{
					Admissions: []v1alpha1.Admission{
						{Name: "ps-1", Replicas: 4, ReclaimState: &v1alpha1.ReclaimState{Replicas: 3}},
					},
				},
			},
			admissions: []v1alpha1.Admission{
				{Name: "ps-1", Replicas: 4, ReclaimState: &v1alpha1.ReclaimState{Replicas: 3}},
			},
			podsByPs: map[string][]*corev1.Pod{
				"ps-1": {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Spec:   v1.PodSpec{NodeName: "test-node"},
						Status: v1.PodStatus{Phase: v1.PodRunning},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
				},
			},
			expectedReclaims: map[string][]*corev1.Pod{
				"ps-1": {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Namespace: "default", Annotations: map[string]string{
							util.RelatedQueueUnitAnnoKey: "default/test-qu",
							util.RelatedPodSetAnnoKey:    "ps-1",
						}},
						Status: v1.PodStatus{Phase: v1.PodPending},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.queueUnit).WithStatusSubresource(&v1alpha1.QueueUnit{}).
				WithIndex(&v1.Pod{}, util.RelatedQueueUnitPodSetCacheFieldsForTest, func(o client.Object) []string {
					po, ok := o.(*corev1.Pod)
					if !ok {
						return nil
					}
					qu, ps := po.Annotations[util.RelatedQueueUnitAnnoKey], po.Annotations[util.RelatedPodSetAnnoKey]
					if qu == "" || ps == "" {
						return nil
					}
					return []string{qu + "/" + ps}
				}).WithLists(&v1.PodList{})
			cli := builder.Build()

			reporter := &ResourceReporter{
				client: cli,
			}

			reclaims := reporter.reconcileReclaim(tt.admissions, tt.podsByPs)

			assert.Equal(t, len(tt.expectedReclaims), len(reclaims))
			for psName, expectedPods := range tt.expectedReclaims {
				actualPods, exists := reclaims[psName]
				assert.True(t, exists)
				assert.Equal(t, len(expectedPods), len(actualPods))
				for i := range expectedPods {
					assert.Equal(t, expectedPods[i].Name, actualPods[i].Name)
				}
			}
		})
	}
}

func Test_syncInFlightWorkers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = kueue.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	tests := []struct {
		name string
		pods []client.Object

		expectRes       map[corev1.ResourceName]int64
		expectAdmission []v1alpha1.Admission
	}{
		{
			name: "pods with ps annotations",
			pods: []client.Object{
				wrappers.MakePod().Namespace("default").Name("pod1").Annotation(util.RelatedPodSetAnnoKey, "ps-1").Node("mock").Res(map[v1.ResourceName]string{"cpu": "1"}).Obj(),
				wrappers.MakePod().Namespace("default").Name("pod2").Annotation(util.RelatedPodSetAnnoKey, "gpu-ps").Node("mock").Res(map[v1.ResourceName]string{"memory": "5Gi", "nvidia.com/gpu": "8"}).Obj(),
			},
			expectRes: map[v1.ResourceName]int64{
				v1.ResourceCPU:    1000,
				v1.ResourceMemory: 5 * 1024 * 1024 * 1024,
				"nvidia.com/gpu":  8,
			},
			expectAdmission: []v1alpha1.Admission{{Name: "ps-1", Resources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}, Replicas: 1, Running: 1},
				{Name: "gpu-ps", Replicas: 1, Running: 1, Resources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("5Gi"), "nvidia.com/gpu": resource.MustParse("8")}}},
		},
		{
			name: "pods without ps annotations",
			pods: []client.Object{
				wrappers.MakePod().Namespace("default").Name("pod1").Node("mock").Res(map[v1.ResourceName]string{"cpu": "1"}).Obj(),
				wrappers.MakePod().Namespace("default").Name("pod2").Node("mock").Res(map[v1.ResourceName]string{"memory": "5Gi", "nvidia.com/gpu": "8"}).Obj(),
			},
			expectRes: map[v1.ResourceName]int64{
				v1.ResourceCPU:    1000,
				v1.ResourceMemory: 5 * 1024 * 1024 * 1024,
				"nvidia.com/gpu":  8,
			},
			expectAdmission: []v1alpha1.Admission{{Name: "ps-1", Replicas: 1},
				{Name: "gpu-ps", Replicas: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qu := wrappers.MakeQueueUnit("qu", "default").
				PodSetSimple("ps-1", map[string]int64{"cpu": 1000}, 1).
				PodSetSimple("gpu-ps", map[string]int64{"memory": 5 * 1024 * 1024 * 1024, "nvidia.com/gpu": 1}, 1).
				Admission("ps-1", map[string]int64{}, 1).
				Admission("gpu-ps", map[string]int64{}, 1).
				QueueUnit()

			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(qu).WithStatusSubresource(&v1alpha1.QueueUnit{}).
				WithIndex(&v1.Pod{}, util.RelatedQueueUnitPodSetCacheFieldsForTest, func(o client.Object) []string {
					po, ok := o.(*corev1.Pod)
					if !ok {
						return nil
					}
					qu, ps := po.Annotations[util.RelatedQueueUnitAnnoKey], po.Annotations[util.RelatedPodSetAnnoKey]
					if qu == "" || ps == "" {
						return nil
					}
					return []string{qu + "/" + ps}
				}).WithObjects(tt.pods...)
			cli := builder.Build()

			reporter := &ResourceReporter{
				client: cli,
			}

			pods := []*corev1.Pod{}
			podsByPs := map[string][]*corev1.Pod{}
			for _, p := range tt.pods {
				pd, _ := p.(*corev1.Pod)
				pods = append(pods, pd)
				ps := p.GetAnnotations()[util.RelatedPodSetAnnoKey]
				if ps == "" {
					continue
				}
				if len(podsByPs[ps]) == 0 {
					podsByPs[ps] = []*corev1.Pod{}
				}
				podsByPs[ps] = append(podsByPs[ps], pd)
			}
			_, err := reporter.syncInFlightWorkers(context.Background(), klog.Background(), qu.DeepCopy(), pods, podsByPs)
			if !cmp.Equal(err, nil) {
				t.Fatal(err)
			}
			_ = cli.Get(context.Background(), types.NamespacedName{Name: qu.Name, Namespace: qu.Namespace}, qu)
			// first time syncInFlightWorkers will only update qu.Spec.Resources
			_, err = reporter.syncInFlightWorkers(context.Background(), klog.Background(), qu.DeepCopy(), pods, podsByPs)
			if !cmp.Equal(err, nil) {
				t.Fatal(err)
			}

			newQu := &v1alpha1.QueueUnit{}
			_ = cli.Get(context.Background(), types.NamespacedName{Name: qu.Name, Namespace: qu.Namespace}, newQu)
			convertFromRLToMap := func(rl v1.ResourceList) map[v1.ResourceName]int64 {
				m := make(map[v1.ResourceName]int64)
				for k, v := range rl {
					if k == v1.ResourceCPU {
						m[k] = v.MilliValue()
					} else {
						m[k] = v.Value()
					}
				}
				return m
			}
			if diff := cmp.Diff(tt.expectRes, convertFromRLToMap(newQu.Spec.Resource)); diff != "" {
				t.Errorf("case (%v), unexpected resource diff (-want, +got):\n%s", tt.name, diff)
			}
			if diff := cmp.Diff(tt.expectAdmission, newQu.Status.Admissions, cmpopts.SortSlices(func(a, b v1alpha1.Admission) bool {
				return a.Name < b.Name
			})); diff != "" {
				t.Errorf("case (%v), unexpected resource diff (-want, +got):\n%s", tt.name, diff)
			}
		})
	}
}
