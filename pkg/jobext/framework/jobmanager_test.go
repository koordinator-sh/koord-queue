package framework

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func TestGetPodSetName(t *testing.T) {
	// 初始化 scheme
	s := runtime.NewScheme()
	_ = v1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	tests := []struct {
		name               string
		queueUnit          *v1alpha1.QueueUnit
		pod                *v1.Pod
		jobHandles         map[string]JobHandle
		expectedPodSetName string
	}{
		{
			name: "ConsumerRef is nil, get podset name from pod annotation",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-qu",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{
					ConsumerRef: nil,
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.RelatedPodSetAnnoKey: "worker",
					},
				},
			},
			expectedPodSetName: "worker",
		},
		{
			name: "Pod has podset annotation, return it directly",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-qu",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{
					ConsumerRef: &v1.ObjectReference{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       "test-job",
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.RelatedPodSetAnnoKey: "main",
					},
				},
			},
			expectedPodSetName: "main",
		},
		{
			name: "No job handle found for consumer ref",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-qu",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{
					ConsumerRef: &v1.ObjectReference{
						APIVersion: "unknown/v1",
						Kind:       "UnknownJob",
						Name:       "test-job",
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			},
			jobHandles:         map[string]JobHandle{},
			expectedPodSetName: "",
		},
		{
			name: "Job handle found but no custom GetPodSetName implementation",
			queueUnit: &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-qu",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{
					ConsumerRef: &v1.ObjectReference{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       "test-job",
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.RelatedPodSetAnnoKey: "worker",
					},
				},
			},
			jobHandles: map[string]JobHandle{
				"batch/v1/Job": NewJobHandle(time.Minute, time.Minute, &mockGenericJobExtension{}, false),
			},
			expectedPodSetName: "worker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构造 fake client
			cl := fake.NewClientBuilder().WithScheme(s).Build()

			// 创建 manager 实例
			manager := &manager{
				cli:        cl,
				jobHandles: tt.jobHandles,
			}

			// 调用被测函数
			result := manager.GetPodSetName(context.Background(), tt.queueUnit, tt.pod)

			// 验证结果
			if diff := cmp.Diff(tt.expectedPodSetName, result); diff != "" {
				t.Errorf("GetPodSetName() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// mockGenericJobExtension 实现 GenericJobExtension 接口用于测试
type mockGenericJobExtension struct{}

func (m *mockGenericJobExtension) Object() client.Object {
	return nil
}

func (m *mockGenericJobExtension) DeepCopy(client.Object) client.Object {
	return nil
}

func (m *mockGenericJobExtension) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{}
}

func (m *mockGenericJobExtension) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	return v1.ResourceList{}
}

func (m *mockGenericJobExtension) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	return nil
}

func (m *mockGenericJobExtension) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	return "", nil
}

func (m *mockGenericJobExtension) QueueUnitSuffix() string {
	return ""
}

func (m *mockGenericJobExtension) GetPodSetName(ownerName string, p *v1.Pod) string {
	// 返回一个固定的值用于测试
	return "mock-podset"
}

func (m *mockGenericJobExtension) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return nil
}

func (m *mockGenericJobExtension) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (JobStatus, time.Time) {
	return "", time.Now()
}

func (m *mockGenericJobExtension) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	return nil
}

func (m *mockGenericJobExtension) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	return nil
}

func (m *mockGenericJobExtension) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	return nil
}

func (m *mockGenericJobExtension) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	return true
}

func (m *mockGenericJobExtension) GetRelatedQueueUnit(ctx context.Context, obj client.Object, client client.Client) (*v1alpha1.QueueUnit, error) {
	return nil, nil
}

func (m *mockGenericJobExtension) GetRelatedJob(ctx context.Context, qu *v1alpha1.QueueUnit, client client.Client) client.Object {
	return nil
}

func (m *mockGenericJobExtension) SupportReservation() (GenericReservationJobExtension, bool) {
	return nil, false
}

func (m *mockGenericJobExtension) SupportNetworkAware() (NetworkAwareJobExtension, bool) {
	return nil, false
}
