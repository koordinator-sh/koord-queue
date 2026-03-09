package framework

import (
	"context"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/stretchr/testify/assert"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestFindWorkloadFromPod(t *testing.T) {
	// 初始化 scheme
	s := runtime.NewScheme()
	_ = v1.AddToScheme(s)
	_ = rayv1.AddToScheme(s)

	// 构造 fake client
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	if err := cl.Create(context.Background(), &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "ray-cluster-withowner", Namespace: "default", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: "ray.io/v1",
		Kind:       "RayJob",
		Name:       "ray-job",
	}}}}); err != nil {
		t.Fatalf("failed to create RayCluster: %v", err)
	}

	// 创建 ResourceReporter 实例
	reporter := &manager{
		cli: cl,
		jobHandles: map[string]JobHandle{
			"batch/v1/Job": JobHandle{}, // 模拟 jobHandles 中包含 Job 类型
		},
	}

	// 测试用例定义
	tests := []struct {
		name        string
		pod         *v1.Pod
		expected    []ownerInfo
		expectError bool
	}{
		{
			name: "Deployment Owner",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "Deployment",
							Name:       "my-deploy",
						},
					},
				},
			},
			expected: []ownerInfo{
				{
					groupVersionKind: "dsw.alibaba.com/v1/NoteBook",
					name:             "my-deploy",
				},
			},
		},
		{
			name: "RayCluster without RayJob owner",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-ray",
					Namespace: "default",
					Annotations: map[string]string{
						util.RelatedAPIVersionKindAnnoKey: "ray.io/v1/RayCluster",
						util.RelatedObjectAnnoKey:         "ray-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ray.io/v1",
							Kind:       "RayCluster",
							Name:       "ray-cluster",
						},
					},
				},
			},
			expected: []ownerInfo{
				{
					groupVersionKind: "ray.io/v1/RayCluster",
					name:             "ray-cluster",
				},
			},
		},
		{
			name: "RayCluster with RayJob owner",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-ray-job",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ray.io/v1",
							Kind:       "RayCluster",
							Name:       "ray-cluster-withowner",
						},
					},
				},
			},
			expected: []ownerInfo{
				{
					groupVersionKind: "ray.io/v1/RayJob",
					name:             "ray-job",
				},
			},
		},
		{
			name: "Custom Job Handle",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-job",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "my-job",
						},
					},
				},
			},
			expected: []ownerInfo{
				{
					groupVersionKind: "batch/v1/Job",
					name:             "my-job",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测函数
			result := reporter.FindWorkloadFromPod(context.Background(), tt.pod)

			// 验证结果
			assert.NotNil(t, result)
			assert.Equal(t, tt.expected[0].groupVersionKind, result.groupVersionKind)
			assert.Equal(t, tt.expected[0].name, result.name)
		})
	}
}
