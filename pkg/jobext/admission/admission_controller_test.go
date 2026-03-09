package admissioncontroller

import (
	"testing"

	v1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildXorAdmit(t *testing.T) {
	// 创建测试用的 AdmissionController 实例
	controller := &AdmissionControlelr{
		cli: nil, // 在单元测试中，不需要实际的 client.Client
	}

	// 准备测试用的 admissions 数据
	admissions := []v1alpha1.Admission{
		{
			Name:     "podset-1",
			Replicas: 2,
		},
		{
			Name:     "podset-2",
			Replicas: 1,
		},
	}

	// 准备 admitted 和 unadmitted 的 Pod 映射
	admitted := map[string][]*corev1.Pod{
		"podset-1": {
			{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Labels: map[string]string{"scheduler-admission": "true"}}},
		},
		"podset-2": {
			{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Labels: map[string]string{"scheduler-admission": "true"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Labels: map[string]string{"scheduler-admission": "true"}}},
		},
	}

	unadmitted := map[string][]*corev1.Pod{
		"podset-1": {
			{ObjectMeta: metav1.ObjectMeta{Name: "pod-2"}},
		},
	}

	// 调用 buildXorAdmit 方法
	xorAdmit := controller.buildXorAdmit(admissions, admitted, unadmitted)

	// 验证结果是否符合预期
	assert.Equal(t, 1, len(xorAdmit["podset-1"]))
	assert.Equal(t, "pod-2", xorAdmit["podset-1"][0].Name)

	assert.Equal(t, 1, len(xorAdmit["podset-2"]))
	assert.Equal(t, "pod-3", xorAdmit["podset-2"][0].Name)
}
