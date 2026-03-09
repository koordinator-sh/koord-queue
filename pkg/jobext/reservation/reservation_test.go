package reservation

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgofake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetJobBlackList(t *testing.T) {
	testData := []struct {
		cmData        map[string]string
		expectedTerms []v1.NodeSelectorRequirement
	}{
		{
			cmData: map[string]string{
				"global_anomalousNodesGlobal_xingyun-aiph": `{"node1": [{"lastUpdateTimestamp": "2025-04-14T07:00:00Z", "errorMsg": "error", "errorCode": 1}]}`,
			},
			expectedTerms: []v1.NodeSelectorRequirement{
				{
					Key:      "kubernetes.io/hostname",
					Operator: v1.NodeSelectorOpNotIn,
					Values:   []string{"node1"},
				},
			},
		},
		{
			cmData: map[string]string{
				"global_anomalousNodesGlobal_xingyun-aiph": `{"node1": [{"lastUpdateTimestamp": "2023-04-14T07:00:00Z", "errorMsg": "error", "errorCode": 1}]}`,
			},
			expectedTerms: []v1.NodeSelectorRequirement{
				{
					Key:      "kubernetes.io/hostname",
					Operator: v1.NodeSelectorOpNotIn,
					Values:   []string{"node1"},
				},
			},
		},
	}

	for i, ttd := range testData {
		t.Run(fmt.Sprint(i), func(tt *testing.T) {
			// 创建一个 ConfigMap 对象
			blacklistConfigMap := &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kubedl-blacklist-data",
					Namespace: "kube-system",
				},
				Data: ttd.cmData,
			}

			// 添加 ConfigMap 到假的客户端
			// fakeClient.CoreV1().ConfigMaps("kube-system").Create(context.Background(), blacklistConfigMap, metav1.CreateOptions{})
			scheme := runtime.NewScheme()
			v1.AddToScheme(scheme)
			// 创建一个 GenericJobReconciler 实例
			reconciler := &ReservationController{
				cli: clientgofake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			reconciler.cli.Create(context.Background(), blacklistConfigMap)

			// 创建一个日志记录器
			logger := zapr.NewLogger(zap.NewNop())

			// 调用 GetJobBlackList 方法
			nodeSelectorTerms, _ := reconciler.GetJobBlackList(context.Background(), logger)

			// 验证结果
			expectedNodeSelectorTerms := ttd.expectedTerms
			if len(nodeSelectorTerms) != len(expectedNodeSelectorTerms) {
				t.Errorf("Index %v, Expected %d node selector terms, got %d", i, len(expectedNodeSelectorTerms), len(nodeSelectorTerms))
			}

			for i := range nodeSelectorTerms {
				if !reflect.DeepEqual(nodeSelectorTerms[i], expectedNodeSelectorTerms[i]) {
					t.Errorf("Index %v, Expected node selector term %v, got %v", i, expectedNodeSelectorTerms[i], nodeSelectorTerms[i])
				}
			}
		})
	}
}
