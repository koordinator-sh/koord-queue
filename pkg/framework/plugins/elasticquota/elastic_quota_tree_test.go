package elasticquotatree

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	"github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	eqv1beta1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/elasticquotatree"
	internalruntime "github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"
	"github.com/stretchr/testify/assert"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

func newTestElasticQuota(t *testing.T) *ElasticQuota {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset()
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	var plg framework.Plugin
	proxy := func(pf internalruntime.PluginFactory) internalruntime.PluginFactory {
		return func(configuration runtime.Object, handle framework.Handle) (framework.Plugin, error) {
			plg, _ = pf(configuration, handle)
			return plg, nil
		}
	}
	registry := internalruntime.Registry{
		Name: proxy(FakeNew),
	}
	_, err := internalruntime.NewFramework(
		registry,
		nil,
		"",
		informers,
		versionedInformers,
		record.NewFakeRecorder(100),
		versionedclient,
		1, nil)

	if err != nil {
		panic(err)
	}
	return plg.(*ElasticQuota)
}

func TestFilter(t *testing.T) {
	tests := []struct {
		name          string
		queueunit     *v1alpha1.QueueUnit
		tree          *eqv1beta1.ElasticQuotaTree
		request       map[string]framework.Admission
		namespace     string
		isPreemptible bool
		expectError   bool
	}{
		{
			name: "Valid non-preemptible request within min",
			request: map[string]framework.Admission{
				"admission-1": {Name: "admission-1", Replicas: 1},
			},
			queueunit: queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "false"}).PodSets([]v1beta1.PodSet{{
				Name: "admission-1",
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					}}}}}}, Count: 1}}...).QueueUnit(),
			tree: queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
				Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("1")}, v1.ResourceList{"cpu": resource.MustParse("10")}).Obj(),
			namespace:   "default",
			expectError: false,
		},
		{
			name: "Invalid non-preemptible request over min",
			request: map[string]framework.Admission{
				"admission-1": {Name: "admission-1", Replicas: 3},
			},
			queueunit: queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "false"}).PodSets([]v1beta1.PodSet{{
				Name: "admission-1",
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					}}}}}}, Count: 1}}...).QueueUnit(),
			tree: queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
				Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("0")}, v1.ResourceList{"cpu": resource.MustParse("10")}).Obj(),
			namespace:   "default",
			expectError: true,
		},
		{
			name: "Valid preemptible request within max",
			request: map[string]framework.Admission{
				"admission-1": {Name: "admission-1", Replicas: 1},
			},
			queueunit: queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "true"}).PodSets([]v1beta1.PodSet{{
				Name: "admission-1",
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					}}}}}}, Count: 1}}...).QueueUnit(),
			tree: queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
				Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("1")}, v1.ResourceList{"cpu": resource.MustParse("1")}).Obj(),
			namespace:   "default",
			expectError: false,
		},
		{
			name: "Invalid preemptible request over max",
			request: map[string]framework.Admission{
				"admission-1": {Name: "admission-1", Replicas: 3},
			},
			queueunit: queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "true"}).PodSets([]v1beta1.PodSet{{
				Name: "admission-1",
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					}}}}}}, Count: 1}}...).QueueUnit(),
			tree: queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
				Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("0")}, v1.ResourceList{"cpu": resource.MustParse("0")}).Obj(),
			namespace:   "default",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eq := newTestElasticQuota(t)
			eq.AddElasticQuotaTree(tt.tree)
			ui := framework.NewQueueUnitInfo(tt.queueunit)

			status := eq.Filter(context.TODO(), ui, tt.request)
			if status.Code() == framework.Success && tt.expectError {
				t.Errorf("expected an error but got success")
			} else if status.Code() != framework.Success && !tt.expectError {
				t.Errorf("unexpected error: %v", status.Message())
			}
		})
	}
}

func TestReserve(t *testing.T) {
	eq := newTestElasticQuota(t)
	tree := queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
		Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("1")}, v1.ResourceList{"cpu": resource.MustParse("1")}).Obj()
	eq.AddElasticQuotaTree(tree)
	unit := queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "true"}).PodSets([]v1beta1.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 1}}...).QueueUnit()
	ui := framework.NewQueueUnitInfo(unit)
	admission := map[string]framework.Admission{
		"admission-1": {Name: "admission-1", Replicas: 1},
	}

	// 初始状态
	info := eq.elasticQuotaTree.GetElasticQuotaInfoByNamespace(ui.Unit.Namespace)
	initialUsed := info.Used.Clone()

	// 调用 Reserve
	eq.Reserve(context.TODO(), ui, admission)
	// 检查 Used 是否增加
	if !info.Used.ResourceNames().Has(string(v1.ResourceCPU)) {
		t.Errorf("expected used resource has cpu, but got %v", info.Used.ResourceNames())
	}
	for rName, value := range info.Used.Resources {
		expected := initialUsed.Resources[rName] + 1000 // 假设每个资源单位是 1
		if value != expected {
			t.Errorf("Expected used %v for %s, got %v", expected, rName, info.Used.Resources[rName])
		}
	}

	ui.Unit.Status.Admissions = []v1alpha1.Admission{{
		Name:     "admission-1",
		Replicas: 1,
	}}
	eq.DequeueComplete(context.TODO(), ui)
	// 检查 Used 是否增加
	if !info.Used.ResourceNames().Has(string(v1.ResourceCPU)) {
		t.Errorf("expected used resource has cpu, but got %v", info.Used.ResourceNames())
	}
	for rName, value := range info.Used.Resources {
		expected := initialUsed.Resources[rName] + 1000 // 假设每个资源单位是 1
		if value != expected {
			t.Errorf("Expected used %v for %s, got %v", expected, rName, info.Used.Resources[rName])
		}
	}

	// 调用 Reserve
	eq.Reserve(context.TODO(), ui, admission)
	// 检查 Used 是否增加
	if !info.Used.ResourceNames().Has(string(v1.ResourceCPU)) {
		t.Errorf("expected used resource has cpu, but got %v", info.Used.ResourceNames())
	}
	for rName, value := range info.Used.Resources {
		expected := initialUsed.Resources[rName] + 2000 // 假设每个资源单位是 1
		if value != expected {
			t.Errorf("Expected used %v for %s, got %v", expected, rName, info.Used.Resources[rName])
		}
	}

	ui.Unit.Status.Admissions = []v1alpha1.Admission{{
		Name:     "admission-1",
		Replicas: 2,
	}}
	eq.DequeueComplete(context.TODO(), ui)
	// 检查 Used 是否增加
	if !info.Used.ResourceNames().Has(string(v1.ResourceCPU)) {
		t.Errorf("expected used resource has cpu, but got %v", info.Used.ResourceNames())
	}
	for rName, value := range info.Used.Resources {
		expected := initialUsed.Resources[rName] + 2000 // 假设每个资源单位是 1
		if value != expected {
			t.Errorf("Expected used %v for %s, got %v", expected, rName, info.Used.Resources[rName])
		}
	}
}

func TestReserveConcurrent(t *testing.T) {
	eq := newTestElasticQuota(t)
	tree := queueunits.ElasticQuotaTree(v1.ResourceList{"cpu": resource.MustParse("10")}, v1.ResourceList{"cpu": resource.MustParse("10")}).
		Child("child1", []string{"default"}, v1.ResourceList{"cpu": resource.MustParse("1")}, v1.ResourceList{"cpu": resource.MustParse("1")}).Obj()
	eq.AddElasticQuotaTree(tree)
	unit := queueunits.MakeQueueUnit("unit-1", "default").Labels(map[string]string{"quota.scheduling.alibabacloud.com/preemptible": "true"}).PodSets([]v1beta1.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 1}}...).QueueUnit()
	ui := framework.NewQueueUnitInfo(unit)
	admission := map[string]framework.Admission{
		"admission-1": {Name: "admission-1", Replicas: 2},
	}
	// 初始状态
	info := eq.elasticQuotaTree.GetElasticQuotaInfoByNamespace(ui.Unit.Namespace)
	initialUsed := info.Used.Clone()

	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			eq.Reserve(context.TODO(), ui, admission)
		}()
	}
	wg.Wait()

	// 检查最终状态
	if !info.Used.ResourceNames().Has(string(v1.ResourceCPU)) {
		t.Errorf("expected used resource has cpu, but got %v", info.Used.ResourceNames())
	}
	for rName := range info.Used.Resources {
		expected := 2000 + initialUsed.Resources[rName] // one queueunit can be reserved once before admitted
		if info.Used.Resources[rName] != int64(expected) {
			t.Errorf("Expected used %v for %s after concurrent reserve, got %v", expected, rName, info.Used.Resources[rName])
		}
	}
}

func TestGetElasticQuotaInfoWithoutLock(t *testing.T) {
	// 创建mock elasticQuotaTree实例
	mockTree := &elasticquotatree.ElasticQuotaTree{
		Name2Quota:        make(map[string]*elasticquotatree.ElasticQuotaInfo),
		NamespaceToQuota:  make(map[string]string),
		ElasticQuotaInfos: make(elasticquotatree.ElasticQuotaInfos),
	}

	// 创建ElasticQuota插件实例
	eq := &ElasticQuota{
		elasticQuotaTree:  mockTree,
		namespaceToQuotas: make(map[string]sets.Set[string]),
	}

	// 测试用例1: QueueUnit没有quota标签，应该根据namespace查找
	t.Run("Get quota by namespace", func(t *testing.T) {
		queueUnit := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-unit",
				Namespace: "test-namespace",
			},
		}

		// 创建一个命名空间对应的quota信息
		quotaInfo := &elasticquotatree.ElasticQuotaInfo{
			Name:       "test-quota",
			FullName:   "root/test-quota",
			Namespaces: []string{"test-namespace"},
		}

		// 将quota信息添加到mockTree中
		mockTree.ElasticQuotaInfos["root/test-quota"] = quotaInfo
		mockTree.Name2Quota["test-quota"] = quotaInfo
		mockTree.NamespaceToQuota["test-namespace"] = "root/test-quota"

		quota, err := eq.getElasticQuotaInfoWithoutLock(queueUnit)
		assert.NoError(t, err)
		assert.Equal(t, "test-quota", quota.Name)
	})

	// 测试用例2: QueueUnit有quota标签，应该根据标签查找
	t.Run("Get quota by label", func(t *testing.T) {
		queueUnit := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-unit",
				Namespace: "test-namespace",
				Labels: map[string]string{
					"quota.scheduling.alibabacloud.com/name": "test-label-quota",
				},
			},
		}

		// 创建一个标签对应的quota信息
		quotaInfo := &elasticquotatree.ElasticQuotaInfo{
			Name:       "test-label-quota",
			FullName:   "root/test-label-quota",
			Namespaces: []string{"test-namespace"},
		}

		// 将quota信息添加到mockTree中
		mockTree.Name2Quota["test-label-quota"] = quotaInfo

		quota, err := eq.getElasticQuotaInfoWithoutLock(queueUnit)
		assert.NoError(t, err)
		assert.Equal(t, "test-label-quota", quota.Name)
	})

	// 测试用例3: 启用ElasticQuotaTreeCheckAvailableQuota特性，quota在namespace中不可用
	t.Run("Quota not available in namespace with feature enabled", func(t *testing.T) {
		// 保存原始feature状态并在测试后恢复
		originalElasticQuotaState := features.Enabled(features.ElasticQuota)
		originalCheckAvailableQuotaState := features.Enabled(features.ElasticQuotaTreeCheckAvailableQuota)

		// 启用相关特性
		utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.ElasticQuota):                        true,
			string(features.ElasticQuotaTreeCheckAvailableQuota): true,
		})
		defer func() {
			// 恢复原始状态
			utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
				string(features.ElasticQuota):                        originalElasticQuotaState,
				string(features.ElasticQuotaTreeCheckAvailableQuota): originalCheckAvailableQuotaState,
			})
		}()

		queueUnit := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-unit",
				Namespace: "test-namespace",
				Labels: map[string]string{
					"quota.scheduling.alibabacloud.com/name": "test-unavailable-quota",
				},
			},
		}

		// 设置namespace中可用的quota为空
		eq.namespaceToQuotas["test-namespace"] = sets.New[string]()

		quota, err := eq.getElasticQuotaInfoWithoutLock(queueUnit)
		assert.Error(t, err)
		assert.Nil(t, quota)
		assert.Equal(t, "quota is not available in namespace", err.Error())
	})

	// 测试用例4: QueueUnit不属于任何elastic quota
	t.Run("QueueUnit does not belong to any elastic quota", func(t *testing.T) {
		queueUnit := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-unit",
				Namespace: "test-namespace",
			},
		}

		// 清空mockTree中的信息
		mockTree.Name2Quota = make(map[string]*elasticquotatree.ElasticQuotaInfo)
		mockTree.NamespaceToQuota = make(map[string]string)

		quota, err := eq.getElasticQuotaInfoWithoutLock(queueUnit)
		assert.Error(t, err)
		assert.Nil(t, quota)
		assert.Equal(t, "the job doesn't belong to any elastic quota", err.Error())
	})
}
