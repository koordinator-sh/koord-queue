package schedulingqueuev2

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	queueunitfake "github.com/kube-queue/api/pkg/client/clientset/versioned/fake"
	queueunitfakeex "github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/kube-queue/pkg/apis/config/scheme"
	framework "github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/framework/runtime"
	"github.com/kube-queue/kube-queue/pkg/test/fake"
	"github.com/kube-queue/kube-queue/pkg/utils"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func makeTestQueueUnit(namespace, name string, priority int32) *framework.QueueUnitInfo {
	unit := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			ResourceVersion: "1",
			UID:             types.UID(name),
		},
		Spec: v1alpha1.QueueUnitSpec{
			Priority: &priority,
			PodSets: []v1beta1.PodSet{{
				Name:  "default",
				Count: 1,
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						}},
					},
				},
			}},
		},
	}
	return framework.NewQueueUnitInfo(unit)
}

func TestAddQueueUnitInfo(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	added, err := q.AddQueueUnitInfo(item1)
	if !added || err != nil {
		t.Errorf("Expected item to be added, got error: %v", err)
	}

	// Duplicate should not be added
	added, err = q.AddQueueUnitInfo(item1)
	if added || err == nil {
		t.Errorf("Expected duplicate not to be added")
	}
}

func TestDelete(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	q.AddQueueUnitInfo(item1)

	err := q.Delete(item1.Unit)
	if err != nil {
		t.Errorf("Expected no error when deleting, got: %v", err)
	}

	if len(q.queue) != 0 || len(q.queueUnits) != 0 {
		t.Errorf("Expected queue and queueUnits to be empty after deletion")
	}
}

func TestUpdate(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	oldItem := makeTestQueueUnit("default", "job1", 10)
	q.AddQueueUnitInfo(oldItem)

	newItem := oldItem.Unit.DeepCopy()
	newItem.ResourceVersion = "2" // simulate update
	prio := int32(20)
	newItem.Spec.Priority = &prio

	err := q.Update(oldItem.Unit, newItem)
	if err != nil {
		t.Errorf("Expected no error during update, got: %v", err)
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	if len(q.queue) != 1 {
		t.Errorf("Expected queue length to remain 1 after update")
	}

	if q.queue[0].Unit.Spec.Priority == nil || *q.queue[0].Unit.Spec.Priority != 20 {
		t.Errorf("Expected priority to be updated to 20")
	}
}

func TestNext(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	item2 := makeTestQueueUnit("default", "job2", 20)

	q.AddQueueUnitInfo(item1)
	q.AddQueueUnitInfo(item2)

	ctx := context.Background()
	next, _ := q.Next(ctx)
	if next.Name != "default/job2" {
		t.Errorf("Expected higher priority job 'job2' to be selected first")
	}

	q.assumed["default/job2"] = struct{}{}
	next, _ = q.Next(ctx)
	if next.Name != "default/job2" {
		t.Errorf("Expected same item if assumed is set")
	}

	q.queue[0].Unit.Status.Admissions = []v1alpha1.Admission{{
		Name:     "default",
		Replicas: 1,
	}}
	next, _ = q.Next(ctx)
	if next.Name != "default/job1" {
		t.Errorf("Expected lower priority job 'job1' to be selected after blocking")
	}
}

func TestLength(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	item2 := makeTestQueueUnit("default", "job2", 20)

	q.AddQueueUnitInfo(item1)
	q.AddQueueUnitInfo(item2)

	if q.Length() != 2 {
		t.Errorf("Expected Length() to return 2, got %d", q.Length())
	}

	q.Delete(item1.Unit)
	if q.Length() != 1 {
		t.Errorf("Expected Length() to return 1 after deletion, got %d", q.Length())
	}
}

func TestSortedList(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	item2 := makeTestQueueUnit("default", "job2", 20)
	item3 := makeTestQueueUnit("default", "job3", 15)

	q.AddQueueUnitInfo(item1)
	q.AddQueueUnitInfo(item2)
	q.AddQueueUnitInfo(item3)

	detailMap := q.SortedList()
	active := detailMap["active"]
	if len(active) != 3 {
		t.Errorf("Expected 3 items in sorted list, got %d", len(active))
	}

	if active[0].Name != "job2" || active[1].Name != "job3" || active[2].Name != "job1" {
		t.Errorf("Sorted list order incorrect. Got %+v", active)
	}
}

func TestReserve(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	q.AddQueueUnitInfo(item1)

	err := q.Reserve(context.TODO(), item1)
	if err != nil {
		t.Errorf("Expected Reserve to succeed, got error: %v", err)
	}

	if _, ok := q.assumed[item1.Name]; !ok {
		t.Errorf("Expected job to be in assumed after reserve")
	}
}

func TestWaitForPodsRunningReserve(t *testing.T) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{})

	schemeModified := scheme.Scheme
	recorder := eventBroadcaster.NewRecorder(schemeModified, v1.EventSource{Component: utils.ControllerAgentName})

	queueClient := queueunitfake.NewSimpleClientset()
	queueInformerFactory := queueunitfakeex.NewSharedInformerFactory(queueClient, 0)

	queueInformerFactory.Start(wait.NeverStop)
	kubecli := kubefake.NewSimpleClientset()
	informersFactory := informers.NewSharedInformerFactory(kubecli, 0)
	fw, err := runtime.NewFramework(nil, nil, "", informersFactory, queueInformerFactory, recorder, queueClient, 1.0, nil)
	if err != nil {
		t.Fatalf("NewFramework error: %v", err)
	}
	q := NewPriorityQueue("test",
		&v1alpha1.Queue{ObjectMeta: metav1.ObjectMeta{
			Name: "test-wait-for-pods-running",
			Annotations: map[string]string{
				WaitForPodsRunningAnnotation: "true",
			},
		}},
		fw, nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	item2 := makeTestQueueUnit("default", "job2", 12)
	item3 := makeTestQueueUnit("default", "job3", 13)
	q.assumed = map[string]struct{}{"default/job2": struct{}{}}
	q.queueUnits = map[string]*framework.QueueUnitInfo{
		"default/job1": item1,
		"default/job2": item2,
		"default/job3": item3,
	}
	err = q.Reserve(context.TODO(), item3)
	if err == nil || !strings.HasPrefix(err.Error(), "preemption: waiting for queueUnit preempted") {
		t.Errorf("Expected Reserve to failed, actual %v", err)
	}
	err = q.Reserve(context.TODO(), item1)
	if err == nil || err.Error() != "no more queueUnit allowed to schedule in this queue" {
		t.Errorf("Expected Reserve to failed, actual %v", err)
	}
}

func TestAddUnschedulableIfNotPresent(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	q.AddUnschedulableIfNotPresent(context.Background(), item1, false)

	if q.nextIdx != 1 {
		t.Errorf("Expected nextIdx to increment by 1")
	}
}

func TestFix(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	item1 := makeTestQueueUnit("default", "job1", 10)
	q.AddQueueUnitInfo(item1)

	// Simulate mapping function that moves the queue unit away
	funcMap := func(qu *v1alpha1.QueueUnit) (string, error) {
		return "other-queue", nil
	}

	res := q.Fix(funcMap)
	if len(res) != 1 || res[0].Name != "default/job1" {
		t.Errorf("Expected Fix to return moved queueUnit")
	}

	if len(q.queue) != 0 || len(q.queueUnits) != 0 {
		t.Errorf("Expected queue to be cleared after Fix")
	}
}

func createQueueUnitInfo(name string, namespace string, priority *int32, initialAttemptTimestamp time.Time) *framework.QueueUnitInfo {
	return &framework.QueueUnitInfo{
		Name: namespace + "/" + name,
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				SelfLink:  "/api/v1/namespaces/" + namespace + "/pods/" + name,
				UID:       types.UID(name),
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: priority,
			},
		},
		InitialAttemptTimestamp: initialAttemptTimestamp,
	}
}

func TestLess(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Second)
	prio10 := int32(10)
	prio5 := int32(5)
	// prio0 := int32(0)

	tests := []struct {
		name     string
		a        *framework.QueueUnitInfo
		b        *framework.QueueUnitInfo
		expected int
	}{
		{
			name:     "Same UID",
			a:        createQueueUnitInfo("pod1", "default", &prio10, now),
			b:        createQueueUnitInfo("pod1", "default", &prio5, later),
			expected: 0,
		},
		{
			name:     "a higher priority",
			a:        createQueueUnitInfo("pod1", "default", &prio10, now),
			b:        createQueueUnitInfo("pod2", "default", &prio5, now),
			expected: -1,
		},
		{
			name:     "b higher priority",
			a:        createQueueUnitInfo("pod1", "default", &prio5, now),
			b:        createQueueUnitInfo("pod2", "default", &prio10, now),
			expected: 1,
		},
		{
			name:     "equal priority, a earlier timestamp",
			a:        createQueueUnitInfo("pod1", "default", &prio5, now),
			b:        createQueueUnitInfo("pod2", "default", &prio5, later),
			expected: -1,
		},
		{
			name:     "equal priority, b earlier timestamp",
			a:        createQueueUnitInfo("pod1", "default", &prio5, later),
			b:        createQueueUnitInfo("pod2", "default", &prio5, now),
			expected: 1,
		},
		{
			name:     "equal priority and timestamp, a.Name < b.Name",
			a:        createQueueUnitInfo("pod1", "default", &prio5, now),
			b:        createQueueUnitInfo("pod2", "default", &prio5, now),
			expected: -1,
		},
		{
			name:     "equal priority and timestamp, a.Name > b.Name",
			a:        createQueueUnitInfo("pod2", "default", &prio5, now),
			b:        createQueueUnitInfo("pod1", "default", &prio5, now),
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := less(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("less(%v) = %d; expected %d", tt.name, result, tt.expected)
			}
		})
	}
}

func TestPriorityQueue_Update(t *testing.T) {
	q := &PriorityQueue{
		name:       "test-queue",
		queueUnits: make(map[string]*framework.QueueUnitInfo),
		assumed:    make(map[string]struct{}),
		lessFunc:   less,
		lock:       sync.RWMutex{},
		sessionId:  0,
		blocked:    false,
		fw:         fake.NewFakeHandle(map[string]string{"default/test-unit": "test-quota"}),
		cond:       sync.NewCond(&sync.Mutex{}),
		nextIdx:    10,
	}

	// 初始化一个 QueueUnit
	oldUnit := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-unit",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.QueueUnitSpec{PodSets: []v1beta1.PodSet{{
			Name:  "default",
			Count: 1,
			Template: v1.PodTemplateSpec{Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				}},
			}},
		}}},
		Status: v1alpha1.QueueUnitStatus{
			Phase:      v1alpha1.Running,
			Admissions: []v1alpha1.Admission{{Name: "default", Replicas: 1}},
		},
	}

	// 添加到队列
	q.AddQueueUnitInfo(framework.NewQueueUnitInfo(oldUnit))

	// 验证是否已从 assumed 中删除
	if _, exists := q.assumed[oldUnit.Namespace+"/"+oldUnit.Name]; !exists {
		t.Errorf("Expected QueueUnit to be existed in assumed, but it do not exists")
	}

	// 更新 QueueUnit 的状态和资源版本
	newUnit := oldUnit.DeepCopy()
	newUnit.ResourceVersion = "2"
	newUnit.Status.Phase = v1alpha1.Running
	newUnit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 0}}

	// 调用 Update 方法
	err := q.Update(oldUnit, newUnit)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// 验证是否已从 assumed 中删除
	if _, exists := q.assumed[newUnit.Namespace+"/"+newUnit.Name]; exists {
		t.Errorf("Expected QueueUnit to be removed from assumed, but it still exists")
	}

	// 验证 sessionId 是否增加
	if q.sessionId <= 0 {
		t.Errorf("Expected sessionId to be incremented, got %d", q.sessionId)
	}

	// 验证 nextIdx 是否重置
	if !q.resetNextIdxFlag {
		t.Error("Expected resetNextIdxFlag to be reset")
	}
}

// 测试优先级变更的情况
func TestPriorityQueue_UpdatePriority(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	// 创建一个优先级为10的QueueUnit
	priority10 := int32(10)
	priority11 := int32(11)
	oldItem := makeTestQueueUnit("default", "job1", priority10)
	highPrioItem := makeTestQueueUnit("default", "job2", priority11)
	q.AddQueueUnitInfo(oldItem)
	q.AddQueueUnitInfo(highPrioItem)
	// 验证队列长度仍然为2
	assert.Equal(t, 2, len(q.queue))
	// 验证优先级已更新
	assert.Equal(t, priority11, *q.queue[0].Unit.Spec.Priority)
	// 验证队列中的元素是更新后的元素
	assert.Equal(t, "1", q.queue[0].Unit.ResourceVersion)

	// 创建一个优先级为20的新QueueUnit（模拟优先级变更）
	newItem := oldItem.Unit.DeepCopy()
	newItem.ResourceVersion = "2"
	priority20 := int32(20)
	newItem.Spec.Priority = &priority20

	// 调用Update方法
	err := q.Update(oldItem.Unit, newItem)
	assert.NoError(t, err)
	// 验证队列长度仍然为2
	assert.Equal(t, 2, len(q.queue))
	// 验证优先级已更新
	assert.Equal(t, priority20, *q.queue[0].Unit.Spec.Priority)
	// 验证队列中的元素是更新后的元素
	assert.Equal(t, "2", q.queue[0].Unit.ResourceVersion)

	// 创建一个优先级为20的新QueueUnit（模拟优先级变更）
	againNewItem := newItem.DeepCopy()
	againNewItem.ResourceVersion = "3"
	priority5 := int32(5)
	againNewItem.Spec.Priority = &priority5

	// 调用Update方法
	err = q.Update(newItem, againNewItem)
	assert.NoError(t, err)
	// 验证队列长度仍然为2
	assert.Equal(t, 2, len(q.queue))
	// 验证优先级已更新
	assert.Equal(t, priority11, *q.queue[0].Unit.Spec.Priority)
	// 验证队列中的元素是更新后的元素
	assert.Equal(t, "1", q.queue[0].Unit.ResourceVersion)
}

// 测试多个元素优先级变更后的排序
func TestPriorityQueue_UpdatePriorityOrder(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	// 创建三个QueueUnit，优先级分别为10, 20, 30
	priority10 := int32(10)
	priority20 := int32(20)
	priority30 := int32(30)

	item1 := makeTestQueueUnit("default", "job1", priority10)
	item2 := makeTestQueueUnit("default", "job2", priority20)
	item3 := makeTestQueueUnit("default", "job3", priority30)

	q.AddQueueUnitInfo(item1)
	q.AddQueueUnitInfo(item2)
	q.AddQueueUnitInfo(item3)

	// 验证初始顺序（按优先级降序）
	assert.Equal(t, "job3", q.queue[0].Unit.Name) // 优先级30
	assert.Equal(t, "job2", q.queue[1].Unit.Name) // 优先级20
	assert.Equal(t, "job1", q.queue[2].Unit.Name) // 优先级10

	// 将job1的优先级从10更新为25
	updatedItem1 := item1.Unit.DeepCopy()
	updatedItem1.ResourceVersion = "2"
	priority25 := int32(25)
	updatedItem1.Spec.Priority = &priority25

	// 调用Update方法
	err := q.Update(item1.Unit, updatedItem1)
	assert.NoError(t, err)

	// 验证更新后顺序（按优先级降序）
	assert.Equal(t, "job3", q.queue[0].Unit.Name) // 优先级30
	assert.Equal(t, "job1", q.queue[1].Unit.Name) // 优先级25
	assert.Equal(t, "job2", q.queue[2].Unit.Name) // 优先级20
}

// 测试状态和优先级都不变的情况
func TestPriorityQueue_UpdateNoChange(t *testing.T) {
	q := NewPriorityQueue("test", &v1alpha1.Queue{}, fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*PriorityQueue)

	// 创建一个QueueUnit
	priority10 := int32(10)
	oldItem := makeTestQueueUnit("default", "job1", priority10)
	q.AddQueueUnitInfo(oldItem)

	// 创建一个只有ResourceVersion不同的新QueueUnit
	newItem := oldItem.Unit.DeepCopy()
	newItem.ResourceVersion = "2"
	// 状态和优先级都保持不变

	// 调用Update方法前记录队列状态
	oldQueueLen := len(q.queue)
	oldPriority := q.queue[0].Unit.Spec.Priority

	// 调用Update方法
	err := q.Update(oldItem.Unit, newItem)
	assert.NoError(t, err)

	// 验证队列状态没有变化
	assert.Equal(t, oldQueueLen, len(q.queue))
	assert.Equal(t, oldPriority, q.queue[0].Unit.Spec.Priority)
	assert.Equal(t, "2", q.queue[0].Unit.ResourceVersion) // ResourceVersion应该更新
}

func TestMain(t *testing.T) {
	os.Setenv("TestENV", "true")
}
