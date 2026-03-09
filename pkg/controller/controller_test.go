package controller

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins"
	fr "github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func NewFrameworkForTesting(extraPlugins fr.Registry) (framework.Framework, map[string]framework.Plugin, versioned.Interface) {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset()
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	registry, plugins := plugins.NewFakeRegistry()
	for n, p := range extraPlugins {
		registry[n] = p
	}
	fwk, err := fr.NewFramework(
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
	return fwk, plugins, versionedclient
}

func TestWatchDequeuedQueueUnitDirectly(t *testing.T) {
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	fw, _, _ := NewFrameworkForTesting(nil)

	controller := &Controller{}
	controller.scheduler = &scheduler.Scheduler{}
	{
		controller.SetFramework(fw)

		queueUnitInformerFactory := externalversions.NewSharedInformerFactory(fw.QueueUnitClient(), 0)
		queueUnitLister := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
		queueUnitInformer := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
		queueInformer := queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()
		queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Lister()

		multiSchedulingQueue, _ := multischedulingqueue.NewMultiSchedulingQueue(fw,
			0, 0, queueUnitLister, false)
		controller.SetMultiSchedulingQueue(multiSchedulingQueue)

		controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
		go queueInformer.Run(wait.NeverStop)
		go queueUnitInformer.Run(wait.NeverStop)
		queueUnitInformerFactory.Start(wait.NeverStop)
		time.Sleep(time.Millisecond * 100)

		queue := &v1alpha1.Queue{}
		queue.Name = "Q1"
		queue.Spec.QueuePolicy = "Block"
		multiSchedulingQueue.Add(queue)

		unit1 := &v1alpha1.QueueUnit{}
		unit1.Annotations = make(map[string]string)
		unit1.Labels = make(map[string]string)
		unit1.Namespace = "test"
		unit1.Name = "job1"
		priority1 := int32(1000)
		unit1.Spec.Priority = &priority1
		unit1.Spec.Resource = make(v1.ResourceList)
		unit1.Spec.Resource["cpu"] = resource.MustParse("5")
		unit1.Spec.Resource["mem"] = resource.MustParse("5")
		unit1.Status.Phase = v1alpha1.Dequeued
		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("test").Create(
			context.Background(), unit1, metav1.CreateOptions{})
		time.Sleep(time.Millisecond * 100)

		q, _ := multiSchedulingQueue.GetQueueByName("Q1")
		assert.Equal(t, 1, len(multiSchedulingQueue.GetAllQueues()))
		paiq := q.GetQueueImplForTest().(*schedulingqueuev2.PriorityQueue)
		debugInfo := paiq.GetUserQuotaDebugInfo()
		debugData, _ := json.MarshalIndent(debugInfo, "", "\t")
		t.Logf("%v", string(debugData))

		unit2 := &v1alpha1.QueueUnit{}
		unit2.Annotations = make(map[string]string)
		unit2.Labels = make(map[string]string)
		unit2.Namespace = "test"
		unit2.Name = "job2"
		priority2 := int32(1000)
		unit2.Spec.Priority = &priority2
		unit2.Spec.Resource = make(v1.ResourceList)
		unit2.Spec.Resource["cpu"] = resource.MustParse("5")
		unit2.Spec.Resource["mem"] = resource.MustParse("5")
		unit2.Status.Phase = v1alpha1.Dequeued
		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("test").Create(
			context.Background(), unit2, metav1.CreateOptions{})
		time.Sleep(time.Millisecond * 100)

		q, _ = multiSchedulingQueue.GetQueueByName("Q1")
		paiq = q.GetQueueImplForTest().(*schedulingqueuev2.PriorityQueue)
		debugInfo = paiq.GetUserQuotaDebugInfo()
		debugData, _ = json.MarshalIndent(debugInfo, "", "\t")
		t.Logf("%v", string(debugData))

		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("test").Delete(
			context.Background(), unit2.Name, metav1.DeleteOptions{})
		time.Sleep(time.Millisecond * 100)

		q, _ = multiSchedulingQueue.GetQueueByName("Q1")
		paiq = q.GetQueueImplForTest().(*schedulingqueuev2.PriorityQueue)
		debugInfo = paiq.GetUserQuotaDebugInfo()
		debugData, _ = json.MarshalIndent(debugInfo, "", "\t")
		t.Logf("%v", string(debugData))
	}
}

func TestGetQueueUnitsByQueue(t *testing.T) {
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	fw, _, versionedclient := NewFrameworkForTesting(fr.Registry{
		"mockedQueueUnitsProvider": func(configuration runtime.Object, handle framework.Handle) (framework.Plugin, error) {
			return &mockedQueueUnitsProvider{}, nil
		},
	})

	controller := &Controller{}
	controller.scheduler = &scheduler.Scheduler{}
	controller.SetFramework(fw)

	queueUnitInformerFactory := externalversions.NewSharedInformerFactory(fw.QueueUnitClient(), 0)
	queueUnitLister := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
	queueUnitInformer := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
	queueInformer := queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()

	multiSchedulingQueue, _ := multischedulingqueue.NewMultiSchedulingQueue(fw,
		0, 0, queueUnitLister, false)
	controller.SetMultiSchedulingQueue(multiSchedulingQueue)
	controller.queueUnitLister = queueUnitLister

	controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
	go queueInformer.Run(wait.NeverStop)
	go queueUnitInformer.Run(wait.NeverStop)
	queueUnitInformerFactory.Start(wait.NeverStop)
	time.Sleep(time.Millisecond * 100)

	// Create a queue
	queue := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Block",
		},
	}
	multiSchedulingQueue.Add(queue)

	// Create queue units
	unit1 := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job1",
		},
		Spec: v1alpha1.QueueUnitSpec{
			Resource: v1.ResourceList{
				"cpu": resource.MustParse("2"),
				"mem": resource.MustParse("2Gi"),
			},
			Request: v1.ResourceList{
				"cpu": resource.MustParse("2"),
				"mem": resource.MustParse("2Gi"),
			},
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Enqueued,
		},
	}

	unit2 := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job2",
		},
		Spec: v1alpha1.QueueUnitSpec{
			Resource: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
			Request: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Enqueued,
			PodState: v1alpha1.PodState{
				Running: 0,
				Pending: 2,
			},
		},
	}

	unit3 := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job3",
		},
		Spec: v1alpha1.QueueUnitSpec{
			Resource: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
			Request: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Dequeued,
			PodState: v1alpha1.PodState{
				Running: 0,
				Pending: 2,
			},
		},
	}

	// Add units to queue
	versionedclient.SchedulingV1alpha1().QueueUnits("test").Create(context.Background(), unit1, metav1.CreateOptions{})
	versionedclient.SchedulingV1alpha1().QueueUnits("test").Create(context.Background(), unit2, metav1.CreateOptions{})
	versionedclient.SchedulingV1alpha1().QueueUnits("test").Create(context.Background(), unit3, metav1.CreateOptions{})
	time.Sleep(time.Millisecond * 100)

	// Manually add units to queue
	q, _ := multiSchedulingQueue.GetQueueByName("test-queue")
	q.AddQueueUnitInfo(&framework.QueueUnitInfo{Unit: unit1})
	q.AddQueueUnitInfo(&framework.QueueUnitInfo{Unit: unit2})

	// Test GetQueueUnitsByQueue
	units, err := controller.GetQueueUnitsByQueue("test-queue")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(units))

	// Verify unit details
	for _, unit := range units {
		assert.NotEqual(t, "job3", unit.Name)
		if unit.Name == "job1" {
			assert.Equal(t, "test", unit.Namespace)
			assert.Equal(t, "Enqueued", unit.Phase)
		} else if unit.Name == "job2" {
			assert.Equal(t, "test", unit.Namespace)
			assert.Equal(t, "Enqueued", unit.Phase)
		}
	}

	// Test non-existent queue
	_, err = controller.GetQueueUnitsByQueue("non-existent")
	assert.Error(t, err)
}

type mockedQueueUnitsProvider struct{}

func (m *mockedQueueUnitsProvider) Name() string { return "mockedQueueUnitsProvider" }

// GetQueueUnitQuotaName returns the quotaNames for the given unit.
func (m *mockedQueueUnitsProvider) GetQueueUnitQuotaName(*v1alpha1.QueueUnit) ([]string, error) {
	return []string{"quota1"}, nil
}

func TestGetQueueUnitsByQuota(t *testing.T) {
	// Create test objects
	fw, _, versionedclient := NewFrameworkForTesting(fr.Registry{
		"mockedQueueUnitsProvider": func(configuration runtime.Object, handle framework.Handle) (framework.Plugin, error) {
			return &mockedQueueUnitsProvider{}, nil
		},
	})
	// Use the framework's internal informer factory so the framework lister sees the same data
	queueUnitLister := fw.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister()
	controller := &Controller{}
	controller.SetFramework(fw)
	mq, _ := multischedulingqueue.NewMultiSchedulingQueue(fw,
		1, 10, queueUnitLister, false)
	controller.multiSchedulingQueue = mq
	fw.QueueInformerFactory().Start(nil)
	fw.QueueInformerFactory().WaitForCacheSync(nil)

	// Create queue units
	unit1 := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job1",
		},
		Spec: v1alpha1.QueueUnitSpec{
			Resource: v1.ResourceList{
				"cpu": resource.MustParse("2"),
				"mem": resource.MustParse("2Gi"),
			},
			Request: v1.ResourceList{
				"cpu": resource.MustParse("2"),
				"mem": resource.MustParse("2Gi"),
			},
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Enqueued,
		},
	}
	unit2 := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job2",
		},
		Spec: v1alpha1.QueueUnitSpec{
			Resource: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
			Request: v1.ResourceList{
				"cpu": resource.MustParse("1"),
				"mem": resource.MustParse("1Gi"),
			},
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase: v1alpha1.Enqueued,
			PodState: v1alpha1.PodState{
				Running: 0,
				Pending: 2,
			},
		},
	}
	// Add units to queue
	versionedclient.SchedulingV1alpha1().QueueUnits("test").Create(context.Background(), unit1, metav1.CreateOptions{})
	versionedclient.SchedulingV1alpha1().QueueUnits("test").Create(context.Background(), unit2, metav1.CreateOptions{})

	time.Sleep(time.Millisecond * 20)

	controller.queueUnitLister = queueUnitLister

	// Test with nil result (error case)
	quotaName := "quota1"
	units := controller.GetQueueUnitsByQuota(quotaName, &apiv1alpha1.QueueUnitOptions{
		Phase: string(v1alpha1.Enqueued),
	})
	assert.Equal(t, 2, len(units))
	sort.Slice(units, func(i, j int) bool {
		return units[i].Name < units[j].Name
	})
	assert.Equal(t, "job1", units[0].Name)
	assert.Equal(t, "job2", units[1].Name)
}
