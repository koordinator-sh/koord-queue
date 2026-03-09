package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	queueunitfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	queueunitfakeex "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	queue2 "github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

func TestDequeue(t *testing.T) {
	kubecli := kubefake.NewSimpleClientset()
	informersFactory := informers.NewSharedInformerFactory(kubecli, 0)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{})

	schemeModified := scheme.Scheme
	recorder := eventBroadcaster.NewRecorder(schemeModified, v1.EventSource{Component: utils.ControllerAgentName})

	queueClient := queueunitfake.NewSimpleClientset()
	queueInformerFactory := queueunitfakeex.NewSharedInformerFactory(queueClient, 0)

	queueInformerFactory.Start(wait.NeverStop)
	time.Sleep(time.Millisecond * 100)

	r, _ := plugins.NewFakeRegistry()
	fw, err := runtime.NewFramework(r, nil, "", informersFactory, queueInformerFactory, recorder, queueClient, 1.0, nil)
	if err != nil {
		t.Fatalf("NewFramework error: %v", err)
	}
	scheduler := &Scheduler{
		fw: fw,
	}
	scheduler.QueueClient = queueClient
	scheduler.recorder = recorder

	unit1 := &v1alpha1.QueueUnit{}
	unit1.Annotations = make(map[string]string)
	// unit1.Annotations[api.DequeueFailReasonAnnotation] = "aa"
	unit1.Namespace = "test"
	unit1.Name = "job1"
	priority1 := int32(1000)
	unit1.Spec.Priority = &priority1
	unit1.Spec.Resource = make(v1.ResourceList)
	unit1.Spec.Resource["cpu"] = resource.MustParse("5")
	unit1.Spec.Resource["mem"] = resource.MustParse("5")

	queueClient.SchedulingV1alpha1().QueueUnits("test").Create(
		context.Background(), unit1, metav1.CreateOptions{})
	time.Sleep(time.Microsecond * 100)

	q1 := queue.NewQueueForTesting("q1", make([]string, 0))
	scheduler.Dequeue(context.Background(), klog.Background(), unit1, utils.GetAdmissionMap(unit1), q1)
	time.Sleep(time.Microsecond * 100)

	newUnit1, _ := queueClient.SchedulingV1alpha1().QueueUnits("test").Get(
		context.Background(), "job1", metav1.GetOptions{})
	assert.Equal(t, v1alpha1.Dequeued, newUnit1.Status.Phase)
}

func TestDequeueWithAdmissionCheck(t *testing.T) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{})

	schemeModified := scheme.Scheme
	recorder := eventBroadcaster.NewRecorder(schemeModified, v1.EventSource{Component: utils.ControllerAgentName})

	queueClient := queueunitfake.NewSimpleClientset()
	queueInformerFactory := queueunitfakeex.NewSharedInformerFactory(queueClient, 0)

	queueInformerFactory.Start(wait.NeverStop)
	time.Sleep(time.Millisecond * 100)

	kubecli := kubefake.NewSimpleClientset()
	informersFactory := informers.NewSharedInformerFactory(kubecli, 0)
	r, _ := plugins.NewFakeRegistry()
	fw, err := runtime.NewFramework(r, nil, "", informersFactory, queueInformerFactory, recorder, queueClient, 1.0, nil)
	if err != nil {
		t.Fatalf("NewFramework error: %v", err)
	}
	scheduler := &Scheduler{
		fw: fw,
	}
	scheduler.QueueClient = queueClient
	scheduler.recorder = recorder

	unit1 := &v1alpha1.QueueUnit{}
	unit1.Annotations = make(map[string]string)
	// unit1.Annotations[api.DequeueFailReasonAnnotation] = "aa"
	unit1.Namespace = "test"
	unit1.Name = "job1"
	priority1 := int32(1000)
	unit1.Spec.Priority = &priority1
	unit1.Spec.Resource = make(v1.ResourceList)
	unit1.Spec.Resource["cpu"] = resource.MustParse("5")
	unit1.Spec.Resource["mem"] = resource.MustParse("5")

	queueClient.SchedulingV1alpha1().QueueUnits("test").Create(
		context.Background(), unit1, metav1.CreateOptions{})
	time.Sleep(time.Microsecond * 100)

	q1 := queue.NewQueueForTesting("q1", []string{"ad1", "ad2"})
	wantAdmissions := []v1beta1.AdmissionCheckState{
		v1beta1.AdmissionCheckState{
			Name:  "ad1",
			State: v1beta1.CheckStatePending,
		},
		v1beta1.AdmissionCheckState{
			Name:  "ad2",
			State: v1beta1.CheckStatePending,
		},
	}
	scheduler.Dequeue(context.Background(), klog.Background(), unit1, utils.GetAdmissionMap(unit1), q1)
	time.Sleep(time.Microsecond * 100)

	newUnit1, _ := queueClient.SchedulingV1alpha1().QueueUnits("test").Get(
		context.Background(), "job1", metav1.GetOptions{})
	if diff := cmp.Diff(newUnit1.Status.Phase, v1alpha1.Reserved); diff != "" {
		t.Error(diff)
	}
	if diff := cmp.Diff(newUnit1.Status.AdmissionChecks, wantAdmissions,
		cmpopts.IgnoreFields(v1beta1.AdmissionCheckState{}, "Message", "LastTransitionTime", "PodSetUpdates"),
		cmpopts.SortSlices(func(a, b v1beta1.AdmissionCheckState) bool {
			return a.Name > b.Name
		})); diff != "" {
		t.Error(diff)
	}
}

func TestErrorFunc(t *testing.T) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{})

	schemeModified := scheme.Scheme
	recorder := eventBroadcaster.NewRecorder(schemeModified, v1.EventSource{Component: utils.ControllerAgentName})

	queueClient := queueunitfake.NewSimpleClientset()
	queueInformerFactory := queueunitfakeex.NewSharedInformerFactory(queueClient, 0)
	queueUnitLister := queueInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()

	queueInformerFactory.Start(wait.NeverStop)
	time.Sleep(time.Millisecond * 100)

	qCr := &v1alpha1.Queue{}
	qCr.Name = "q1"
	qCr.Spec.QueuePolicy = "Priority"

	scheduler := &Scheduler{}
	scheduler.QueueClient = queueClient
	scheduler.recorder = recorder
	queue := &queue2.Queue{}
	queue.SetQueueImplForTest(schedulingqueuev2.NewPriorityQueue("q1", qCr, nil, queueClient, queueUnitLister, nil))

	unit1 := &v1alpha1.QueueUnit{}
	unit1.Annotations = make(map[string]string)
	unit1.Namespace = "test"
	unit1.Name = "job1"
	priority1 := int32(1000)
	unit1.Spec.Priority = &priority1
	unit1.Spec.Resource = make(v1.ResourceList)
	unit1.Spec.Resource["cpu"] = resource.MustParse("5")
	unit1.Spec.Resource["mem"] = resource.MustParse("5")

	queueClient.SchedulingV1alpha1().QueueUnits("test").Create(
		context.Background(), unit1, metav1.CreateOptions{})
	time.Sleep(time.Microsecond * 100)

	scheduler.ErrorFunc(context.Background(), framework.NewQueueUnitInfo(unit1), queue, "QuotaCheckFailed", 0, false)
	time.Sleep(time.Microsecond * 100)

	newUnit1, _ := queueClient.SchedulingV1alpha1().QueueUnits("test").Get(
		context.Background(), "job1", metav1.GetOptions{})
	assert.Equal(t, "QuotaCheckFailed", newUnit1.Status.Message)
}
