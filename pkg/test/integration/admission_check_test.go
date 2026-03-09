package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/cmd/app/options"
	queuev1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/controller"
	"github.com/koordinator-sh/koord-queue/pkg/controllers"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	elasticquotatree "github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"
	"github.com/koordinator-sh/koord-queue/pkg/utils"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

func TestAdmissionCheck(t *testing.T) {
	os.Setenv("QueueGroupPlugin", "elasticquota")
	os.Setenv("TestENV", "true")
	framework.ResetQueueUnitCache()
	options.SetDefaultPreemptibleForTest(true)

	fw, plgs, cli := testutils.NewFrameworkForTesting()
	eqcli := plgs["ElasticQuota"].(*elasticquotatree.ElasticQuota).GetClient()
	kubeCli := fake.NewSimpleClientset()

	// Create event broadcaster
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeCli.CoreV1().Events("")})

	schemeModified := scheme.Scheme
	v1alpha1.AddToScheme(schemeModified)
	recorder := eventBroadcaster.NewRecorder(schemeModified, corev1.EventSource{Component: utils.ControllerAgentName})

	queueUnitInformerFactory := fw.QueueInformerFactory()
	queueUnitLister := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
	// queueLister := queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Lister()
	queueUnitInformer := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
	queueInformer := queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()
	queueInformer.AddIndexers(cache.Indexers{"quotaFullName": func(obj interface{}) ([]string, error) {
		qu, ok := obj.(*queuev1alpha1.Queue)
		if !ok {
			return nil, nil
		}
		return []string{qu.Annotations["koord-queue/quota-fullname"]}, nil
	}})
	queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Lister()

	multiQueue, err := multischedulingqueue.NewMultiSchedulingQueue(fw, 1, 10,
		queueUnitLister, false)
	if err != nil {
		t.Error(err)
	}
	sched, err := scheduler.NewScheduler(multiQueue, fw, cli, recorder, false, false, false, 10, "")
	if err != nil {
		t.Error(err)
	}
	quCtrl := controllers.NewQueueUnitController(2, false, cli, queueUnitInformer, queueUnitLister)

	controller := &controller.Controller{}
	controller.SetScheduler(sched)
	controller.SetFramework(fw)
	controller.SetQuController(quCtrl)
	controller.SetMultiSchedulingQueue(multiQueue)
	controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
	queueUnitInformerFactory.Start(wait.NeverStop)
	queueUnitInformerFactory.WaitForCacheSync(wait.NeverStop)
	go controller.Start(context.Background())

	// cli.SchedulingV1alpha1().QueueUnits("default").Create()
	root := queueunits.ElasticQuotaTree(corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("100")})
	root.Child("quota1", []string{"default"}, corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("100")})
	eqcli.SchedulingV1beta1().ElasticQuotaTrees("kube-system").Create(context.Background(), root.Obj(), metav1.CreateOptions{})

	var defaultQueue *queuev1alpha1.Queue
	err = wait.PollUntilContextTimeout(context.Background(), time.Second, time.Second*10, false,
		func(ctx context.Context) (done bool, err error) {
			queues, err := queueInformer.GetIndexer().ByIndex("quotaFullName", "root/quota1")
			if err != nil {
				return false, err
			}
			if len(queues) == 0 {
				return false, nil
			}
			defaultQueue = queues[0].(*queuev1alpha1.Queue)
			return true, nil
		})
	if err != nil {
		t.Error(err)
	}

	defaultQueue = defaultQueue.DeepCopy()
	defaultQueue.Spec.AdmissionChecks = []queuev1alpha1.AdmissionCheckWithSelector{
		{
			Name: "ad1",
		},
		{
			Name: "ad2",
		},
	}
	defaultQueue, err = cli.SchedulingV1alpha1().Queues("koord-queue").Update(context.Background(), defaultQueue, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err)
	}

	qu := queueunits.MakeQueueUnit("qu1", "default").Resources(map[string]int64{"cpu": 1}).QueueUnit()
	_, err = cli.SchedulingV1alpha1().QueueUnits("default").Create(context.Background(), qu, metav1.CreateOptions{})
	if err != nil {
		t.Error(err)
	}

	err = wait.PollUntilContextTimeout(context.Background(), time.Second, time.Second*10, false,
		func(ctx context.Context) (done bool, err error) {
			qu, err = queueUnitLister.QueueUnits("default").Get("qu1")
			if err != nil {
				return false, err
			}

			if qu.Status.Phase != queuev1alpha1.Reserved {
				t.Logf("queue unit phase is %v, try after 10s.", qu.Status.Phase)
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		t.Error(err)
	}
}
