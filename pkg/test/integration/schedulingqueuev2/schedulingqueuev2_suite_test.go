package schedulingqueuev2

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/cmd/app/options"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	listerv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	ctrl "github.com/koordinator-sh/koord-queue/pkg/controller"
	"github.com/koordinator-sh/koord-queue/pkg/controllers"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	eqversioned "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned"
	elasticquotatree "github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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

func TestSchedulingQueueV2(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SchedulingQueueV2 Suite")
}

var _ = Describe("SchedulingQueueV2", Ordered, func() {
	os.Setenv("QueueGroupPlugin", "elasticquota")
	os.Setenv("TestENV", "true")
	options.SetDefaultPreemptibleForTest(true)

	var (
		ctx    context.Context
		cancel context.CancelFunc

		fw   framework.Framework
		plgs map[string]framework.Plugin

		kubeCli    *fake.Clientset
		cli        versioned.Interface
		eqcli      eqversioned.Interface
		controller *ctrl.Controller

		queueUnitInformerFactory externalversions.SharedInformerFactory
		queueUnitLister          listerv1alpha1.QueueUnitLister
		// queueLister              listerv1alpha1.QueueLister
		queueInformer     cache.SharedIndexInformer
		queueUnitInformer cache.SharedIndexInformer
	)

	BeforeAll(func() {
		ctx, cancel = context.WithCancel(context.Background())
		fw, plgs, cli = testutils.NewFrameworkForTesting()
		eqcli = plgs["ElasticQuota"].(*elasticquotatree.ElasticQuota).GetClient()
		kubeCli = fake.NewSimpleClientset()

		// Create event broadcaster
		eventBroadcaster := record.NewBroadcaster()
		eventBroadcaster.StartLogging(klog.Infof)
		eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeCli.CoreV1().Events("")})

		schemeModified := scheme.Scheme
		v1alpha1.AddToScheme(schemeModified)
		recorder := eventBroadcaster.NewRecorder(schemeModified, corev1.EventSource{Component: utils.ControllerAgentName})

		queueUnitInformerFactory = fw.QueueInformerFactory()
		queueUnitLister = queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
		// queueLister = queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Lister()
		queueUnitInformer = queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
		queueInformer = queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()
		queueInformer.AddIndexers(cache.Indexers{"quotaFullName": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return nil, nil
			}
			return []string{qu.Annotations["koord-queue/quota-fullname"]}, nil
		}})

		var (
			multiQueue queue.MultiSchedulingQueue
			sched      *scheduler.Scheduler
		)

		multiQueue, _ = multischedulingqueue.NewMultiSchedulingQueue(fw, 1, 10,
			queueUnitLister, false)
		sched, _ = scheduler.NewScheduler(multiQueue, fw, cli, recorder, false, false, false, 10, "")
		quCtrl := controllers.NewQueueUnitController(2, false, cli, queueUnitInformer, queueUnitLister)

		controller = &ctrl.Controller{}
		controller.SetScheduler(sched)
		controller.SetFramework(fw)
		controller.SetMultiSchedulingQueue(multiQueue)
		controller.SetQuController(quCtrl)
		controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
		queueUnitInformerFactory.Start(wait.NeverStop)
		go controller.Start(ctx)

		root := queueunits.ElasticQuotaTree(corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("2")})
		root.Child("child-1", []string{"default"}, corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("2")})
		eqcli.SchedulingV1beta1().ElasticQuotaTrees("kube-system").Create(context.Background(), root.Obj(), metav1.CreateOptions{})

		kubeCli.CoreV1().Namespaces().Update(context.Background(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default",
				Annotations: map[string]string{"koord-queue/available-queue": "{\"test-queue-block\":[\"*\"], \"test-queue-priority\":[\"*\"]}"}}},
			metav1.UpdateOptions{})
		fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Create(context.Background(), &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "test-queue-block", Namespace: "koord-queue",
				Annotations: map[string]string{"koord-queue/wait-for-pods-running": "true", "koord-queue/available-quota": "child-1"}},
			Spec: v1alpha1.QueueSpec{
				QueuePolicy: "Block",
			},
		}, metav1.CreateOptions{})
		fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Create(context.Background(), &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "test-queue-priority", Namespace: "koord-queue",
				Annotations: map[string]string{"koord-queue/wait-for-pods-running": "true", "koord-queue/available-quota": "child-1"}},
			Spec: v1alpha1.QueueSpec{
				QueuePolicy: "Priority",
			},
		}, metav1.CreateOptions{})
	})

	AfterAll(func() {
		cancel()
	})

	Describe("schedulingqueuev2", Ordered, func() {
		var (
			qus []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			qus = []*v1alpha1.QueueUnit{
				queueunits.MakeQueueUnit("dequeued-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).Priority(2).QueueUnit(),

				queueunits.MakeQueueUnit("high-block", "default").Resources(map[string]int64{"cpu": 2}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).Priority(3).QueueUnit(),
				queueunits.MakeQueueUnit("high-block1", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).Priority(3).QueueUnit(),
				queueunits.MakeQueueUnit("mid-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).Priority(2).QueueUnit(),
				queueunits.MakeQueueUnit("low-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).Priority(1).QueueUnit(),

				queueunits.MakeQueueUnit("high-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-2"}).Priority(3).QueueUnit(),
				queueunits.MakeQueueUnit("mid-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-2"}).Priority(2).QueueUnit(),
				queueunits.MakeQueueUnit("low-block", "default").Resources(map[string]int64{"cpu": 1}).Annotations(map[string]string{"koord-queue/queue-name": "test-queue-block"}).Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-2"}).Priority(1).QueueUnit(),
			}

			time.Sleep(time.Second)
		})

		AfterEach(func() {
			for _, qu := range qus {
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Delete(context.TODO(), qu.Name, metav1.DeleteOptions{})
			}
		})

		It("dequeued mid job should be preempted by higher priority job", func() {
			Skip("will be enabled after preemption is supported in feat/support-preemption")
			By("waiting queue ready")
			Eventually(func() error {
				Eventually(func() error {
					_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Get(context.TODO(), "test-queue-block", metav1.GetOptions{})
					return err
				}, time.Second*10, time.Millisecond*100).Should(BeNil())
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Get(context.TODO(), "test-queue-priority", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())
			By("create the padding queue unit")
			padding := qus[0]
			Expect(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Create(context.TODO(), padding, metav1.CreateOptions{})
				return err
			}()).Should(BeNil())

			By("waiting the padding queue unit to be scheduled")
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Get(context.TODO(), padding.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*10, time.Millisecond*100).Should(Equal(v1alpha1.Dequeued))

			By("create the high-block queue unit")
			high := qus[1]
			Expect(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(high.Namespace).Create(context.TODO(), high, metav1.CreateOptions{})
				return err
			}()).Should(BeNil())
			By("waiting the high-prio queueunit preempt the other")
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Get(context.TODO(), padding.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*10, time.Millisecond*100).Should(Equal(v1alpha1.QueueUnitPhase("Preempted")))
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(high.Namespace).Get(context.TODO(), high.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*10, time.Millisecond*100).Should(Equal(v1alpha1.Dequeued))
		})

		It("dequeued mid job should be preempted by higher priority job in reserve", func() {
			Skip("will be enabled after preemption is supported in feat/support-preemption")
			By("waiting queue ready")
			Eventually(func() error {
				Eventually(func() error {
					_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Get(context.TODO(), "test-queue-block", metav1.GetOptions{})
					return err
				}, time.Second*10, time.Millisecond*100).Should(BeNil())
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("koord-queue").Get(context.TODO(), "test-queue-priority", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())
			By("create the padding queue unit")
			padding := qus[0]
			Expect(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Create(context.TODO(), padding, metav1.CreateOptions{})
				return err
			}()).Should(BeNil())

			By("waiting the padding queue unit to be scheduled")
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Get(context.TODO(), padding.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*10, time.Millisecond*100).Should(Equal(v1alpha1.Dequeued))

			By("create the high-block queue unit")
			high := qus[2]
			Expect(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(high.Namespace).Create(context.TODO(), high, metav1.CreateOptions{})
				return err
			}()).Should(BeNil())
			By("waiting the high-prio queueunit preempt the other")
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(padding.Namespace).Get(context.TODO(), padding.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*1000, time.Millisecond*100).Should(Equal(v1alpha1.QueueUnitPhase("Preempted")))
			Eventually(func() v1alpha1.QueueUnitPhase {
				q, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(high.Namespace).Get(context.TODO(), high.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return q.Status.Phase
			}, time.Second*10, time.Millisecond*100).Should(Equal(v1alpha1.Dequeued))
		})
	})
})
