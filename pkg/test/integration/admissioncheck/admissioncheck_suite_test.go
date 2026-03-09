package admissioncheck_test

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"

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

func TestAdmissioncheck(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Admissioncheck Suite")
}

var _ = Describe("AdmissionCheck", ginkgo.Ordered, func() {
	flag.Set("v", "3")
	flag.Parse()
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

	BeforeEach(func() {
		framework.ResetQueueUnitCache()
	})

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
		controller.SetQuController(quCtrl)
		controller.SetMultiSchedulingQueue(multiQueue)
		controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
		queueUnitInformerFactory.Start(wait.NeverStop)
		go controller.Start(ctx)
	})

	AfterAll(func() {
		cancel()
	})

	Describe("quota is enough, queue unit reserved and admission ready", ginkgo.Ordered, func() {
		var (
			err          error
			root         *queueunits.ElasticQuotaTreeWrapper
			defaultQueue *v1alpha1.Queue
			qus          []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			root = queueunits.ElasticQuotaTree(corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("100")})
			root.Child("quota1", []string{"default"}, corev1.ResourceList{"cpu": resource.MustParse("0")}, corev1.ResourceList{"cpu": resource.MustParse("100")})
			eqcli.SchedulingV1beta1().ElasticQuotaTrees("kube-system").Create(context.Background(), root.Obj(), metav1.CreateOptions{})
		})

		It("update queue should not error", func() {
			Eventually(func() error {
				queues, err := queueInformer.GetIndexer().ByIndex("quotaFullName", "root/quota1")
				if err != nil {
					return err
				}
				if len(queues) == 0 {
					return fmt.Errorf("Not Found")
				}
				defaultQueue = queues[0].(*v1alpha1.Queue)
				return nil
			}).Should(BeNil())
			defaultQueue = defaultQueue.DeepCopy()
			defaultQueue.Spec.AdmissionChecks = []v1alpha1.AdmissionCheckWithSelector{
				{
					Name: "ad1",
				},
				{
					Name: "ad2",
				},
			}
			defaultQueue, err = cli.SchedulingV1alpha1().Queues("koord-queue").Update(context.Background(), defaultQueue, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("create queue unit should not error", func() {
			qus = []*v1alpha1.QueueUnit{
				queueunits.MakeQueueUnit("qu1", "default").Resources(map[string]int64{"cpu": 1}).QueueUnit(),
				queueunits.MakeQueueUnit("qu2", "default").Resources(map[string]int64{"cpu": 1}).QueueUnit(),
				queueunits.MakeQueueUnit("qu3", "default").Resources(map[string]int64{"cpu": 1}).QueueUnit(),
				queueunits.MakeQueueUnit("qu4", "default").Resources(map[string]int64{"cpu": 1}).QueueUnit(),

				queueunits.MakeQueueUnit("qu5", "default").Resources(map[string]int64{"cpu": 1}).PodSetSimple(map[string]int64{"cpu": 1}, 1).QueueUnit(),
				queueunits.MakeQueueUnit("qu6", "default").Resources(map[string]int64{"cpu": 1}).PodSetSimple(map[string]int64{"cpu": 1}, 1).QueueUnit(),
				queueunits.MakeQueueUnit("qu7", "default").Resources(map[string]int64{"cpu": 1}).PodSetSimple(map[string]int64{"cpu": 1}, 1).QueueUnit(),
				queueunits.MakeQueueUnit("qu8", "default").Resources(map[string]int64{"cpu": 1}).PodSetSimple(map[string]int64{"cpu": 1}, 1).QueueUnit(),
			}
			for _, qu := range qus {
				_, err = cli.SchedulingV1alpha1().QueueUnits("default").Create(context.Background(), qu, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() bool {
				for _, qu := range qus {
					_, err = cli.SchedulingV1alpha1().QueueUnits("default").Get(context.TODO(), qu.Name, metav1.GetOptions{})
					if err != nil {
						return false
					}
				}
				return true
			}).Should(Equal(true))
		})

		var reservedqus []*v1alpha1.QueueUnit
		It("qu should be in Reserved phase", func() {
			Eventually(func() error {
				for _, qu := range qus {
					qu, err = cli.SchedulingV1alpha1().QueueUnits("default").Get(context.TODO(), qu.Name, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if qu.Status.Phase != v1alpha1.Reserved {
						klog.Infof("%v is %v", qu.Name, qu.Status.Phase)
						return fmt.Errorf("qu %v phase is %v", qu.Name, qu.Status.Phase)
					}
				}
				return nil
			}, time.Second*1000, time.Second).Should(BeNil())
			for _, qu := range qus {
				qu, err = queueUnitLister.QueueUnits("default").Get(qu.Name)
				Expect(err).NotTo(HaveOccurred())
				reservedqus = append(reservedqus, qu.DeepCopy())
			}
		})

		It("update admission check should not failed", func() {
			qu1 := reservedqus[0]
			qu1.Status.AdmissionChecks[0].State = v1beta1.CheckStateReady
			qu1.Status.AdmissionChecks[1].State = v1beta1.CheckStateReady
			_, err = cli.SchedulingV1alpha1().QueueUnits("default").UpdateStatus(ctx, qu1, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
			qu2 := reservedqus[1]
			qu2.Status.AdmissionChecks[1].State = v1beta1.CheckStateRejected
			_, err = cli.SchedulingV1alpha1().QueueUnits("default").UpdateStatus(ctx, qu2, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
			qu3 := reservedqus[2]
			qu3.Status.AdmissionChecks[1].State = v1beta1.CheckStateRetry
			_, err = cli.SchedulingV1alpha1().QueueUnits("default").UpdateStatus(ctx, qu3, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
			qu4 := reservedqus[3]
			qu4.Status.AdmissionChecks[1].State = v1beta1.CheckStateReady
			_, err = cli.SchedulingV1alpha1().QueueUnits("default").UpdateStatus(ctx, qu4, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("all queue units should be handled", func() {
			Eventually(func() v1alpha1.QueueUnitPhase {
				qu, _ := cli.SchedulingV1alpha1().QueueUnits("default").Get(ctx, "qu1", metav1.GetOptions{})
				if qu == nil {
					return ""
				}
				return qu.Status.Phase
			}, time.Second*2).Should(Equal(v1alpha1.Dequeued))
			Eventually(func() v1alpha1.QueueUnitPhase {
				qu, _ := cli.SchedulingV1alpha1().QueueUnits("default").Get(ctx, "qu2", metav1.GetOptions{})
				if qu == nil {
					return ""
				}
				return qu.Status.Phase
			}, time.Second*2).Should(Equal(v1alpha1.Backoff))
			Eventually(func() v1alpha1.QueueUnitPhase {
				qu, _ := cli.SchedulingV1alpha1().QueueUnits("default").Get(ctx, "qu3", metav1.GetOptions{})
				if qu == nil {
					return ""
				}
				return qu.Status.Phase
			}, time.Second*2).Should(Equal(v1alpha1.Backoff))
			Eventually(func() bool {
				qu, _ := cli.SchedulingV1alpha1().QueueUnits("default").Get(ctx, "qu4", metav1.GetOptions{})
				if qu == nil {
					return false
				}
				return qu.Status.Phase == v1alpha1.Dequeued || qu.Status.Phase == v1alpha1.Backoff
			}, time.Second*2).ShouldNot(Equal(true))
		})
	})
})
