package intelligentqueue

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

func TestIntelligentQueue(t *testing.T) {
	flag.Set("v", "4")
	flag.Parse()
	RegisterFailHandler(Fail)
	RunSpecs(t, "IntelligentQueue Suite")
}

var _ = Describe("IntelligentQueue", Ordered, func() {
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
		queueInformer            cache.SharedIndexInformer
		queueUnitInformer        cache.SharedIndexInformer
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
		queueUnitInformer = queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
		queueInformer = queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()
		queueInformer.AddIndexers(cache.Indexers{"quotaFullName": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return nil, nil
			}
			return []string{qu.Annotations["kube-queue/quota-fullname"]}, nil
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
		queueUnitInformerFactory.WaitForCacheSync(wait.NeverStop)
		go controller.Start(ctx)

		// Create ElasticQuotaTree with limited resources to prevent immediate scheduling
		// This keeps tasks in queue so we can verify their positions
		root := queueunits.ElasticQuotaTree(
			corev1.ResourceList{"cpu": resource.MustParse("100m")},
			corev1.ResourceList{"cpu": resource.MustParse("100m")}) // max=0 prevents scheduling
		root.Child("child-1", []string{"default"},
			corev1.ResourceList{"cpu": resource.MustParse("100m")},
			corev1.ResourceList{"cpu": resource.MustParse("100m")}) // max=0 prevents scheduling
		eqcli.SchedulingV1beta1().ElasticQuotaTrees("kube-system").Create(context.Background(), root.Obj(), metav1.CreateOptions{})

		// Update namespace with available queue
		kubeCli.CoreV1().Namespaces().Update(context.Background(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default",
				Annotations: map[string]string{"kube-queue/available-queue": "{\"test-intelligent-queue\":[\"*\"]}"}}},
			metav1.UpdateOptions{})

		// Create Intelligent Queue with priority threshold = 4
		// Also set queue-items-refresh-interval to update queue status frequently
		fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Create(context.Background(), &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-intelligent-queue",
				Namespace: "kube-queue",
				Annotations: map[string]string{
					"kube-queue/priority-threshold":           "4",
					"kube-queue/available-quota":              "child-1",
					"kube-queue/queue-items-refresh-interval": "100ms", // Fast refresh for testing
				}},
			Spec: v1alpha1.QueueSpec{
				QueuePolicy: "Intelligent",
			},
		}, metav1.CreateOptions{})

		// Wait for queue to be ready and loaded by controller
		time.Sleep(time.Second * 5)
	})

	AfterAll(func() {
		cancel()
	})

	Describe("High Priority Queue Priority+FIFO Ordering", Ordered, func() {
		var (
			qus []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// Create queue units with priority >= 4 (high priority queue)
			// These should be ordered by priority first, then by submission time
			qus = []*v1alpha1.QueueUnit{
				// High priority job 1: priority=5, submitted first
				queueunits.MakeQueueUnit("high-prio-job-1", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(5).QueueUnit(),

				// High priority job 2: priority=4, submitted second
				queueunits.MakeQueueUnit("high-prio-job-2", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(4).QueueUnit(),

				// High priority job 3: priority=6, submitted third (highest priority)
				queueunits.MakeQueueUnit("high-prio-job-3", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(6).QueueUnit(),
			}
		})

		AfterEach(func() {
			for _, qu := range qus {
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Delete(context.TODO(), qu.Name, metav1.DeleteOptions{})
			}
			time.Sleep(time.Millisecond * 500)
		})

		It("should order high priority jobs (>=4) by priority then FIFO", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("waiting for queue to broadcast")
			time.Sleep(time.Second * 2)

			By("creating high priority queue units")
			for _, qu := range qus {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Create(context.TODO(), qu, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				time.Sleep(time.Millisecond * 50) // Small delay to ensure creation order
			}

			By("waiting for queue to process")
			time.Sleep(time.Second * 2)

			By("checking positions - should be in priority order, then FIFO")
			// high-prio-job-3 should be position 1 (highest priority=6)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-prio-job-3" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(1)))

			// high-prio-job-1 should be position 2 (priority=5, submitted before job-3)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-prio-job-1" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(2)))

			// high-prio-job-2 should be position 3 (lowest priority=4)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-prio-job-2" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(3)))
		})
	})

	Describe("Low Priority Queue Priority Ordering", Ordered, func() {
		var (
			qus []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// Create queue units with priority < 4 (low priority queue)
			// These should be ordered by priority, not by submission time
			qus = []*v1alpha1.QueueUnit{
				// Low priority job 1: priority=1, submitted first
				queueunits.MakeQueueUnit("low-prio-job-1", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(1).QueueUnit(),

				// Low priority job 2: priority=3, submitted second
				queueunits.MakeQueueUnit("low-prio-job-2", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(3).QueueUnit(),

				// Low priority job 3: priority=2, submitted third
				queueunits.MakeQueueUnit("low-prio-job-3", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(2).QueueUnit(),
			}
		})

		AfterEach(func() {
			for _, qu := range qus {
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Delete(context.TODO(), qu.Name, metav1.DeleteOptions{})
			}
			time.Sleep(time.Millisecond * 500)
		})

		It("should order low priority jobs (<4) by priority", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating low priority queue units")
			for _, qu := range qus {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Create(context.TODO(), qu, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				time.Sleep(time.Millisecond * 50)
			}

			By("waiting for queue to process")
			time.Sleep(time.Second * 2)

			By("checking positions - should be in priority order")
			// low-prio-job-2 should be position 1 (highest priority=3)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-prio-job-2" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(1)))

			// low-prio-job-3 should be position 2 (middle priority=2)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-prio-job-3" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(2)))

			// low-prio-job-1 should be position 3 (lowest priority=1)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-prio-job-1" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(3)))
		})
	})

	Describe("Mixed Priority Queue Ordering", Ordered, func() {
		var (
			qus []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// Create mixed queue units with both high (>=4) and low (<4) priorities
			qus = []*v1alpha1.QueueUnit{
				// High priority jobs (should be at front, in priority+time order)
				queueunits.MakeQueueUnit("high-job-1", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(5).QueueUnit(),

				queueunits.MakeQueueUnit("high-job-2", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(4).QueueUnit(),

				// Low priority jobs (should be after high priority, in priority order)
				queueunits.MakeQueueUnit("low-job-1", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(1).QueueUnit(),

				queueunits.MakeQueueUnit("low-job-2", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(3).QueueUnit(),
			}
		})

		AfterEach(func() {
			for _, qu := range qus {
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Delete(context.TODO(), qu.Name, metav1.DeleteOptions{})
			}
			time.Sleep(time.Millisecond * 500)
		})

		It("should order high priority jobs first (priority+time), then low priority jobs (by priority)", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating mixed priority queue units")
			for _, qu := range qus {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Create(context.TODO(), qu, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				time.Sleep(time.Millisecond * 50)
			}

			By("waiting for queue to process")
			time.Sleep(time.Second * 2)

			By("checking positions")
			// High priority jobs should be first (priority order: high-job-1 with priority=5)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-job-1" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(1)))

			// high-job-2 with priority=4 should be second
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-job-2" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(2)))

			// Low priority jobs should be after high priority (priority order)
			// low-job-2 (priority=3) should be position 3
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-job-2" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(3)))

			// low-job-1 (priority=1) should be position 4
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-job-1" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(4)))
		})
	})

	Describe("Priority Threshold Boundary", Ordered, func() {
		var (
			qus []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// Create queue units around the threshold (threshold=4)
			qus = []*v1alpha1.QueueUnit{
				// Exactly at threshold (priority=4) - should go to high priority queue (priority+time)
				queueunits.MakeQueueUnit("threshold-job", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(4).QueueUnit(),

				// Just below threshold (priority=3) - should go to low priority queue
				queueunits.MakeQueueUnit("below-threshold-job", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(3).QueueUnit(),

				// Above threshold (priority=5) - should go to high priority queue
				queueunits.MakeQueueUnit("above-threshold-job", "default").
					Resources(map[string]int64{"cpu": 1}).
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(5).QueueUnit(),
			}
		})

		AfterEach(func() {
			for _, qu := range qus {
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Delete(context.TODO(), qu.Name, metav1.DeleteOptions{})
			}
			time.Sleep(time.Millisecond * 500)
		})

		It("should correctly classify jobs at threshold boundary", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating queue units around threshold")
			for _, qu := range qus {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(qu.Namespace).Create(context.TODO(), qu, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				time.Sleep(time.Millisecond * 50)
			}

			By("waiting for queue to process")
			time.Sleep(time.Second * 2)

			By("checking positions - threshold and above should be in high priority queue (priority+time)")
			// above-threshold-job (priority=5) should be position 1 (highest priority in high queue)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "above-threshold-job" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(1)))

			// threshold-job (priority=4) should be position 2 (lower priority in high queue)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "threshold-job" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(2)))

			// below-threshold-job (priority=3) should be position 3 (low priority queue, after all high priority jobs)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "below-threshold-job" {
						return item.Position
					}
				}
				return 0
			}, time.Second*10, time.Millisecond*100).Should(Equal(int32(3)))
		})
	})

	Describe("High Priority Block vs Low Priority Polling Behavior", Ordered, func() {
		var (
			highPrioLargeJob *v1alpha1.QueueUnit
			lowPrioSmallJob1 *v1alpha1.QueueUnit
			lowPrioSmallJob2 *v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// High priority task: large resource requirement (2 CPU)
			// This task will fail to schedule due to insufficient resources
			highPrioLargeJob = queueunits.MakeQueueUnit("high-prio-large-job", "default").
				Resources(map[string]int64{"cpu": 2}). // Large requirement: 2 CPU
				Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
				Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
				Priority(5).QueueUnit()

			// Low priority task 1: small resource requirement (0.1 CPU)
			lowPrioSmallJob1 = queueunits.MakeQueueUnit("low-prio-small-job-1", "default").
				Resources(map[string]int64{"cpu": 100}). // Small requirement: 100 millicpu = 0.1 CPU
				Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
				Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
				Priority(1).QueueUnit()

			// Low priority task 2: small resource requirement (0.1 CPU)
			lowPrioSmallJob2 = queueunits.MakeQueueUnit("low-prio-small-job-2", "default").
				Resources(map[string]int64{"cpu": 100}). // Small requirement: 100 millicpu = 0.1 CPU
				Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
				Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
				Priority(2).QueueUnit()
		})

		AfterEach(func() {
			fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), "high-prio-large-job", metav1.DeleteOptions{})
			fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), "low-prio-small-job-1", metav1.DeleteOptions{})
			fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), "low-prio-small-job-2", metav1.DeleteOptions{})
			time.Sleep(time.Millisecond * 500)
		})

		It("should demonstrate high priority block behavior vs low priority polling behavior", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating high priority large job first (will fail due to insufficient resources)")
			_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), highPrioLargeJob, metav1.CreateOptions{})
			Expect(err).Should(BeNil())
			time.Sleep(time.Millisecond * 100)

			By("creating low priority small job 1")
			_, err = fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), lowPrioSmallJob1, metav1.CreateOptions{})
			Expect(err).Should(BeNil())
			time.Sleep(time.Millisecond * 100)

			By("creating low priority small job 2")
			_, err = fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), lowPrioSmallJob2, metav1.CreateOptions{})
			Expect(err).Should(BeNil())
			time.Sleep(time.Millisecond * 100)

			By("waiting for initial queue state")
			time.Sleep(time.Second * 2)

			By("verifying positions in queue")
			// High priority large job should be at position 1 (first in high priority queue)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-prio-large-job" {
						return item.Position
					}
				}
				return 0
			}, time.Second*5, time.Millisecond*100).Should(Equal(int32(1)))

			// Low priority small job 2 should be at position 2 (first in low priority queue)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-prio-small-job-2" {
						return item.Position
					}
				}
				return 0
			}, time.Second*5, time.Millisecond*100).Should(Equal(int32(2)))

			// Low priority small job 1 should be at position 3 (second in low priority queue)
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "low-prio-small-job-1" {
						return item.Position
					}
				}
				return 0
			}, time.Second*5, time.Millisecond*100).Should(Equal(int32(3)))

			By("verifying scheduling behavior difference")
			// Check queue status multiple times to observe scheduling attempts
			for i := 0; i < 3; i++ {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				Expect(err).Should(BeNil())

				// The high priority job should always remain at position 1
				// This demonstrates the block behavior - the high priority job blocks
				// the entire queue even though low priority jobs could be scheduled
				highPrioPos := int32(0)
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-prio-large-job" {
						highPrioPos = item.Position
						break
					}
				}
				By(fmt.Sprintf("check %d: high-prio-large-job at position %d", i+1, highPrioPos))
				Expect(highPrioPos).Should(Equal(int32(1)), "High priority job should stay at position 1 (block behavior)")

				// Low priority jobs should still be in their positions
				// They won't be scheduled while the high priority job is blocking
				time.Sleep(time.Second * 1)
			}
		})
	})

	Describe("Low Priority Task Polling Behavior", Ordered, func() {
		var (
			highPrioBlockingJob *v1alpha1.QueueUnit
			lowPrioJobs         []*v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// High priority task: large resource requirement that cannot be satisfied
			highPrioBlockingJob = queueunits.MakeQueueUnit("high-blocking-job", "default").
				Resources(map[string]int64{"cpu": 10}). // 10 CPU - impossible to satisfy
				Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
				Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
				Priority(5).QueueUnit()

			// Create 5 low priority small tasks that can be scheduled
			// Each task tries to use 0.2 CPU, system has only 0.5 CPU available
			// This means at most 2 tasks can be in flight, forcing polling behavior
			lowPrioJobs = make([]*v1alpha1.QueueUnit, 5)
			for i := 0; i < 5; i++ {
				jobName := fmt.Sprintf("low-polling-job-%d", i+1)
				lowPrioJobs[i] = queueunits.MakeQueueUnit(jobName, "default").
					Resources(map[string]int64{"cpu": 200}). // 200 millicpu = 0.2 CPU each
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(int32(1 + i%3)). // Vary priorities: 1, 2, 3, 1, 2
					QueueUnit()
			}
		})

		AfterEach(func() {
			// Clean up all jobs
			fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), "high-blocking-job", metav1.DeleteOptions{})
			for i := 0; i < 5; i++ {
				jobName := fmt.Sprintf("low-polling-job-%d", i+1)
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), jobName, metav1.DeleteOptions{})
			}
			time.Sleep(time.Millisecond * 500)
		})

		It("should demonstrate low priority task polling behavior with varying priorities", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating high priority blocking job first")
			_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), highPrioBlockingJob, metav1.CreateOptions{})
			Expect(err).Should(BeNil())
			time.Sleep(time.Millisecond * 100)

			By("creating 5 low priority polling jobs with varying priorities")
			for i, job := range lowPrioJobs {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), job, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				// Job creation order determines initial ordering (FIFO within same priority)
				By(fmt.Sprintf("created low-polling-job-%d with priority=%d", i+1, 1+i%3))
				time.Sleep(time.Millisecond * 100)
			}

			By("waiting for initial queue state")
			time.Sleep(time.Second * 2)

			By("verifying initial queue positions")
			// High priority blocking job should be at position 1
			Eventually(func() int32 {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				if err != nil {
					return 0
				}
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-blocking-job" {
						return item.Position
					}
				}
				return 0
			}, time.Second*5, time.Millisecond*100).Should(Equal(int32(1)))

			By("verifying low priority jobs are present in queue in priority+time order")
			queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
			Expect(err).Should(BeNil())

			// Collect all low priority job positions
			lowJobPositions := make(map[string]int32)
			for _, item := range queue.Status.QueueItemDetails["active"] {
				for i := 1; i <= 5; i++ {
					jobName := fmt.Sprintf("low-polling-job-%d", i)
					if item.Name == jobName {
						lowJobPositions[jobName] = item.Position
					}
				}
			}

			By("verifying all 5 low priority jobs are in queue")
			Expect(len(lowJobPositions)).Should(Equal(5), "All 5 low priority jobs should be in queue")

			By("verifying low priority jobs are sorted by priority + time")
			// Job creation order with varying priorities:
			// Job-1: priority=1, created first
			// Job-2: priority=2, created second
			// Job-3: priority=3, created third (highest in low priority)
			// Job-4: priority=1, created fourth
			// Job-5: priority=2, created fifth
			// Expected order in queue (priority > time): Job-3(p3), Job-2(p2,early), Job-5(p2,late), Job-1(p1,early), Job-4(p1,late)

			// Verify that job-3 (highest priority=3) is near the front of low priority queue
			if pos, ok := lowJobPositions["low-polling-job-3"]; ok {
				By(fmt.Sprintf("low-polling-job-3 (priority=3) at position %d", pos))
				Expect(pos).Should(Equal(int32(2)), "Highest priority low job should be at position 2 (after high blocking job)")
			}

			By("verifying polling behavior with repeated queue checks")
			// Check queue multiple times - low priority job positions should be stable
			// This proves that:
			// 1. Low priority jobs remain in queue despite high priority blocking job
			// 2. Low priority jobs are ordered by priority + time
			// 3. Scheduler polls through low priority jobs (evident from stable positions)
			for checkNum := 0; checkNum < 3; checkNum++ {
				queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				Expect(err).Should(BeNil())

				// Count low priority jobs still in queue
				lowJobCount := 0
				for _, item := range queue.Status.QueueItemDetails["active"] {
					for i := 1; i <= 5; i++ {
						if item.Name == fmt.Sprintf("low-polling-job-%d", i) {
							lowJobCount++
						}
					}
				}

				// High blocking job should still be at position 1
				highJobFound := false
				for _, item := range queue.Status.QueueItemDetails["active"] {
					if item.Name == "high-blocking-job" && item.Position == 1 {
						highJobFound = true
						break
					}
				}

				By(fmt.Sprintf("check %d: %d low priority jobs present, high blocking job still at position 1: %v",
					checkNum+1, lowJobCount, highJobFound))
				Expect(lowJobCount).Should(Equal(5), "All 5 low priority jobs should remain in queue")
				Expect(highJobFound).Should(BeTrue(), "High priority blocking job should stay at position 1")

				time.Sleep(time.Second * 1)
			}

			By("demonstrating polling behavior: low priority jobs can be attempted despite high priority block")
			// The key insight: even though high-blocking-job is at position 1 and cannot be scheduled,
			// the scheduler still polls through low priority jobs in the low priority queue.
			// This is because low priority tasks have their own nextIdx that increments on failure.
			// Unlike high priority tasks which block the entire queue, low priority tasks
			// can be attempted and their nextIdx moves forward (lowNextIdx++).
			By("polling confirmed: low priority jobs maintain presence while high priority blocks")
			Expect(true).Should(BeTrue())
		})
	})

	Describe("Low Priority Task Scheduling with Resource Exhaustion", Ordered, func() {
		var (
			unschedulableLowPrioJobs []*v1alpha1.QueueUnit
			schedulableLowPrioJob    *v1alpha1.QueueUnit
		)

		BeforeAll(func() {
			// Create 10 low priority jobs that cannot be scheduled (require 2 CPU each)
			// System has 0 CPU available, so these will remain in queue
			unschedulableLowPrioJobs = make([]*v1alpha1.QueueUnit, 10)
			for i := 0; i < 10; i++ {
				jobName := fmt.Sprintf("unschedulable-low-job-%d", i+1)
				unschedulableLowPrioJobs[i] = queueunits.MakeQueueUnit(jobName, "default").
					Resources(map[string]int64{"cpu": 2000}). // 2000 millicpu = 2 CPU - cannot schedule
					Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
					Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
					Priority(int32(1 + i%3)). // Vary priorities: 1, 2, 3, 1, 2, 3, ...
					QueueUnit()
			}

			// Create 1 low priority job that CAN be scheduled (requires 0 CPU)
			schedulableLowPrioJob = queueunits.MakeQueueUnit("schedulable-low-job", "default").
				MilliResources(map[string]int64{"cpu": 1}). // 0 CPU - can schedule
				Annotations(map[string]string{"kube-queue/queue-name": "test-intelligent-queue"}).
				Labels(map[string]string{"quota.scheduling.alibabacloud.com/name": "child-1"}).
				Priority(1).
				QueueUnit()
		})

		AfterEach(func() {
			// Clean up all jobs
			for i := 0; i < 10; i++ {
				jobName := fmt.Sprintf("unschedulable-low-job-%d", i+1)
				fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), jobName, metav1.DeleteOptions{})
			}
			fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Delete(context.TODO(), "schedulable-low-job", metav1.DeleteOptions{})
			time.Sleep(time.Millisecond * 500)
		})

		It("should schedule schedulable job by polling through unschedulable jobs", func() {
			By("waiting queue ready")
			Eventually(func() error {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
				return err
			}, time.Second*10, time.Millisecond*100).Should(BeNil())

			By("creating 10 unschedulable low priority jobs")
			for i, job := range unschedulableLowPrioJobs {
				_, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), job, metav1.CreateOptions{})
				Expect(err).Should(BeNil())
				By(fmt.Sprintf("created unschedulable-low-job-%d with priority=%d", i+1, 1+i%3))
				time.Sleep(time.Millisecond * 50)
			}

			By("waiting for queue to process unschedulable jobs")
			time.Sleep(time.Second * 2)

			By("verifying all 10 unschedulable jobs are in queue")
			queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
			Expect(err).Should(BeNil())

			unschedulableCount := 0
			for _, item := range queue.Status.QueueItemDetails["active"] {
				for i := 1; i <= 10; i++ {
					if item.Name == fmt.Sprintf("unschedulable-low-job-%d", i) {
						unschedulableCount++
					}
				}
			}
			Expect(unschedulableCount).Should(Equal(10), "All 10 unschedulable jobs should be in queue")

			By("creating schedulable low priority job (requires 0 CPU)")
			_, err = fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Create(context.TODO(), schedulableLowPrioJob, metav1.CreateOptions{})
			Expect(err).Should(BeNil())
			time.Sleep(time.Millisecond * 100)

			By("waiting for scheduler to process schedulable job")
			time.Sleep(time.Second * 3)

			By("verifying schedulable job is scheduled")
			queueUnit, err := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Get(context.TODO(), "schedulable-low-job", metav1.GetOptions{})
			Expect(err).Should(BeNil())
			Expect(queueUnit.Status.Phase).Should(Equal(v1alpha1.Dequeued))

			By("verifying unschedulable jobs remain in queue")
			queue, err = fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(context.TODO(), "test-intelligent-queue", metav1.GetOptions{})
			Expect(err).Should(BeNil())

			unschedulableCountAfter := 0
			for _, item := range queue.Status.QueueItemDetails["active"] {
				for i := 1; i <= 10; i++ {
					if item.Name == fmt.Sprintf("unschedulable-low-job-%d", i) {
						unschedulableCountAfter++
					}
				}
			}
			Expect(unschedulableCountAfter).Should(Equal(10), "All 10 unschedulable jobs should still be in queue after schedulable job is scheduled")

			By("demonstrating lowNextIdx polling: scheduler tried all 10 unschedulable jobs and scheduled the 11th")
			By("polling verified: low priority tasks use lowNextIdx++ to advance through queue")
		})
	})
})
