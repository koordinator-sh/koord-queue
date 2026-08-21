package elasticquotav1alpha1preemption_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// This suite verifies that after a high-priority task preempts a low-priority
// victim and finishes, the remaining low-priority tasks are scheduled in
// priority order: B (priority=2) is Dequeued before A (priority=1, preempted).
var _ = Describe("ElasticQuotaV2 priority ordering after preemption", Ordered, func() {

	const (
		quotaName = "order-preempt-quota"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	preemptibleUnit := func(name string, priority int32, cpu string) *v1alpha1.QueueUnit {
		cpuQty := resource.MustParse(cpu)
		qu := queueunits.MakeQueueUnit(name, unitNS).
			PodSets(kueue.PodSet{
				Name:  name,
				Count: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": cpuQty},
						},
					}},
				}},
			}).
			Resources(map[string]int64{"cpu": cpuQty.Value()}).
			Priority(priority).
			Labels(map[string]string{
				elasticquotav1alpha1.QuotaNameLabelKey:          quotaName,
				"quota.scheduling.alibabacloud.com/preemptible": "true",
			}).
			QueueUnit()
		return qu
	}

	BeforeAll(func() {
		By("pre-creating a preemption-enabled Queue")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:      quotaName,
				Namespace: queueNS,
				Annotations: map[string]string{
					schedulingqueuev2.WaitForPodsRunningAnnotation: "true",
					schedulingqueuev2.EnableQueueUnitPreemption:    "true",
				},
			},
			Spec: v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("creating the ElasticQuota (Min == Max == cpu:4)")
		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("4")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("4")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			q, err := cli.SchedulingV1alpha1().Queues(queueNS).Get(ctx, quotaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(q.Annotations[schedulingqueuev2.WaitForPodsRunningAnnotation]).To(Equal("true"))
			g.Expect(q.Annotations[schedulingqueuev2.EnableQueueUnitPreemption]).To(Equal("true"))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	AfterAll(func() {
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "order-low-a", metav1.DeleteOptions{})
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "order-low-b", metav1.DeleteOptions{})
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "order-high-c", metav1.DeleteOptions{})
	})

	It("should schedule remaining low-priority tasks in priority order after high-priority preemption", func() {

		By("creating low-priority task A (priority=1) and waiting for Dequeued")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("order-low-a", 1, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-a", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("creating high-priority task C (priority=100) — preempts A, C is Dequeued")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("order-high-c", 100, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Wait for C to be Dequeued (it preempts A which has lowest priority)
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-high-c", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued),
				"C should be Dequeued after preempting A, got phase: %s", qu.Status.Phase)
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		// Verify A was preempted (ReclaimState set)
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-a", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].ReclaimState).NotTo(BeNil(),
				"A should have been preempted by C")
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("creating low-priority task B (priority=2) — should be blocked (C in assumed)")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("order-low-b", 2, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// B should not be Dequeued yet (C is in assumed)
		Consistently(func() v1alpha1.QueueUnitPhase {
			qu, _ := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-b", metav1.GetOptions{})
			if qu == nil {
				return ""
			}
			return qu.Status.Phase
		}, 3*time.Second, 500*time.Millisecond).Should(BeEquivalentTo(""),
			"B should not be Dequeued while C is in assumed")

		By("simulating C Pods Running so C leaves assumed")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-high-c", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			qu.Status.Admissions[0].Running = 1
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying B (priority=2) is Dequeued before A (priority=1, preempted, updating still set)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-b", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued),
				"B should be Dequeued after C leaves assumed, got phase: %s", qu.Status.Phase)
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		// A should still NOT be re-scheduled (updating still set, reclaim not done)
		Consistently(func() bool {
			qu, _ := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-a", metav1.GetOptions{})
			if qu == nil || len(qu.Status.Admissions) == 0 {
				return false
			}
			// A was preempted: if ReclaimState is cleared and Replicas=1, it was re-scheduled
			return qu.Status.Admissions[0].ReclaimState == nil && qu.Status.Admissions[0].Replicas == 1
		}, 3*time.Second, 500*time.Millisecond).Should(BeFalse(),
			"A should not be re-scheduled while B is in assumed (updating still set)")

		By("simulating B Pods Running so B leaves assumed")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-b", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			qu.Status.Admissions[0].Running = 1
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("simulating job-extension reclaiming A: Replicas → 0, clear ReclaimState")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-a", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			qu.Status.Admissions[0].Replicas = 0
			qu.Status.Admissions[0].ReclaimState = nil
			qu.Status.Message = ""
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying A (priority=1) is re-scheduled after reclaim")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "order-low-a", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)),
				"A should have a fresh admission with Replicas=1 after re-scheduling")
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil(),
				"A's new admission should not have ReclaimState")
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())
	})
})
