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

// This suite verifies that after a victim is preempted and its resources are
// reclaimed (Replicas reduced to 0 by job-extension), the victim's updating
// flag is cleared so findNextQueueUnit can re-schedule it.
var _ = Describe("ElasticQuotaV2 victim re-scheduling after reclaim", Ordered, func() {

	const (
		quotaName = "reclaim-resched-quota"
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
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "reclaim-victim", metav1.DeleteOptions{})
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "reclaim-preemptor", metav1.DeleteOptions{})
	})

	It("should re-schedule a victim after its resources are reclaimed following preemption", func() {

		By("creating the low-priority victim and waiting for Dequeued")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("reclaim-victim", 10, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil())
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("creating the high-priority preemptor (Filter passes -> Reserve preempts victim)")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("reclaim-preemptor", 100, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying victim gets ReclaimState set")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].ReclaimState).NotTo(BeNil())
			g.Expect(qu.Status.Admissions[0].ReclaimState.Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("verifying preemptor is Dequeued")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-preemptor", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("simulating preemptor Pods Running (so it leaves assumed)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-preemptor", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			qu.Status.Admissions[0].Running = 1
			qu.Status.Phase = v1alpha1.Dequeued
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("simulating job-extension reclaiming victim: Replicas → 0, clear ReclaimState")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			// Simulate job-extension: reduce Replicas to 0 and clear ReclaimState
			qu.Status.Admissions[0].Replicas = 0
			qu.Status.Admissions[0].ReclaimState = nil
			qu.Status.Message = ""
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying victim is re-scheduled (Dequeued again with new admission)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reclaim-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			// After reclaim, updating should be cleared, and the victim should be
			// re-scheduled: findNextQueueUnit picks it up, Filter passes (quota available),
			// Reserve passes (assumed empty), Dequeue succeeds.
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued),
				"victim should be re-Dequeued after reclaim, got phase: %s, msg: %s", qu.Status.Phase, qu.Status.Message)
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)),
				"victim should have a fresh admission with Replicas=1")
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil(),
				"victim's new admission should not have ReclaimState")
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())
	})
})
