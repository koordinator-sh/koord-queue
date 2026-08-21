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

// This suite exercises the queue-level preemption path ("dequeue-to-preempt when preemptible"):
// a higher-priority QueueUnit that cannot fit within its quota causes the scheduler to invoke
// q.Preempt, which marks a lower-priority reserved victim for reclamation by setting ReclaimState
// on its admission. It uses the ElasticQuotaV2 (elasticquotav1alpha1) plugin because that plugin's
// Filter returns Unschedulable when a quota is exhausted (it has no in-Filter preemption dry-run),
// which is precisely what routes the scheduler into q.Preempt (scheduler.go: Filter != Success).
var _ = Describe("ElasticQuotaV2 queue-level preemption", Ordered, func() {

	const (
		quotaName = "preempt-quota"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	// preemptibleUnit builds a QueueUnit routed to quotaName, requesting cpu and carrying a named
	// PodSet so it acquires a reclaimable admission once dequeued.
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
		By("pre-creating a preemption-enabled Queue (name == quota name) so the plugin's reconcile preserves the annotations")
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

		By("creating the ElasticQuota (Min == Max == cpu:2) so a single 2-cpu unit exhausts the guaranteed quota")
		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("2")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("2")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the preemption annotations survived the plugin's Queue reconcile")
		Eventually(func(g Gomega) {
			q, err := cli.SchedulingV1alpha1().Queues(queueNS).Get(ctx, quotaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(q.Annotations[schedulingqueuev2.WaitForPodsRunningAnnotation]).To(Equal("true"))
			g.Expect(q.Annotations[schedulingqueuev2.EnableQueueUnitPreemption]).To(Equal("true"))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	AfterAll(func() {
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "victim", metav1.DeleteOptions{})
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "preemptor", metav1.DeleteOptions{})
	})

	It("should dequeue a lower-priority victim for reclamation when a higher-priority unit needs the quota", func() {
		By("creating the low-priority victim and waiting for it to be Dequeued with a reserved admission")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("victim", 10, "2"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil(), "victim not preempted yet")
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("creating the high-priority preemptor whose request exhausts the quota (Filter -> Unschedulable -> q.Preempt)")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("preemptor", 100, "2"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the victim's admission gets ReclaimState set (dequeued for preemption)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].ReclaimState).NotTo(BeNil(),
				"a higher-priority unit should have caused the victim to be marked for reclamation")
			g.Expect(qu.Status.Admissions[0].ReclaimState.Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())
	})
})
