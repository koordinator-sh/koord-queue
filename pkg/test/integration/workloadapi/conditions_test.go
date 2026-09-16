package workloadapi_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// MR1: the phase state machine is mirrored into status.conditions. The assertions go through
// the apiserver on purpose: a condition missing from the CRD schema would be silently pruned,
// which is exactly the class of failure that has bitten this repo before.
var _ = Describe("QueueUnit status conditions", Ordered, func() {

	const (
		quotaName = "conditions-quota"
		checkName = "conditions-check"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	unit := func(name string, cpu string) *v1alpha1.QueueUnit {
		cpuQty := resource.MustParse(cpu)
		return queueunits.MakeQueueUnit(name, unitNS).
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
			Priority(10).
			Labels(map[string]string{elasticquotav1alpha1.QuotaNameLabelKey: quotaName}).
			QueueUnit()
	}

	conditionOf := func(qu *v1alpha1.QueueUnit, condType string) *metav1.Condition {
		for i := range qu.Status.Conditions {
			if qu.Status.Conditions[i].Type == condType {
				return &qu.Status.Conditions[i]
			}
		}
		return nil
	}

	BeforeAll(func() {
		By("creating the Queue and its ElasticQuota (cpu:4)")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: queueNS},
			Spec:       v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("4")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("4")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		_ = cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "cond-admitted", metav1.DeleteOptions{})
		_ = cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "cond-evicted", metav1.DeleteOptions{})
	})

	It("should persist QuotaReserved and Admitted conditions once the unit is admitted", func() {
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("cond-admitted", "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the scheduler to admit the queue unit")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "cond-admitted", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying the conditions survived the apiserver round trip")
		qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "cond-admitted", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		admitted := conditionOf(qu, v1alpha1.QueueUnitAdmitted)
		Expect(admitted).NotTo(BeNil(), "Admitted condition must be persisted, not pruned by the CRD schema")
		Expect(admitted.Status).To(Equal(metav1.ConditionTrue))
		Expect(admitted.Reason).NotTo(BeEmpty())
		Expect(admitted.LastTransitionTime.IsZero()).To(BeFalse())

		reserved := conditionOf(qu, v1alpha1.QueueUnitQuotaReserved)
		Expect(reserved).NotTo(BeNil())
		Expect(reserved.Status).To(Equal(metav1.ConditionTrue))
	})

	It("should report the Evicted condition when an admission check rejects the unit", func() {
		By("adding an admission check to the queue so the unit stops at Reserved")
		Eventually(func(g Gomega) {
			q, err := cli.SchedulingV1alpha1().Queues(queueNS).Get(ctx, quotaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			q.Spec.AdmissionChecks = []v1alpha1.AdmissionCheckWithSelector{{Name: checkName}}
			_, err = cli.SchedulingV1alpha1().Queues(queueNS).Update(ctx, q, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("cond-evicted", "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the unit to reserve quota and wait on its admission check")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "cond-evicted", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Reserved))
			g.Expect(qu.Status.AdmissionChecks).NotTo(BeEmpty())

			quotaReserved := conditionOf(qu, v1alpha1.QueueUnitQuotaReserved)
			g.Expect(quotaReserved).NotTo(BeNil())
			g.Expect(quotaReserved.Status).To(Equal(metav1.ConditionTrue))
			// Quota is reserved but the unit is not admitted until every check is ready.
			admitted := conditionOf(qu, v1alpha1.QueueUnitAdmitted)
			g.Expect(admitted).NotTo(BeNil())
			g.Expect(admitted.Status).To(Equal(metav1.ConditionFalse))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("rejecting the admission check the way an external check controller would")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "cond-evicted", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			for i := range qu.Status.AdmissionChecks {
				qu.Status.AdmissionChecks[i].State = kueue.CheckStateRejected
				qu.Status.AdmissionChecks[i].LastTransitionTime = metav1.Now()
				qu.Status.AdmissionChecks[i].Message = "rejected by the test"
			}
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying QueueUnitController records the eviction in conditions alongside the phase")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "cond-evicted", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Backoff))

			evicted := conditionOf(qu, v1alpha1.QueueUnitEvicted)
			g.Expect(evicted).NotTo(BeNil())
			g.Expect(evicted.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(evicted.Reason).To(Equal(v1alpha1.QueueUnitEvictedByBackoffTimeout))

			// The reservation must no longer be advertised once the unit backs off.
			quotaReserved := conditionOf(qu, v1alpha1.QueueUnitQuotaReserved)
			g.Expect(quotaReserved).NotTo(BeNil())
			g.Expect(quotaReserved.Status).To(Equal(metav1.ConditionFalse))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})
