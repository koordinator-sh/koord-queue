package elasticquotav1alpha1preemption_test

import (
	"fmt"
	"strings"
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

// This suite exercises the Reserve-level preemption path with different
// QueuePolicies (Priority and Block). Both should support preemption
// as long as the annotations are set.
var _ = DescribeTable("ElasticQuotaV2 Reserve-level preemption",
	func(policy string) {
		const (
			queueNS = "koord-queue"
			unitNS  = "default"
		)
		quotaName := fmt.Sprintf("reserve-preempt-%s", strings.ToLower(policy))
		victimName := fmt.Sprintf("%s-victim", strings.ToLower(policy))
		preemptorName := fmt.Sprintf("%s-preemptor", strings.ToLower(policy))

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

		By(fmt.Sprintf("creating a %s-policy Queue with preemption annotations", policy))
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:      quotaName,
				Namespace: queueNS,
				Annotations: map[string]string{
					schedulingqueuev2.WaitForPodsRunningAnnotation: "true",
					schedulingqueuev2.EnableQueueUnitPreemption:    "true",
				},
			},
			Spec: v1alpha1.QueueSpec{QueuePolicy: v1alpha1.QueuePolicy(policy)},
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
			g.Expect(string(q.Spec.QueuePolicy)).To(Equal(policy))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		defer func() {
			cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, victimName, metav1.DeleteOptions{})
			cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, preemptorName, metav1.DeleteOptions{})
		}()

		By("creating the low-priority victim and waiting for Dequeued")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit(victimName, 10, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, victimName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil(), "victim not preempted yet")
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("creating the high-priority preemptor (Filter passes -> Reserve preempts victim)")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit(preemptorName, 100, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying victim gets ReclaimState set")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, victimName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].ReclaimState).NotTo(BeNil(),
				"preemption should work with %s policy, victim should have ReclaimState", policy)
			g.Expect(qu.Status.Admissions[0].ReclaimState.Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())

		By("verifying preemptor is Dequeued")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, preemptorName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued),
				"preemptor should be Dequeued with %s policy, got phase: %s, msg: %s", policy, qu.Status.Phase, qu.Status.Message)
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
		}, preemptionTimeout, 200*time.Millisecond).Should(Succeed())
	},
	Entry("with Priority policy", "Priority"),
	Entry("with Block policy", "Block"),
)
