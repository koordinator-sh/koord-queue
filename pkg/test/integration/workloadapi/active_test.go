package workloadapi_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// MR2: spec.active gates admission. The interesting property is not just that an inactive unit
// stays out of the scheduler, but that it does not consume quota while it waits: the whole point
// of deactivating a job is to give its capacity to somebody else.
var _ = Describe("QueueUnit spec.active", Ordered, func() {

	const (
		quotaName = "active-quota"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	unit := func(name string, cpu string, active *bool) *v1alpha1.QueueUnit {
		cpuQty := resource.MustParse(cpu)
		w := queueunits.MakeQueueUnit(name, unitNS).
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
			Labels(map[string]string{elasticquotav1alpha1.QuotaNameLabelKey: quotaName})
		if active != nil {
			w = w.Active(*active)
		}
		return w.QueueUnit()
	}

	BeforeAll(func() {
		By("enabling the QueueUnitActive feature gate for this suite")
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.QueueUnitActive): true,
		})).To(Succeed())

		By("creating the Queue and its ElasticQuota (cpu:2, room for a single unit)")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: queueNS},
			Spec:       v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("2")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("2")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		for _, name := range []string{"active-born-inactive", "active-rival", "active-flip"} {
			_ = cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, name, metav1.DeleteOptions{})
		}
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.QueueUnitActive): false,
		})).To(Succeed())
	})

	It("should never admit a queue unit that is created inactive, and leave its quota free", func() {
		By("creating a queue unit that is inactive from birth")
		inactive := unit("active-born-inactive", "2", ptrBool(false))
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, inactive, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying spec.active survived the apiserver round trip")
		created, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-born-inactive", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.Spec.Active).NotTo(BeNil(), "spec.active must be persisted, not pruned by the CRD schema")
		Expect(*created.Spec.Active).To(BeFalse())

		By("checking it is never admitted")
		Consistently(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-born-inactive", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).NotTo(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Phase).NotTo(Equal(v1alpha1.Reserved))
			g.Expect(qu.Status.Admissions).To(BeEmpty())
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		By("verifying the whole quota is still available to another unit")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("active-rival", "2", nil), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-rival", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("cleaning up so the following spec starts from an empty quota")
		Expect(cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "active-rival", metav1.DeleteOptions{})).To(Succeed())
		Expect(cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "active-born-inactive", metav1.DeleteOptions{})).To(Succeed())
	})

	It("should admit a queue unit once it is activated", func() {
		By("creating an inactive queue unit")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("active-flip", "1", ptrBool(false)), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Consistently(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-flip", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).NotTo(Equal(v1alpha1.Dequeued))
		}, 5*time.Second, 500*time.Millisecond).Should(Succeed())

		By("activating it")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-flip", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			qu.Spec.Active = ptrBool(true)
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Update(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying it gets admitted now")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "active-flip", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})

func ptrBool(b bool) *bool { return &b }
