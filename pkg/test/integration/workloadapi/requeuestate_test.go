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

// MR3: a queue unit that keeps failing must back off for longer each time instead of hammering
// the scheduler. The retry schedule lives in status.requeueState so it survives restarts and is
// visible to users, which a flat in-memory timer could not offer.
var _ = Describe("QueueUnit requeueState", Ordered, func() {

	const (
		quotaName = "requeue-quota"
		checkName = "requeue-check"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	unit := func(name string) *v1alpha1.QueueUnit {
		cpuQty := resource.MustParse("1")
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

	rejectChecks := func(name string) {
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, name, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.AdmissionChecks).NotTo(BeEmpty())
			for i := range qu.Status.AdmissionChecks {
				qu.Status.AdmissionChecks[i].State = kueue.CheckStateRejected
				qu.Status.AdmissionChecks[i].LastTransitionTime = metav1.Now()
				qu.Status.AdmissionChecks[i].Message = "rejected by the test"
			}
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	}

	BeforeAll(func() {
		By("enabling the QueueUnitRequeueState feature gate")
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.QueueUnitRequeueState): true,
		})).To(Succeed())

		By("creating a Queue with an admission check plus its ElasticQuota")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: queueNS},
			Spec: v1alpha1.QueueSpec{
				QueuePolicy:     "Priority",
				AdmissionChecks: []v1alpha1.AdmissionCheckWithSelector{{Name: checkName}},
			},
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
		_ = cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "requeue-unit", metav1.DeleteOptions{})
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.QueueUnitRequeueState): false,
		})).To(Succeed())
	})

	It("should record a structured backoff schedule when an admission check is rejected", func() {
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("requeue-unit"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the unit to reserve quota and wait on its admission check")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "requeue-unit", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Reserved))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("rejecting the check so QueueUnitController pushes the unit into Backoff")
		rejectChecks("requeue-unit")

		By("verifying the backoff schedule was persisted")
		var firstRequeueAt time.Time
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "requeue-unit", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Backoff))

			g.Expect(qu.Status.RequeueState).NotTo(BeNil(), "requeueState must be persisted, not pruned by the CRD schema")
			g.Expect(qu.Status.RequeueState.Count).To(Equal(int32(1)), "the first backoff is attempt one")
			g.Expect(qu.Status.RequeueState.RequeueAt).NotTo(BeNil())
			// The retry must be scheduled in the future, which is what makes the backoff real.
			g.Expect(qu.Status.RequeueState.RequeueAt.Time).To(BeTemporally(">", qu.Status.LastUpdateTime.Time))
			firstRequeueAt = qu.Status.RequeueState.RequeueAt.Time
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("driving a second failure and verifying the wait grows")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "requeue-unit", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			// Put it back into Reserved with pending checks, as a re-admission would.
			qu.Status.Phase = v1alpha1.Reserved
			qu.Status.Message = "reserved again by the test"
			qu.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
			for i := range qu.Status.AdmissionChecks {
				qu.Status.AdmissionChecks[i].State = kueue.CheckStatePending
				qu.Status.AdmissionChecks[i].LastTransitionTime = metav1.Now()
			}
			_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		rejectChecks("requeue-unit")

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "requeue-unit", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Backoff))
			g.Expect(qu.Status.RequeueState).NotTo(BeNil())
			g.Expect(qu.Status.RequeueState.Count).To(Equal(int32(2)), "the attempt counter must keep climbing")
			// Second attempt waits about twice as long, so it lands after the first schedule.
			g.Expect(qu.Status.RequeueState.RequeueAt.Time).To(BeTemporally(">", firstRequeueAt))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})
