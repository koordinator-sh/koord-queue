package activation

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
)

// MR2 end-to-end: deactivating a queue unit must suspend the job so its pods go away, and the
// eviction must be recorded on the Evicted condition. The quota itself is handed back by the
// existing "pods disappear -> replicas shrink" path, which is why the assertions here focus on
// the job being suspended rather than on touching quota directly.
var _ = Describe("QueueUnit deactivation", func() {

	const unitNS = "default"

	makeJob := func(name string, annotations map[string]string) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   unitNS,
				Annotations: annotations,
			},
			Spec: batchv1.JobSpec{
				Parallelism: ptr.To(int32(1)),
				// Start suspended: this is how kube-queue takes control of a job.
				Suspend: ptr.To(true),
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{
							Name:  "main",
							Image: "registry.k8s.io/pause:3.9",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": resource.MustParse("1")},
							},
						}},
					},
				},
			},
		}
	}

	waitForQueueUnit := func(name string) *v1alpha1.QueueUnit {
		qu := &v1alpha1.QueueUnit{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		return qu
	}

	conditionOf := func(qu *v1alpha1.QueueUnit, condType string) *metav1.Condition {
		for i := range qu.Status.Conditions {
			if qu.Status.Conditions[i].Type == condType {
				return &qu.Status.Conditions[i]
			}
		}
		return nil
	}

	// admit marks the queue unit as admitted with one running replica, which is the state the
	// scheduler and the resource reporter leave behind once a job is running.
	admit := func(name string) {
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			qu.Status.Phase = v1alpha1.Dequeued
			qu.Status.Message = "admitted by the test"
			qu.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
			qu.Status.LastAllocateTime = &metav1.Time{Time: time.Now()}
			qu.Status.Admissions = []v1alpha1.Admission{{
				Name:      name,
				Replicas:  1,
				Running:   1,
				Resources: corev1.ResourceList{"cpu": resource.MustParse("1")},
			}}
			g.Expect(k8sClient.Status().Update(ctx, qu)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	}

	It("should create the queue unit already inactive when the job is annotated inactive", func() {
		name := "deactivate-at-birth"
		job := makeJob(name, map[string]string{framework.ActiveAnnotationKey: "false"})
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, job) })

		qu := waitForQueueUnit(name)
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			g.Expect(qu.Spec.Active).NotTo(BeNil(), "spec.active must be set from the job annotation at creation time")
			g.Expect(*qu.Spec.Active).To(BeFalse())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	It("should suspend the job and record the eviction when the queue unit is deactivated", func() {
		name := "deactivate-running"
		job := makeJob(name, nil)
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, job) })

		waitForQueueUnit(name)

		By("pretending the scheduler admitted the unit and the job was resumed")
		admit(name)
		Eventually(func(g Gomega) {
			cur := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, cur)).To(Succeed())
			cur.Spec.Suspend = ptr.To(false)
			g.Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("deactivating the queue unit, the way an operator or an external controller would")
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			qu.Spec.Active = ptr.To(false)
			g.Expect(k8sClient.Update(ctx, qu)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying the job is suspended again so its pods are removed")
		Eventually(func(g Gomega) {
			cur := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, cur)).To(Succeed())
			g.Expect(cur.Spec.Suspend).NotTo(BeNil())
			g.Expect(*cur.Spec.Suspend).To(BeTrue(), "a deactivated queue unit must suspend its job")
		}, 60*time.Second, 500*time.Millisecond).Should(Succeed())

		By("verifying the queue unit is parked back in the queue with an Evicted condition")
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Enqueued))

			evicted := conditionOf(qu, v1alpha1.QueueUnitEvicted)
			g.Expect(evicted).NotTo(BeNil())
			g.Expect(evicted.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(evicted.Reason).To(Equal(v1alpha1.QueueUnitEvictedByDeactivation))

			// It must not be advertised as admitted any more.
			admitted := conditionOf(qu, v1alpha1.QueueUnitAdmitted)
			g.Expect(admitted).NotTo(BeNil())
			g.Expect(admitted.Status).To(Equal(metav1.ConditionFalse))
		}, 60*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	It("should deactivate a queue unit that outlives its maximum execution time", func() {
		name := "max-exec-time"
		job := makeJob(name, map[string]string{framework.MaxExecTimeSecondsAnnotationKey: "2"})
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, job) })

		waitForQueueUnit(name)

		By("verifying the budget was taken from the job annotation")
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			g.Expect(qu.Spec.MaximumExecutionTimeSeconds).NotTo(BeNil())
			g.Expect(*qu.Spec.MaximumExecutionTimeSeconds).To(Equal(int32(2)))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("marking the pods as running, which is what starts the execution clock")
		admit(name)
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			qu.Status.Phase = v1alpha1.Running
			qu.Status.Conditions = []metav1.Condition{{
				Type:   v1alpha1.QueueUnitPodsReady,
				Status: metav1.ConditionTrue,
				Reason: "PodsRunning",
				// Already over the 2s budget so the deadline fires on the next reconcile.
				LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Second)),
				Message:            "pods are running",
			}}
			g.Expect(k8sClient.Status().Update(ctx, qu)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("nudging the reconciler so it observes the expired budget")
		Eventually(func(g Gomega) {
			cur := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, cur)).To(Succeed())
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["test.kube-queue/nudge"] = fmt.Sprintf("%d", time.Now().UnixNano())
			g.Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying the queue unit was deactivated with the right reason and a reset budget")
		Eventually(func(g Gomega) {
			qu := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: name}, qu)).To(Succeed())
			g.Expect(qu.Spec.Active).NotTo(BeNil())
			g.Expect(*qu.Spec.Active).To(BeFalse(), "exceeding the execution budget must deactivate the unit")

			evicted := conditionOf(qu, v1alpha1.QueueUnitEvicted)
			g.Expect(evicted).NotTo(BeNil())
			g.Expect(evicted.Reason).To(Equal(v1alpha1.QueueUnitEvictedByMaximumExecutionTimeExceeded))

			// Reactivation is a deliberate act that grants a fresh budget.
			g.Expect(qu.Status.AccumulatedPastExecutionTimeSeconds).To(BeNil())
		}, 90*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})
