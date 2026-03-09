package admissioncontroller

import (
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
	"github.com/kube-queue/kube-queue/pkg/jobext/test/util/wrappers"

	client "sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var _ = Describe("Admission Controller", func() {
	Context("Create a QueueUnit and related pods.", func() {
		It("ScheduleAdmission should be managed by admission controller.", func() {
			queueunit := &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-queueunit",
					Namespace: "default",
				},
				Spec: v1alpha1.QueueUnitSpec{PodSets: []kueue.PodSet{{
					Name: "test-podset", Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "default"}}}},
				}}},
				Status: v1alpha1.QueueUnitStatus{Admissions: []v1alpha1.Admission{{
					Name: "test-podset", Replicas: 2,
				}}},
			}
			By("Create QueueUnit")
			Expect(k8sClient.Create(ctx, queueunit)).To(Succeed())

			queueUnitLookUpKey := types.NamespacedName{
				Name:      queueunit.Name,
				Namespace: queueunit.Namespace,
			}
			expectQueueUnit := &v1alpha1.QueueUnit{}

			By("Expect QueueUnit to be created")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, queueUnitLookUpKey, expectQueueUnit)).To(Succeed())
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			pods := []*v1.Pod{
				wrappers.MakePod().Name("pod-1").UID("pod-1").Namespace("default").Container("default").Labels(map[string]string{util.SchedulerAdmissionLabelKey: "false"}).
					Annotations(map[string]string{util.RelatedQueueUnitAnnoKey: queueunit.Namespace + "/" + queueunit.Name, util.RelatedPodSetAnnoKey: "test-podset"}).Obj(),
				wrappers.MakePod().Name("pod-2").UID("pod-2").Namespace("default").Container("default").Labels(map[string]string{util.SchedulerAdmissionLabelKey: "false"}).
					Annotations(map[string]string{util.RelatedQueueUnitAnnoKey: queueunit.Namespace + "/" + queueunit.Name, util.RelatedPodSetAnnoKey: "test-podset"}).Obj(),
				wrappers.MakePod().Name("pod-3").UID("pod-3").Namespace("default").Container("default").Labels(map[string]string{util.SchedulerAdmissionLabelKey: "false"}).
					Annotations(map[string]string{util.RelatedQueueUnitAnnoKey: queueunit.Namespace + "/" + queueunit.Name, util.RelatedPodSetAnnoKey: "test-podset"}).Obj(),
				wrappers.MakePod().Name("pod-4").UID("pod-4").Namespace("default").Container("default").Labels(map[string]string{util.SchedulerAdmissionLabelKey: "false"}).
					Annotations(map[string]string{util.RelatedQueueUnitAnnoKey: queueunit.Namespace + "/" + queueunit.Name, util.RelatedPodSetAnnoKey: "test-podset"}).Obj(),
				wrappers.MakePod().Name("pod-5").UID("pod-5").Namespace("default").Container("default").Labels(map[string]string{util.SchedulerAdmissionLabelKey: "false"}).
					Annotations(map[string]string{util.RelatedQueueUnitAnnoKey: queueunit.Namespace + "/" + queueunit.Name, util.RelatedPodSetAnnoKey: "test-podset"}).Obj(),
			}

			for _, pod := range pods {
				By("Create pods")
				Expect(k8sClient.Create(ctx, pod)).Should(Succeed())
			}

			queueunit.Status.Admissions = []v1alpha1.Admission{{
				Name: "test-podset", Replicas: 2, Running: 0,
			}}
			queueunit.Status.LastUpdateTime = ptr.To(metav1.Now())
			By("Scale up queueunit")
			Expect(k8sClient.Status().Update(ctx, queueunit)).To(Succeed())

			By("Expect 2 pods can be scheduled")
			Eventually(func() bool {
				podList := v1.PodList{}
				k8sClient.List(ctx, &podList, client.InNamespace("default"), client.MatchingLabels{util.SchedulerAdmissionLabelKey: "true"})
				return len(podList.Items) == 2
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			queueunit.Status.Admissions = []v1alpha1.Admission{{
				Name: "test-podset", Replicas: 5, Running: 0,
			}}
			queueunit.Status.LastUpdateTime = ptr.To(metav1.Now())
			By("Scale up queueunit")
			Expect(k8sClient.Status().Update(ctx, queueunit)).To(Succeed())

			time.Sleep(1 * time.Second)
			By("Expect 5 pods can be scheduled")
			Eventually(func() bool {
				podList := v1.PodList{}
				k8sClient.List(ctx, &podList, client.InNamespace("default"), client.MatchingLabels{util.SchedulerAdmissionLabelKey: "true"})
				return len(podList.Items) == 5
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			queueunit.Status.Admissions = []v1alpha1.Admission{{
				Name: "test-podset", Replicas: 0, Running: 0,
			}}
			queueunit.Status.LastUpdateTime = ptr.To(metav1.Now())
			By("Scale up queueunit")
			Expect(k8sClient.Status().Update(ctx, queueunit)).To(Succeed())

			time.Sleep(1 * time.Second)
			By("Expect 5 pods can be scheduled")
			Eventually(func() bool {
				podList := v1.PodList{}
				k8sClient.List(ctx, &podList, client.InNamespace("default"), client.MatchingLabels{util.SchedulerAdmissionLabelKey: "true"})
				return len(podList.Items) == 0
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())
		})
	})
})
