package reservationcontroller

import (
	"os"
	"time"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	jobv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/common/job_controller/v1"
	pytorchv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/pytorch/v1"
	"github.com/kube-queue/kube-queue/pkg/jobext/handles"
	v1 "k8s.io/api/core/v1"
)

var _ = Describe("PytorchJob Reservation", func() {
	Context("Create a PyTorchJob and EnableReservation is True.", func() {
		It("Reservation should be created. And when reservation is scheduled, queueUnit should be schedSucceed.", func() {

			pytorchjob := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pytorchjob-resv-test",
					Namespace: "default",
					Annotations: map[string]string{
						handles.QueueAnnotation: "true",
					},
				},
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*jobv1.ReplicaSpec{
						"Master": &jobv1.ReplicaSpec{
							Replicas: ptr.To(int32(1)),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch/pytorch:1.9.0-cuda11.1-cudnn8-runtime",
											Command: []string{
												"python",
												"-c",
												"import torch",
											},
										},
									},
								},
							},
						},
						"Worker": &jobv1.ReplicaSpec{
							Replicas: ptr.To(int32(1)),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch/pytorch:1.9.0-cuda11.1-cudnn8-runtime",
											Command: []string{
												"python",
												"-c",
												"import torch",
											},
										},
									},
								},
							},
						},
					},
				},
			}
			By("Create PyTorchJob")
			Expect(k8sClient.Create(ctx, pytorchjob)).To(Succeed())

			queueUnitLookUpKey := types.NamespacedName{
				Name:      "pytorchjob-resv-test-py-qu",
				Namespace: "default",
			}
			if os.Getenv("PAI_ENV") != "" {
				queueUnitLookUpKey.Name = "pytorchjob-resv-test"
			}
			expectQueueUnit := &v1alpha1.QueueUnit{}

			By("Expect QueueUnit to be created")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, queueUnitLookUpKey, expectQueueUnit)).To(Succeed())
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			now := metav1.Now()
			modifiedQueueUnit := expectQueueUnit.DeepCopy()
			modifiedQueueUnit.Status.Phase = v1alpha1.SchedReady
			modifiedQueueUnit.Status.LastUpdateTime = &now
			modifiedQueueUnit.Status.Attempts = 1
			Expect(k8sClient.Status().Update(ctx, modifiedQueueUnit)).To(Succeed())

			resvLookUpKey1 := types.NamespacedName{
				Name: "pytorchjob-resv-test-master-0",
			}
			expectResv1 := &koordinatorschedulerv1alpha1.Reservation{}
			resvLookUpKey2 := types.NamespacedName{
				Name: "pytorchjob-resv-test-worker-0",
			}
			expectResv2 := &koordinatorschedulerv1alpha1.Reservation{}
			By("Expect Reservation to be created")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, resvLookUpKey1, expectResv1)).To(Succeed())
				g.Expect(k8sClient.Get(ctx, resvLookUpKey2, expectResv2)).To(Succeed())
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			now = metav1.Now()
			modifiedResv1 := expectResv1.DeepCopy()
			modifiedResv1.Status.Phase = koordinatorschedulerv1alpha1.ReservationPending
			modifiedResv1.Status.Conditions = append(modifiedResv1.Status.Conditions, koordinatorschedulerv1alpha1.ReservationCondition{
				Type:               koordinatorschedulerv1alpha1.ReservationConditionScheduled,
				Status:             koordinatorschedulerv1alpha1.ConditionStatusFalse,
				Reason:             "Unschedulable/1",
				LastTransitionTime: now,
				LastProbeTime:      now,
			})
			modifiedResv2 := expectResv2.DeepCopy()
			modifiedResv2.Status.Phase = koordinatorschedulerv1alpha1.ReservationPending
			modifiedResv2.Status.Conditions = append(modifiedResv2.Status.Conditions, koordinatorschedulerv1alpha1.ReservationCondition{
				Type:               koordinatorschedulerv1alpha1.ReservationConditionScheduled,
				Status:             koordinatorschedulerv1alpha1.ConditionStatusFalse,
				Reason:             "Unschedulable/1",
				LastTransitionTime: now,
				LastProbeTime:      now,
			})
			By("Update Reservation to be Pending")
			Expect(k8sClient.Status().Update(ctx, modifiedResv1)).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, modifiedResv2)).To(Succeed())
			By("Expect QueueUnit to be schedFailed")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, queueUnitLookUpKey, expectQueueUnit)).To(Succeed())
				g.Expect(expectQueueUnit.Status.Phase).Should(Equal(v1alpha1.SchedFailed))
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			By("Update QueueUnit to be schedReady")
			now = metav1.Now()
			modifiedQueueUnit = expectQueueUnit.DeepCopy()
			modifiedQueueUnit.Status.Phase = v1alpha1.SchedReady
			modifiedQueueUnit.Status.LastUpdateTime = &now
			modifiedQueueUnit.Status.Attempts = 2
			Expect(k8sClient.Status().Update(ctx, modifiedQueueUnit)).To(Succeed())

			Expect(k8sClient.Get(ctx, resvLookUpKey1, expectResv1)).To(Succeed())
			Expect(k8sClient.Get(ctx, resvLookUpKey2, expectResv2)).To(Succeed())
			now = metav1.Now()
			modifiedResv1 = expectResv1.DeepCopy()
			modifiedResv1.Status.Phase = koordinatorschedulerv1alpha1.ReservationAvailable
			modifiedResv1.Status.Conditions[0] = koordinatorschedulerv1alpha1.ReservationCondition{
				Type:               koordinatorschedulerv1alpha1.ReservationConditionScheduled,
				Status:             koordinatorschedulerv1alpha1.ConditionStatusTrue,
				LastTransitionTime: now,
				LastProbeTime:      now,
			}
			modifiedResv2 = expectResv2.DeepCopy()
			modifiedResv2.Status.Phase = koordinatorschedulerv1alpha1.ReservationAvailable
			modifiedResv2.Status.Conditions[0] = koordinatorschedulerv1alpha1.ReservationCondition{
				Type:               koordinatorschedulerv1alpha1.ReservationConditionScheduled,
				Status:             koordinatorschedulerv1alpha1.ConditionStatusTrue,
				LastTransitionTime: now,
				LastProbeTime:      now,
			}
			By("Update Reservation to be Scheduled")
			Expect(k8sClient.Status().Update(ctx, modifiedResv1)).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, modifiedResv2)).To(Succeed())
			By("Expect QueueUnit to be schedSucceed")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, queueUnitLookUpKey, expectQueueUnit)).To(Succeed())
				g.Expect(expectQueueUnit.Status.Phase).Should(Equal(v1alpha1.SchedSucceed))
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

		})
	})
})
