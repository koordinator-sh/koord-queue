package resourcereportcontroller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	v1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

var _ = Describe("ResourceReporter Integration Tests", func() {
	var (
		ctx       context.Context
		namespace string = "default"
	)

	BeforeEach(func() {
		ctx = context.TODO()
	})

	It("should update QueueUnit PodSet replicas when RayCluster Pods change", func() {
		const (
			rayJobName     = "rayjob-test"
			rayClusterName = "raycluster-test"
			queueUnitName  = "rayjob-test-ray-qu"
		)

		// 创建 RayJob
		rayJob := &rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        rayJobName,
				Namespace:   namespace,
				Annotations: map[string]string{"kube-queue/job-enqueue-timestamp": "123"},
			},
			Spec: rayv1.RayJobSpec{
				Entrypoint: "python train.py",
				RayClusterSpec: &rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{RayStartParams: map[string]string{"num_cpus": "1"},
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "ray-head", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}}}},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
						RayStartParams: map[string]string{"resources_per_worker": "1"},
						GroupName:      "worker", Replicas: ptr.To(int32(0)),
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "worker", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}},
						},
					}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rayJob)).Should(Succeed())
		rayJob.Status.RayClusterName = rayClusterName
		Expect(k8sClient.Status().Update(ctx, rayJob)).Should(Succeed())

		// 创建 RayCluster
		rayCluster := &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rayClusterName,
				Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayJob",
						Name:       rayJobName,
						UID:        rayJob.UID,
					},
				},
			},
			Spec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{RayStartParams: map[string]string{"num_cpus": "1"},
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "head", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}},
					}},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
					RayStartParams: map[string]string{"resources_per_worker": "1"},
					GroupName:      "worker", Replicas: ptr.To(int32(2)),
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "worker", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}},
					},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, rayCluster)).Should(Succeed())

		// 创建 QueueUnit
		queueUnit := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      queueUnitName,
				Namespace: namespace,
			},
			Spec: v1alpha1.QueueUnitSpec{
				ConsumerRef: &corev1.ObjectReference{
					APIVersion: "ray.io/v1",
					Kind:       "RayJob",
					Name:       rayJobName,
					Namespace:  namespace,
				},
				PodSets: []kueue.PodSet{
					{
						Name:  "head",
						Count: 1,
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "ray-head", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}}},
					},
					{
						Name:  "worker",
						Count: 1,
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "ray-head", Image: "rayproject/autoscaler:2.5.0", Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}}}}},
					},
				},
			},
		}
		By("Create queueUnit")
		Expect(k8sClient.Create(ctx, queueUnit)).Should(Succeed())
		queueUnit.Status.Admissions = []v1alpha1.Admission{{
			Name: "head", Replicas: 1, Running: 0,
		}, {
			Name: "worker", Replicas: 2, Running: 0,
		}}
		queueUnit.Status.Phase = v1alpha1.Dequeued
		queueUnit.Status.LastUpdateTime = ptr.To(metav1.Now())
		By("Update queueUnit's status")
		Expect(k8sClient.Update(ctx, queueUnit)).Should(Succeed())

		// 创建 Head Pod
		headPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "head-pod",
				Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       rayClusterName,
						UID:        types.UID("fake-raycluster-uid"),
					},
				},
				Labels: map[string]string{
					"ray.io/node-type": "head",
				},
				Annotations: map[string]string{
					util.RelatedQueueUnitAnnoKey: queueUnit.Namespace + "/" + queueUnit.Name,
					util.RelatedPodSetAnnoKey:    "head",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "container", Image: "pause"}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		}
		By("Create head pod")
		Expect(k8sClient.Create(ctx, headPod)).Should(Succeed())

		// 创建两个 Worker Pod
		for i := 0; i < 2; i++ {
			workerPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("worker-pod-%d", i),
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ray.io/v1",
							Kind:       "RayCluster",
							Name:       rayClusterName,
							UID:        types.UID("fake-raycluster-uid"),
						},
					},
					Labels: map[string]string{
						"ray.io/node-type": "worker",
					},
					Annotations: map[string]string{
						util.RelatedQueueUnitAnnoKey: queueUnit.Namespace + "/" + queueUnit.Name,
						util.RelatedPodSetAnnoKey:    "worker",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "container", Image: "pause"}},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			}
			Expect(k8sClient.Create(ctx, workerPod)).Should(Succeed())
		}

		By("Waiting for reconciling")
		// 获取最新的 QueueUnit
		updatedQu := &v1alpha1.QueueUnit{}
		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: queueUnitName}, updatedQu)
			if err == nil && len(updatedQu.Spec.PodSets) > 0 {
				return updatedQu.Spec.PodSets[0].Count == 1 && updatedQu.Spec.PodSets[1].Count == 2
			}
			return false
		}, 5*time.Hour, 500*time.Millisecond).Should(BeTrue())

		// 验证 PodSet 数量是否正确更新
		Expect(updatedQu.Spec.PodSets[0].Count).Should(Equal(int32(1)))
		Expect(updatedQu.Spec.PodSets[1].Count).Should(Equal(int32(2)))

		Expect(k8sClient.Delete(ctx, rayJob)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, rayCluster)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, queueUnit)).Should(Succeed())
		Expect(k8sClient.DeleteAllOf(ctx, headPod, client.InNamespace("default"))).Should(Succeed())
	})

})
