/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handles

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("RayCluster Controller", func() {
	var (
		rayClusterController *RayCluster
		fakeClient           client.Client
		ctx                  context.Context
		scheme               *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		schedulingv1.AddToScheme(scheme)
		rayv1.AddToScheme(scheme)
		v1.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		rayClusterController = &RayCluster{
			c:              fakeClient,
			managedAllJobs: false,
		}
	})

	Context("Object", func() {
		It("should return a RayCluster object", func() {
			obj := rayClusterController.Object()
			Expect(obj).To(BeAssignableToTypeOf(&rayv1.RayCluster{}))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := rayClusterController.GVK()
			Expect(gvk.Kind).To(Equal("RayCluster"))
			Expect(gvk.Version).To(Equal(rayv1.GroupVersion.Version))
			Expect(gvk.Group).To(Equal(rayv1.GroupVersion.Group))
		})
	})

	Context("GetPodSetName", func() {
		It("should return head for head group pods", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/group": "headgroup",
					},
				},
			}
			name := rayClusterController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("head"))
		})

		It("should return group name for worker group pods", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/group": "worker-group-1",
					},
				},
			}
			name := rayClusterController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("worker-group-1"))
		})

		It("should return worker for pods without group label (PAI case)", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			}
			name := rayClusterController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("worker"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod sets", func() {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName:   "worker-group-1",
							Replicas:    ptr.To[int32](2),
							MinReplicas: ptr.To[int32](0),
							MaxReplicas: ptr.To[int32](5),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "ray-worker",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
			}

			podSets := rayClusterController.PodSet(ctx, cluster)
			Expect(podSets).To(HaveLen(2))

			// Check head pod set
			Expect(podSets[0].Name).To(Equal(headGroupPodSetName))
			Expect(podSets[0].Count).To(Equal(int32(1)))

			// Check worker pod set
			Expect(podSets[1].Name).To(Equal("worker-group-1"))
			Expect(podSets[1].Count).To(Equal(int32(2)))
		})
	})

	Context("Resources", func() {
		It("should calculate resources correctly", func() {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("2"),
												v1.ResourceMemory: resource.MustParse("4Gi"),
											},
										},
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName: "worker-group-1",
							Replicas:  ptr.To[int32](3),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Resources: v1.ResourceRequirements{
												Requests: v1.ResourceList{
													v1.ResourceCPU:    resource.MustParse("1"),
													v1.ResourceMemory: resource.MustParse("2Gi"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}

			resources := rayClusterController.Resources(ctx, cluster)
			Expect(resources).NotTo(BeNil())
			Expect(resources).To(HaveKey(v1.ResourceCPU))
			Expect(resources).To(HaveKey(v1.ResourceMemory))

			// Expected: 1 head * 2 CPU + 3 workers * 1 CPU = 5 CPU
			// Expected: 1 head * 4Gi + 3 workers * 2Gi = 10Gi
			expectedCPU := resource.MustParse("5")
			expectedMemory := resource.MustParse("10Gi")
			Expect(resources[v1.ResourceCPU].Equal(expectedCPU)).To(BeTrue())
			Expect(resources[v1.ResourceMemory].Equal(expectedMemory)).To(BeTrue())
		})
	})

	Context("QueueUnitSuffix", func() {
		It("should return correct suffix when PAI_ENV is not set", func() {
			suffix := rayClusterController.QueueUnitSuffix()
			Expect(suffix).To(Equal("raycluster-qu"))
		})
	})

	Context("Priority", func() {
		It("should return correct priority", func() {
			priority := int32(100)
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								PriorityClassName: "test-priority-class",
								Priority:          &priority,
							},
						},
					},
				},
			}

			Expect(fakeClient.Create(ctx, &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-priority-class",
				},
				Value: priority,
			})).To(Succeed())
			className, priorityValue := rayClusterController.Priority(ctx, cluster)
			Expect(className).To(Equal("test-priority-class"))
			Expect(priorityValue).To(Equal(&priority))
		})
	})

	Context("Suspend", func() {
		It("should suspend cluster correctly", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: rayv1.RayClusterSpec{
					Suspend: ptr.To(false),
				},
			}

			Expect(fakeClient.Create(ctx, cluster)).To(Succeed())
			err := rayClusterController.Suspend(ctx, cluster, fakeClient)
			Expect(fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, cluster))
			Expect(err).To(BeNil())
			Expect(cluster.Spec.Suspend).To(Equal(ptr.To(true)))
		})

		It("should not suspend already suspended cluster", func() {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					Suspend: ptr.To(true),
				},
			}

			err := rayClusterController.Suspend(ctx, cluster, fakeClient)
			Expect(err).To(BeNil())
			Expect(cluster.Spec.Suspend).To(Equal(ptr.To(true)))
		})
	})

	Context("Resume", func() {
		It("should resume suspended cluster", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-dequeue-timestamp": time.Now().String(),
					},
				},
				Spec: rayv1.RayClusterSpec{
					Suspend: ptr.To(true),
				},
			}

			Expect(fakeClient.Create(ctx, cluster)).To(Succeed())
			err := rayClusterController.Resume(ctx, cluster, fakeClient)
			Expect(fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, cluster))
			Expect(err).To(BeNil())
			Expect(cluster.Spec.Suspend).To(Equal(ptr.To(false)))
			Expect(cluster.Annotations["koord-queue/job-dequeue-timestamp"]).NotTo(BeEmpty())
		})

		It("should not resume already running cluster", func() {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					Suspend: ptr.To(false),
				},
			}

			err := rayClusterController.Resume(ctx, cluster, fakeClient)
			Expect(err).To(BeNil())
			Expect(cluster.Spec.Suspend).To(Equal(ptr.To(false)))
		})
	})

	Context("GetJobStatus", func() {
		It("should return running status when worker is ready", func() {
			cluster := &rayv1.RayCluster{
				Status: rayv1.RayClusterStatus{
					ReadyWorkerReplicas: 1,
				},
			}

			status, _ := rayClusterController.GetJobStatus(ctx, cluster, fakeClient)
			Expect(status).To(Equal(framework.Running))
		})

		It("should return created status when not enqueued", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": "",
					},
				},
			}

			status, _ := rayClusterController.GetJobStatus(ctx, cluster, fakeClient)
			Expect(status).To(Equal(framework.Created))
		})

		It("should return queuing status when enqueued but not dequeued", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": time.Now().String(),
					},
				},
			}

			status, _ := rayClusterController.GetJobStatus(ctx, cluster, fakeClient)
			Expect(status).To(Equal(framework.Queuing))
		})

		It("should return pending status when dequeued but no worker ready", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": time.Now().String(),
						"koord-queue/job-dequeue-timestamp": time.Now().String(),
					},
				},
				Status: rayv1.RayClusterStatus{
					ReadyWorkerReplicas: 0,
				},
			}

			status, _ := rayClusterController.GetJobStatus(ctx, cluster, fakeClient)
			Expect(status).To(Equal(framework.Pending))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return false when owned by RayJob", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind: "RayJob",
						},
					},
				},
			}
			Expect(rayClusterController.ManagedByQueue(ctx, cluster)).To(BeFalse())
		})

		It("should return true when managing all jobs", func() {
			rayClusterController.managedAllJobs = true
			cluster := &rayv1.RayCluster{}
			Expect(rayClusterController.ManagedByQueue(ctx, cluster)).To(BeTrue())
		})

		It("should return false when originated from RayCluster CRD", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/originated-from-crd": "RayCluster",
					},
				},
			}
			Expect(rayClusterController.ManagedByQueue(ctx, cluster)).To(BeFalse())
		})

		It("should return true when enqueued", func() {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": "timestamp",
					},
				},
			}
			Expect(rayClusterController.ManagedByQueue(ctx, cluster)).To(BeTrue())
		})

		It("should return true when suspended", func() {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					Suspend: ptr.To(true),
				},
			}
			Expect(rayClusterController.ManagedByQueue(ctx, cluster)).To(BeTrue())
		})
	})
})
