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
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var _ = Describe("RayJob Controller", func() {
	var (
		rayJobController *RayJob
		fakeClient       client.Client
		ctx              context.Context
		scheme           *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		rayv1.AddToScheme(scheme)
		schedulingv1.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		rayJobController = &RayJob{
			c:              fakeClient,
			managedAllJobs: false,
		}
	})

	Context("Object", func() {
		It("should return a RayJob object", func() {
			obj := rayJobController.Object()
			Expect(obj).To(BeAssignableToTypeOf(&rayv1.RayJob{}))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := rayJobController.GVK()
			Expect(gvk.Kind).To(Equal("RayJob"))
			Expect(gvk.Version).To(Equal(rayv1.GroupVersion.Version))
			Expect(gvk.Group).To(Equal(rayv1.GroupVersion.Group))
		})
	})

	Context("DeepCopy", func() {
		It("should deep copy a RayJob object", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
			}
			copied := rayJobController.DeepCopy(rayJob)
			Expect(copied).To(BeAssignableToTypeOf(&rayv1.RayJob{}))
			Expect(copied.GetName()).To(Equal(rayJob.GetName()))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when job has enqueue timestamp annotation", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": time.Now().String(),
					},
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeTrue())
		})

		It("should return true when job has no start time and is suspended", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeTrue())
		})

		It("should return false when job has start time", func() {
			now := metav1.Now()
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
				Status: rayv1.RayJobStatus{
					StartTime: &now,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeFalse())
		})

		It("should return false for non-RayJob object", func() {
			job := &batchv1.Job{}
			managed := rayJobController.ManagedByQueue(ctx, job)
			Expect(managed).To(BeFalse())
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
			name := rayJobController.GetPodSetName("owner", pod)
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
			name := rayJobController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("worker-group-1"))
		})

		It("should return worker for pods without group label (PAI case)", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			}
			name := rayJobController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("worker"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod sets", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "worker-group-1",
								Replicas:  ptr.To[int32](2),
								Template: v1.PodTemplateSpec{
									Spec: v1.PodSpec{
										Containers: []v1.Container{
											{
												Name:  "worker",
												Image: "rayproject/ray:latest",
											},
										},
									},
								},
							},
						},
					},
				},
			}

			podSets := rayJobController.PodSet(ctx, rayJob)
			Expect(podSets).To(HaveLen(2))
			Expect(podSets[0].Name).To(Equal("head"))
			Expect(podSets[0].Count).To(Equal(int32(1)))
			Expect(podSets[1].Name).To(Equal("worker-group-1"))
			Expect(podSets[1].Count).To(Equal(int32(2)))
		})

		It("should generate correct pod sets with empty group name", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "",
								Replicas:  ptr.To[int32](1),
								Template: v1.PodTemplateSpec{
									Spec: v1.PodSpec{
										Containers: []v1.Container{
											{
												Name:  "worker",
												Image: "rayproject/ray:latest",
											},
										},
									},
								},
							},
						},
					},
				},
			}

			podSets := rayJobController.PodSet(ctx, rayJob)
			Expect(podSets).To(HaveLen(2))
			Expect(podSets[0].Name).To(Equal("head"))
			Expect(podSets[0].Count).To(Equal(int32(1)))
			Expect(podSets[1].Name).To(Equal("worker"))
			Expect(podSets[1].Count).To(Equal(int32(1)))
		})

		It("should return empty pod set for non-RayJob object", func() {
			job := &batchv1.Job{}
			podSets := rayJobController.PodSet(ctx, job)
			Expect(podSets).To(BeEmpty())
		})
	})

	Context("Resources", func() {
		It("should calculate total resources correctly", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
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
												Name:  "worker",
												Image: "rayproject/ray:latest",
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
				},
			}

			resources := rayJobController.Resources(ctx, rayJob)
			Expect(resources).To(HaveLen(2))
			Expect(resources.Cpu().String()).To(Equal("5"))
			Expect(resources.Memory().String()).To(Equal("10Gi"))
		})
	})

	Context("QueueUnitSuffix", func() {
		When("PAI_ENV is not set", func() {
			It("should return ray-qu suffix", func() {
				os.Unsetenv("PAI_ENV")
				suffix := rayJobController.QueueUnitSuffix()
				Expect(suffix).To(Equal("ray-qu"))
			})
		})

		When("PAI_ENV is set", func() {
			BeforeEach(func() {
				os.Setenv("PAI_ENV", "true")
			})

			AfterEach(func() {
				os.Unsetenv("PAI_ENV")
			})

			It("should return empty suffix", func() {
				suffix := rayJobController.QueueUnitSuffix()
				Expect(suffix).To(Equal(""))
			})
		})
	})

	Context("Priority", func() {
		It("should return empty priority when no priority class is set", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{},
							},
						},
					},
				},
			}

			className, priority := rayJobController.Priority(ctx, rayJob)
			Expect(className).To(Equal(""))
			Expect(priority).To(BeNil())
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when job has enqueue timestamp annotation", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": time.Now().String(),
					},
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeTrue())
		})

		It("should return true when job has no start time and is suspended", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeTrue())
		})

		It("should return false when job has start time", func() {
			now := metav1.Now()
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
				Status: rayv1.RayJobStatus{
					StartTime: &now,
				},
			}

			managed := rayJobController.ManagedByQueue(ctx, rayJob)
			Expect(managed).To(BeFalse())
		})
	})

	Context("genPsName", func() {
		It("should generate correct pod set name", func() {
			Expect(genPsName("WorkerGroup")).To(Equal("workergroup"))
			Expect(genPsName("")).To(Equal("worker"))
			Expect(genPsName("GROUP")).To(Equal("group"))
		})
	})

	Context("Suspend", func() {
		It("should suspend a RayJob", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Suspend: false,
				},
			}

			Expect(fakeClient.Create(ctx, rayJob)).To(Succeed())
			err := rayJobController.Suspend(ctx, rayJob, fakeClient)
			Expect(fakeClient.Get(ctx, types.NamespacedName{Namespace: rayJob.Namespace, Name: rayJob.Name}, rayJob)).To(Succeed())
			Expect(err).To(BeNil())
			Expect(rayJob.Spec.Suspend).To(BeTrue())
		})

		It("should not suspend an already suspended RayJob", func() {
			rayJob := &rayv1.RayJob{
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			err := rayJobController.Suspend(ctx, rayJob, fakeClient)
			Expect(err).To(BeNil())
			Expect(rayJob.Spec.Suspend).To(BeTrue())
		})
	})

	Context("Resume", func() {
		It("should resume a suspended RayJob", func() {
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-dequeue-timestamp": time.Now().String(),
					},
				},
				Spec: rayv1.RayJobSpec{
					Suspend: true,
				},
			}

			fakeClient.Create(ctx, rayJob)
			err := rayJobController.Resume(ctx, rayJob, fakeClient)
			_ = fakeClient.Get(ctx, types.NamespacedName{Namespace: rayJob.Namespace, Name: rayJob.Name}, rayJob)
			Expect(err).To(BeNil())
			Expect(rayJob.Spec.Suspend).To(BeFalse())
			Expect(rayJob.Annotations["koord-queue/job-dequeue-timestamp"]).NotTo(BeEmpty())
		})

		It("should not resume an already running RayJob", func() {
			rayJob := &rayv1.RayJob{
				Spec: rayv1.RayJobSpec{
					Suspend: false,
				},
			}

			err := rayJobController.Resume(ctx, rayJob, fakeClient)
			Expect(err).To(BeNil())
			Expect(rayJob.Spec.Suspend).To(BeFalse())
		})
	})

	Context("GetJobStatus", func() {
		var (
			rayJob *rayv1.RayJob
		)

		BeforeEach(func() {
			rayJob = &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{},
				},
			}
		})

		It("should return Succeeded when job deployment status is complete", func() {
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusComplete
			rayJob.Status.EndTime = &metav1.Time{Time: time.Now()}

			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Succeeded))
			Expect(timestamp).To(Equal(rayJob.Status.EndTime.Time))
		})

		It("should return Failed when job deployment status is failed", func() {
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
			rayJob.Status.EndTime = &metav1.Time{Time: time.Now()}

			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Failed))
			Expect(timestamp).To(Equal(rayJob.Status.EndTime.Time))
		})

		It("should return Running when job deployment status is running", func() {
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRunning
			rayJob.Status.StartTime = &metav1.Time{Time: time.Now()}

			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Running))
			Expect(timestamp).To(Equal(rayJob.Status.StartTime.Time))
		})

		It("should return Pending when job has dequeue timestamp", func() {
			rayJob.Annotations = map[string]string{
				"koord-queue/job-dequeue-timestamp": time.Now().String(),
				"koord-queue/job-enqueue-timestamp": time.Now().String(),
			}

			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Pending))

			_, err := time.Parse(timeFormat, rayJob.Annotations["koord-queue/job-enqueue-timestamp"])
			if err == nil {
				// If parsing succeeds, timestamp should match
				parsedTime, _ := time.Parse(timeFormat, rayJob.Annotations["koord-queue/job-enqueue-timestamp"])
				Expect(timestamp).To(Equal(parsedTime))
			}
		})

		It("should return Queuing when job has enqueue timestamp but no dequeue timestamp", func() {
			rayJob.Annotations = map[string]string{
				"koord-queue/job-enqueue-timestamp": time.Now().String(),
			}

			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Queuing))

			_, err := time.Parse(timeFormat, rayJob.Annotations["koord-queue/job-enqueue-timestamp"])
			if err == nil {
				// If parsing succeeds, timestamp should match
				parsedTime, _ := time.Parse(timeFormat, rayJob.Annotations["koord-queue/job-enqueue-timestamp"])
				Expect(timestamp).To(Equal(parsedTime))
			} else {
				// If parsing fails, should be current time
				Expect(timestamp).To(BeTemporally("~", time.Now(), time.Second))
			}
		})

		It("should return Created when job has no queue timestamps", func() {
			status, timestamp := rayJobController.GetJobStatus(ctx, rayJob, fakeClient)
			Expect(status).To(Equal(framework.Created))
			Expect(timestamp).To(Equal(rayJob.CreationTimestamp.Time))
		})
	})

	Context("Enqueue", func() {
		var (
			rayJob *rayv1.RayJob
		)

		BeforeEach(func() {
			rayJob = &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "worker-group-1",
								Template: v1.PodTemplateSpec{
									Spec: v1.PodSpec{
										Containers: []v1.Container{
											{
												Name:  "worker",
												Image: "rayproject/ray:latest",
											},
										},
									},
								},
							},
						},
					},
				},
			}
		})

		It("should not enqueue an already enqueued job", func() {
			rayJob.Annotations = map[string]string{
				"koord-queue/job-enqueue-timestamp": time.Now().String(),
			}

			err := rayJobController.Enqueue(ctx, rayJob, fakeClient)
			Expect(err).To(BeNil())
		})

		It("should add enqueue annotations and modify templates", func() {
			Expect(fakeClient.Create(ctx, rayJob)).To(Succeed())

			err := rayJobController.Enqueue(ctx, rayJob, fakeClient)
			Expect(err).To(BeNil())

			updatedRayJob := &rayv1.RayJob{}
			getErr := fakeClient.Get(ctx, types.NamespacedName{
				Name:      "test-rayjob",
				Namespace: "default",
			}, updatedRayJob)
			Expect(getErr).To(BeNil())

			Expect(updatedRayJob.Annotations["koord-queue/job-enqueue-timestamp"]).ToNot(BeEmpty())
			Expect(updatedRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedAPIVersionKindAnnoKey]).To(Equal("ray.io/v1/RayJob"))
			Expect(updatedRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedObjectAnnoKey]).To(Equal(rayJob.Name))
			Expect(updatedRayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.Annotations[util.RelatedAPIVersionKindAnnoKey]).To(Equal("ray.io/v1/RayJob"))
			Expect(updatedRayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.Annotations[util.RelatedObjectAnnoKey]).To(Equal(rayJob.Name))
		})
	})

	Context("Resources", func() {
		var (
			rayJob *rayv1.RayJob
		)

		BeforeEach(func() {
			rayJob = &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
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
												Name:  "worker",
												Image: "rayproject/ray:latest",
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
				},
			}
		})

		It("should calculate total resources correctly", func() {
			resources := rayJobController.Resources(ctx, rayJob)
			Expect(resources).To(HaveLen(2))
			Expect(resources.Cpu().String()).To(Equal("5"))
			Expect(resources.Memory().String()).To(Equal("10Gi"))
		})

		It("should return empty resources for non-RayJob object", func() {
			job := &batchv1.Job{}
			resources := rayJobController.Resources(ctx, job)
			Expect(resources).To(BeEmpty())
		})
	})

	Context("Priority", func() {
		var (
			rayJob *rayv1.RayJob
		)

		BeforeEach(func() {
			rayJob = &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
			}
		})

		It("should return empty priority when no priority class is set", func() {
			className, priority := rayJobController.Priority(ctx, rayJob)
			Expect(className).To(Equal(""))
			Expect(priority).To(BeNil())
		})

		It("should return priority class name and value when priority class exists", func() {
			// Create a priority class
			priorityClass := &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-priority-class",
				},
				Value: 1000,
			}
			Expect(fakeClient.Create(ctx, priorityClass)).To(Succeed())

			// Set priority class name in ray job
			rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.PriorityClassName = "test-priority-class"

			className, priority := rayJobController.Priority(ctx, rayJob)
			Expect(className).To(Equal("test-priority-class"))
			Expect(priority).ToNot(BeNil())
			Expect(*priority).To(Equal(int32(1000)))
		})

		It("should return priority class name and nil when priority class does not exist", func() {
			// Set priority class name in ray job but don't create the priority class
			rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.PriorityClassName = "nonexistent-priority-class"

			className, priority := rayJobController.Priority(ctx, rayJob)
			Expect(className).To(Equal("nonexistent-priority-class"))
			Expect(priority).To(BeNil())
		})

		It("should return empty values for non-RayJob object", func() {
			job := &batchv1.Job{}
			className, priority := rayJobController.Priority(ctx, job)
			Expect(className).To(Equal(""))
			Expect(priority).To(BeNil())
		})
	})
})
