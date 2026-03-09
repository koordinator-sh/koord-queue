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

	mpiv1alpha1 "github.com/AliyunContainerService/mpi-operator/pkg/apis/kubeflow/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("MPIJobV1alpha1 Controller", func() {
	var (
		mpiJobV1alpha1Controller *MpiJobV1alpha1
		fakeClient               client.Client
		ctx                      context.Context
		scheme                   *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		schedulingv1.AddToScheme(scheme)
		// Adding the MPI v1alpha1 scheme
		mpiv1alpha1.AddToScheme(scheme)
		v1.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		mpiJobV1alpha1Controller = &MpiJobV1alpha1{
			managedAllJobs: false,
		}
	})

	Context("Object", func() {
		It("should return a MPIJob object", func() {
			obj := mpiJobV1alpha1Controller.Object()
			Expect(obj).To(BeAssignableToTypeOf(&mpiv1alpha1.MPIJob{}))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := mpiJobV1alpha1Controller.GVK()
			Expect(gvk.Kind).To(Equal(mpiv1alpha1.SchemeGroupVersionKind.Kind))
			Expect(gvk.Version).To(Equal(mpiv1alpha1.SchemeGroupVersion.Version))
			Expect(gvk.Group).To(Equal(mpiv1alpha1.SchemeGroupVersion.Group))
		})
	})

	Context("GetPodSetName", func() {
		It("should return owner name", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
			}
			name := mpiJobV1alpha1Controller.GetPodSetName("test-owner", pod)
			Expect(name).To(Equal("test-owner"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod sets", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-job",
				},
				Spec: mpiv1alpha1.MPIJobSpec{
					Replicas: ptr.To[int32](4),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "mpi-worker",
									Image: "mpi-worker:latest",
								},
							},
						},
					},
				},
			}

			podSets := mpiJobV1alpha1Controller.PodSet(ctx, job)
			Expect(podSets).To(HaveLen(1))
			Expect(podSets[0].Name).To(Equal("test-job"))
			Expect(podSets[0].Count).To(Equal(int32(4)))
		})
	})

	Context("Resources", func() {
		It("should calculate resources correctly", func() {
			job := &mpiv1alpha1.MPIJob{
				Spec: mpiv1alpha1.MPIJobSpec{
					Replicas: ptr.To[int32](4),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "mpi-worker",
									Image: "mpi-worker:latest",
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
			}

			resources := mpiJobV1alpha1Controller.Resources(ctx, job)
			Expect(resources).NotTo(BeNil())
			Expect(resources).To(HaveKey(v1.ResourceCPU))
			Expect(resources).To(HaveKey(v1.ResourceMemory))

			// Expected CPU: 4 workers * 1 CPU = 4 CPU
			// Expected Memory: 4 workers * 2Gi = 8Gi
			expectedCPU := resource.MustParse("4")
			expectedMemory := resource.MustParse("8Gi")
			Expect(resources[v1.ResourceCPU].Equal(expectedCPU)).To(BeTrue())
			Expect(resources[v1.ResourceMemory].Equal(expectedMemory)).To(BeTrue())
		})
	})

	Context("QueueUnitSuffix", func() {
		It("should return correct suffix when PAI_ENV is not set", func() {
			suffix := mpiJobV1alpha1Controller.QueueUnitSuffix()
			Expect(suffix).To(Equal("mpi-qu"))
		})
	})

	Context("Priority", func() {
		It("should return correct priority", func() {
			priority := int32(100)
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: mpiv1alpha1.MPIJobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							PriorityClassName: "test-priority-class",
							Priority:          &priority,
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

			className, priorityValue := mpiJobV1alpha1Controller.Priority(ctx, job)
			Expect(className).To(Equal("test-priority-class"))
			Expect(priorityValue).To(Equal(&priority))
		})
	})

	Context("Enqueue", func() {
		It("should not enqueue already enqueued job", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-has-enqueued": "true",
					},
				},
			}

			err := mpiJobV1alpha1Controller.Enqueue(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should enqueue job correctly", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: mpiv1alpha1.MPIJobSpec{
					Replicas: ptr.To[int32](1),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "mpi-worker",
									Image: "mpi-worker:latest",
								},
							},
						},
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := mpiJobV1alpha1Controller.Enqueue(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &mpiv1alpha1.MPIJob{}
			Expect(fakeClient.Get(ctx, client.ObjectKey{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())
			Expect(updatedJob.Annotations["koord-queue/job-has-enqueued"]).To(Equal("true"))
			Expect(updatedJob.Annotations["koord-queue/job-enqueue-timestamp"]).NotTo(BeEmpty())
		})
	})

	Context("Suspend", func() {
		It("should not suspend already suspended job", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			err := mpiJobV1alpha1Controller.Suspend(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should suspend job correctly", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: mpiv1alpha1.MPIJobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "mpi-worker",
									Image: "mpi-worker:latest",
								},
							},
						},
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := mpiJobV1alpha1Controller.Suspend(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &mpiv1alpha1.MPIJob{}
			Expect(fakeClient.Get(ctx, client.ObjectKey{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())
			Expect(updatedJob.Annotations[QueueAnnotation]).To(Equal("true"))
		})
	})

	Context("Resume", func() {
		It("should not resume already resumed job", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "false",
					},
				},
			}

			err := mpiJobV1alpha1Controller.Resume(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should resume job correctly", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := mpiJobV1alpha1Controller.Resume(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &mpiv1alpha1.MPIJob{}
			Expect(fakeClient.Get(ctx, client.ObjectKey{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())
			Expect(updatedJob.Annotations[QueueAnnotation]).To(Equal(""))
			Expect(updatedJob.Annotations["koord-queue/job-dequeue-timestamp"]).NotTo(BeEmpty())
		})
	})

	Context("GetJobStatus", func() {
		It("should return succeeded status", func() {
			now := metav1.Now()
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: mpiv1alpha1.LauncherSucceeded,
				},
			}

			status, _ := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Succeeded))
		})

		It("should return failed status", func() {
			now := metav1.Now()
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: mpiv1alpha1.LauncherFailed,
				},
			}

			status, _ := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Failed))
		})

		It("should return running status", func() {
			now := metav1.Now()
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: mpiv1alpha1.LauncherActive,
				},
			}

			status, _ := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Running))
		})

		It("should return pending status", func() {
			now := metav1.Now()
			enqueueTime := now.Time.Add(-time.Hour).Format(timeFormat)
			dequeueTime := now.Time.Add(-time.Minute).Format(timeFormat)

			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-enqueue-timestamp": enqueueTime,
						"koord-queue/job-dequeue-timestamp": dequeueTime,
						QueueAnnotation:                    "false",
					},
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: "",
				},
			}

			status, _ := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Pending))
		})

		It("should return queuing status", func() {
			now := metav1.Now()
			enqueueTime := now.Time.Add(-time.Hour).Format(timeFormat)

			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-has-enqueued":      "true",
						"koord-queue/job-enqueue-timestamp": enqueueTime,
						QueueAnnotation:                    "true",
					},
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: "",
				},
			}

			status, _ := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Queuing))
		})

		It("should return created status", func() {
			now := metav1.Now()
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					CreationTimestamp: now,
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: "",
				},
			}

			status, timestamp := mpiJobV1alpha1Controller.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Created))
			Expect(timestamp).To(Equal(now.Time))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when managing all jobs", func() {
			mpiJobV1alpha1Controller.managedAllJobs = true
			job := &mpiv1alpha1.MPIJob{}
			Expect(mpiJobV1alpha1Controller.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return true when job has enqueue annotation", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-has-enqueued": "true",
					},
				},
			}
			Expect(mpiJobV1alpha1Controller.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return true when no launcher status and suspended", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: "",
				},
			}
			Expect(mpiJobV1alpha1Controller.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return false when has launcher status", func() {
			job := &mpiv1alpha1.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
				Status: mpiv1alpha1.MPIJobStatus{
					LauncherStatus: mpiv1alpha1.LauncherActive,
				},
			}
			Expect(mpiJobV1alpha1Controller.ManagedByQueue(ctx, job)).To(BeFalse())
		})
	})
})