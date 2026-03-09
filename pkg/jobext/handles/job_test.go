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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
)

var _ = Describe("Job Controller", func() {
	var (
		jobController *Job
		fakeClient    client.Client
		ctx           context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = fake.NewClientBuilder().Build()
		jobController = &Job{
			podlister:      fakeClient,
			managedAllJobs: false,
		}
	})

	Context("Object", func() {
		It("should return a Job object", func() {
			obj := jobController.Object()
			Expect(obj).To(BeAssignableToTypeOf(&batchv1.Job{}))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := jobController.GVK()
			Expect(gvk.Kind).To(Equal("Job"))
			Expect(gvk.Version).To(Equal("v1"))
			Expect(gvk.Group).To(Equal("batch"))
		})
	})

	Context("Resources", func() {
		It("should calculate resources correctly", func() {
			job := &batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism: ptr.To[int32](2),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("1Gi"),
										},
									},
								},
							},
						},
					},
				},
			}

			resources := jobController.Resources(ctx, job)
			Expect(resources).NotTo(BeNil())
		})
	})

	Context("GetPodSetName", func() {
		It("should return correct pod set name", func() {
			name := jobController.GetPodSetName("owner", &v1.Pod{})
			Expect(name).To(Equal("owner"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod set", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-job",
				},
				Spec: batchv1.JobSpec{
					Parallelism: ptr.To[int32](2),
					Completions: ptr.To[int32](4),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
						},
					},
				},
				Status: batchv1.JobStatus{
					Succeeded: 1,
				},
			}

			podSets := jobController.PodSet(ctx, job)
			Expect(podSets).To(HaveLen(1))
			Expect(podSets[0].Name).To(Equal("test-job"))
			Expect(podSets[0].Count).To(Equal(int32(2)))
		})
	})

	Context("Priority", func() {
		It("should return correct priority", func() {
			priority := int32(100)
			job := &batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							PriorityClassName: "test-priority-class",
							Priority:          &priority,
						},
					},
				},
			}

			className, priorityValue := jobController.Priority(ctx, job)
			Expect(className).To(Equal("test-priority-class"))
			Expect(priorityValue).To(Equal(&priority))
		})
	})

	Context("Suspend", func() {
		It("should suspend job correctly", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Suspend: ptr.To(false),
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())
			err := jobController.Suspend(ctx, job, fakeClient)
			Expect(fakeClient.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
			Expect(err).To(BeNil())
			Expect(job.Spec.Suspend).To(Equal(ptr.To(true)))
		})

		It("should not suspend already suspended job", func() {
			job := &batchv1.Job{
				Spec: batchv1.JobSpec{
					Suspend: ptr.To(true),
				},
			}

			err := jobController.Suspend(ctx, job, fakeClient)
			Expect(err).To(BeNil())
			Expect(job.Spec.Suspend).To(Equal(ptr.To(true)))
		})
	})

	Context("Resume", func() {
		It("should resume suspended job", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Suspend: ptr.To(true),
				},
			}

			_ = fakeClient.Create(ctx, job)
			err := jobController.Resume(ctx, job, fakeClient)
			Expect(err).To(BeNil())
			_ = fakeClient.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, job)
			Expect(job.Spec.Suspend).To(Equal(ptr.To(false)))
			Expect(job.Annotations["koord-queue/job-dequeue-timestamp"]).NotTo(BeEmpty())
		})

		It("should not resume already running job", func() {
			job := &batchv1.Job{
				Spec: batchv1.JobSpec{
					Suspend: ptr.To(false),
				},
			}

			err := jobController.Resume(ctx, job, fakeClient)
			Expect(err).To(BeNil())
			Expect(job.Spec.Suspend).To(Equal(ptr.To(false)))
		})
	})

	Context("GetJobStatus", func() {
		It("should return correct status for completed job", func() {
			now := metav1.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:               batchv1.JobComplete,
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := jobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Succeeded))
			Expect(timestamp).To(Equal(now.Time))
		})

		It("should return correct status for failed job", func() {
			now := metav1.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:               batchv1.JobFailed,
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := jobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Failed))
			Expect(timestamp).To(Equal(now.Time))
		})

		It("should return queuing status for enqueued job", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						"koord-queue/job-has-enqueued": "true",
					},
				},
			}

			status, _ := jobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Queuing))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when managing all jobs", func() {
			jobController.managedAllJobs = true
			job := &batchv1.Job{}
			Expect(jobController.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return true when job is enqueued", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"koord-queue/job-has-enqueued": "true",
					},
				},
			}
			Expect(jobController.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return false when job has started", func() {
			now := metav1.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Suspend: ptr.To(true),
				},
				Status: batchv1.JobStatus{
					StartTime: &now,
				},
			}
			Expect(jobController.ManagedByQueue(ctx, job)).To(BeFalse())
		})
	})

	Context("QueueUnitSuffix", func() {
		It("should return empty string", func() {
			Expect(jobController.QueueUnitSuffix()).To(Equal(""))
		})
	})
})
