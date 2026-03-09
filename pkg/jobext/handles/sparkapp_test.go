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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	kv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

var _ = Describe("SparkApplication Controller", func() {
	var (
		sparkController *SparkApplication
		fakeClient      client.Client
		ctx             context.Context
		scheme          *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		v1beta2.AddToScheme(scheme)
		schedulingv1.AddToScheme(scheme)
		v1.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		sparkController = &SparkApplication{
			c:              fakeClient,
			managedAllJobs: false,
			getPods: func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
				return []*v1.Pod{}, nil
			},
		}
	})

	Context("Object", func() {
		It("should return a SparkApplication object", func() {
			obj := sparkController.Object()
			Expect(obj).To(BeAssignableToTypeOf(&v1beta2.SparkApplication{}))
		})
	})

	Context("DeepCopy", func() {
		It("should create a deep copy of SparkApplication", func() {
			original := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.ScalaApplicationType,
				},
			}
			copy := sparkController.DeepCopy(original)
			Expect(copy).NotTo(BeIdenticalTo(original))
			Expect(copy).To(BeAssignableToTypeOf(&v1beta2.SparkApplication{}))

			copySpark := copy.(*v1beta2.SparkApplication)
			Expect(copySpark.Name).To(Equal(original.Name))
			Expect(copySpark.Namespace).To(Equal(original.Namespace))
			Expect(copySpark.Spec.Type).To(Equal(original.Spec.Type))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := sparkController.GVK()
			Expect(gvk).To(Equal(schema.GroupVersionKind{
				Group:   v1beta2.SchemeGroupVersion.Group,
				Version: v1beta2.SchemeGroupVersion.Version,
				Kind:    "SparkApplication",
			}))
		})
	})

	Context("GetPodSetName", func() {
		It("should return spark-role from pod labels", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"spark-role": "driver",
					},
				},
			}
			name := sparkController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("driver"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod sets for cluster mode", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.ClusterMode,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("2g"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To(int32(2)),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("1g"),
						},
					},
				},
			}

			podSets := sparkController.PodSet(ctx, job)
			Expect(podSets).To(HaveLen(2))

			// Check driver pod set
			var driverPodSet kueue.PodSet
			for _, ps := range podSets {
				if ps.Name == "driver" {
					driverPodSet = ps
					break
				}
			}
			Expect(driverPodSet.Name).To(Equal("driver"))
			Expect(driverPodSet.Count).To(Equal(int32(1)))

			// Check executor pod set
			var executorPodSet kueue.PodSet
			for _, ps := range podSets {
				if ps.Name == "executor" {
					executorPodSet = ps
					break
				}
			}
			Expect(executorPodSet.Name).To(Equal("executor"))
			Expect(executorPodSet.Count).To(Equal(int32(2)))
		})

		It("should generate only executor pod set for client mode", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.ClientMode,
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To(int32(3)),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("1g"),
						},
					},
				},
			}

			podSets := sparkController.PodSet(ctx, job)
			Expect(podSets).To(HaveLen(1))
			Expect(podSets[0].Name).To(Equal("executor"))
			Expect(podSets[0].Count).To(Equal(int32(3)))
		})
	})

	Context("Resources", func() {
		It("should calculate resources for cluster mode", func() {
			job := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.ClusterMode,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(2)),
							Memory: ptr.To("4g"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To(int32(2)),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("2g"),
						},
					},
				},
			}

			resources := sparkController.Resources(ctx, job)
			Expect(resources).NotTo(BeNil())
			Expect(resources).To(HaveKey(v1.ResourceCPU))
			Expect(resources).To(HaveKey(v1.ResourceMemory))

			// Expected: driver (2 cores) + 2 executors (1 core each) = 4 cores
			expectedCPU := resource.MustParse("4")
			Expect(resources[v1.ResourceCPU].Equal(expectedCPU)).To(BeTrue())

			// Expected: driver (4g) + 2 executors (2g each) = 8g
			expectedMemory := resource.MustParse("8Gi")
			Expect(resources[v1.ResourceMemory].Equal(expectedMemory)).To(BeTrue())
		})

		It("should calculate resources for client mode", func() {
			job := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.ClientMode,
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To(int32(3)),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(2)),
							Memory: ptr.To("2g"),
						},
					},
				},
			}

			resources := sparkController.Resources(ctx, job)
			Expect(resources).NotTo(BeNil())
			Expect(resources).To(HaveKey(v1.ResourceCPU))
			Expect(resources).To(HaveKey(v1.ResourceMemory))

			// Expected: 3 executors * 2 cores = 6 cores (no driver in client mode)
			expectedCPU := resource.MustParse("6")
			Expect(resources[v1.ResourceCPU].Equal(expectedCPU)).To(BeTrue())

			// Expected: 3 executors * 2g = 6g
			expectedMemory := resource.MustParse("6Gi")
			Expect(resources[v1.ResourceMemory].Equal(expectedMemory)).To(BeTrue())
		})
	})

	Context("QueueUnitSuffix", func() {
		It("should return correct suffix for non-PAI environment", func() {
			suffix := sparkController.QueueUnitSuffix()
			Expect(suffix).To(Equal("spark-qu"))
		})
	})

	Context("Priority", func() {
		It("should return empty priority when no priority class is set", func() {
			job := &v1beta2.SparkApplication{}
			priorityClassName, priority := sparkController.Priority(ctx, job)
			Expect(priorityClassName).To(BeEmpty())
			Expect(priority).To(BeNil())
		})

		It("should return priority when priority class exists", func() {
			priorityClass := &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "high-priority",
					Namespace: "default",
				},
				Value: 1000,
			}
			Expect(fakeClient.Create(ctx, priorityClass)).To(Succeed())

			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						PriorityClassName: ptr.To("high-priority"),
					},
				},
			}

			priorityClassName, priority := sparkController.Priority(ctx, job)
			Expect(priorityClassName).To(Equal("high-priority"))
			Expect(*priority).To(Equal(int32(1000)))
		})
	})

	Context("Enqueue", func() {
		It("should set enqueue timestamp and labels", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-spark",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Labels:      map[string]string{},
							Annotations: map[string]string{},
						},
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Labels:      map[string]string{},
							Annotations: map[string]string{},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := sparkController.Enqueue(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})

		It("should skip if already enqueued", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
					Annotations: map[string]string{
						"kube-queue/job-enqueue-timestamp": "already-set",
					},
				},
			}

			err := sparkController.Enqueue(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})
	})

	Context("Suspend", func() {
		It("should set queue annotation to true", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-spark",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
			}
			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := sparkController.Suspend(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})

		It("should skip if already suspended", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			err := sparkController.Suspend(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})
	})

	Context("Resume", func() {
		It("should set queue annotation to false for suspended job", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}
			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := sparkController.Resume(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})

		It("should skip if not suspended", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "false",
					},
				},
			}

			err := sparkController.Resume(ctx, job, fakeClient)
			Expect(err).To(Succeed())
		})
	})

	Context("GetJobStatus", func() {
		It("should return Succeeded status for completed applications", func() {
			terminationTime := metav1.NewTime(time.Now())
			job := &v1beta2.SparkApplication{
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.CompletedState,
					},
					TerminationTime: terminationTime,
				},
			}

			status, timestamp := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Succeeded))
			Expect(timestamp).To(Equal(terminationTime.Time))
		})

		It("should return Failed status for failed applications", func() {
			terminationTime := metav1.NewTime(time.Now())
			job := &v1beta2.SparkApplication{
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedState,
					},
					TerminationTime: terminationTime,
				},
			}

			status, timestamp := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Failed))
			Expect(timestamp).To(Equal(terminationTime.Time))
		})

		It("should return Running status for running applications", func() {
			submissionTime := metav1.NewTime(time.Now())
			job := &v1beta2.SparkApplication{
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.RunningState,
					},
					LastSubmissionAttemptTime: submissionTime,
				},
			}

			status, timestamp := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Running))
			Expect(timestamp).To(Equal(submissionTime.Time))
		})

		It("should return Pending status when not queued", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation:                    "false",
						"kube-queue/job-dequeue-timestamp": time.Now().Format(timeFormat),
					},
				},
			}

			status, _ := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Pending))
		})

		It("should return Queuing status when queued", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"kube-queue/job-enqueue-timestamp": time.Now().Format(timeFormat),
						QueueAnnotation:                    "true",
					},
				},
			}

			status, _ := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Queuing))
		})

		It("should return Created status for new applications", func() {
			creationTime := time.Now()
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(creationTime),
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			status, timestamp := sparkController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Created))
			Expect(timestamp).To(Equal(creationTime))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when managedAllJobs is true", func() {
			sparkController.managedAllJobs = true
			job := &v1beta2.SparkApplication{}

			managed := sparkController.ManagedByQueue(ctx, job)
			Expect(managed).To(BeTrue())
		})

		It("should return true when enqueue timestamp exists", func() {
			sparkController.managedAllJobs = false
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"kube-queue/job-enqueue-timestamp": "some-timestamp",
					},
				},
			}

			managed := sparkController.ManagedByQueue(ctx, job)
			Expect(managed).To(BeTrue())
		})

		It("should return true when application is new and queued", func() {
			sparkController.managedAllJobs = false
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: "",
					},
				},
			}

			managed := sparkController.ManagedByQueue(ctx, job)
			Expect(managed).To(BeTrue())
		})

		It("should return false when not managed", func() {
			sparkController.managedAllJobs = false
			job := &v1beta2.SparkApplication{}

			managed := sparkController.ManagedByQueue(ctx, job)
			Expect(managed).To(BeFalse())
		})
	})

	Context("GetPodsFunc", func() {
		It("should return the configured getPods function", func() {
			podsFunc := sparkController.GetPodsFunc()
			Expect(podsFunc).NotTo(BeNil())
		})
	})

	Context("Reservation", func() {
		It("should create reservations for driver and executors", func() {
			job := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("2g"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To(int32(2)),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To(int32(1)),
							Memory: ptr.To("1g"),
						},
					},
				},
			}

			reservations, err := sparkController.Reservation(ctx, job)
			Expect(err).To(Succeed())
			Expect(reservations).To(HaveLen(3)) // 1 driver + 2 executors

			// Verify driver reservation
			driverReservation := reservations[0]
			Expect(driverReservation.Name).To(Equal("test-spark-driver-0"))
			Expect(driverReservation.Spec.Owners).To(HaveLen(1))
			Expect(driverReservation.Spec.Owners[0].LabelSelector.MatchLabels).To(HaveKeyWithValue("spark-app-name", "test-spark"))
			Expect(driverReservation.Spec.Owners[0].LabelSelector.MatchLabels).To(HaveKeyWithValue("spark-role", "driver"))

			// Verify executor reservations
			for i := 1; i < 3; i++ {
				executorReservation := reservations[i]
				Expect(executorReservation.Name).To(Equal("test-spark-executor-" + string(rune('0'+i-1))))
			}
		})
	})

	Context("ReservationStatus", func() {
		It("should return reservation status", func() {
			job := &v1beta2.SparkApplication{}
			qu := &kv1alpha1.QueueUnit{}
			resvs := []koordinatorschedulerv1alpha1.Reservation{}

			status, message := sparkController.ReservationStatus(ctx, job, qu, resvs)
			Expect(status).NotTo(BeEmpty())
			Expect(message).To(BeEmpty())
		})
	})
})
