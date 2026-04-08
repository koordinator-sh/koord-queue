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

	commonv1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/common/job_controller/v1"
	pytorchv1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/pytorch/v1"
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
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

var _ = Describe("PytorchJob Controller", func() {
	var (
		pytorchJobController *PytorchJob
		fakeClient           client.Client
		ctx                  context.Context
		scheme               *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		schedulingv1.AddToScheme(scheme)
		pytorchv1.AddToScheme(scheme)
		v1.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&pytorchv1.PyTorchJob{}).Build()
		pytorchJobController = &PytorchJob{
			c:              fakeClient,
			managedAllJobs: false,
		}
	})

	Context("Object", func() {
		It("should return a PyTorchJob object", func() {
			obj := pytorchJobController.Object()
			Expect(obj).To(BeAssignableToTypeOf(&pytorchv1.PyTorchJob{}))
		})
	})

	Context("GVK", func() {
		It("should return correct GroupVersionKind", func() {
			gvk := pytorchJobController.GVK()
			Expect(gvk.Kind).To(Equal(pytorchv1.Kind))
			Expect(gvk.Version).To(Equal(pytorchv1.SchemeGroupVersion.Version))
			Expect(gvk.Group).To(Equal(pytorchv1.GroupName))
		})
	})

	Context("GetPodSetName", func() {
		It("should return correct pod set name from pod labels", func() {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"job-role": "master",
					},
				},
			}
			name := pytorchJobController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("master"))
			pod = &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"pytorch-replica-type": "worker",
					},
				},
			}
			name = pytorchJobController.GetPodSetName("owner", pod)
			Expect(name).To(Equal("worker"))
		})
	})

	Context("PodSet", func() {
		It("should generate correct pod sets", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeWorker: {
							Replicas: ptr.To[int32](2),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
										},
									},
								},
							},
						},
						pytorchv1.PyTorchReplicaTypeMaster: {
							Replicas: ptr.To[int32](1),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
										},
									},
								},
							},
						},
					},
				},
			}

			podSets := pytorchJobController.PodSet(ctx, job)
			Expect(podSets).To(HaveLen(2))

			// Check worker pod set
			var workerPodSet kueue.PodSet
			for _, ps := range podSets {
				if ps.Name == "worker" {
					workerPodSet = ps
					break
				}
			}
			Expect(workerPodSet.Name).To(Equal("worker"))
			Expect(workerPodSet.Count).To(Equal(int32(2)))

			// Check master pod set
			var masterPodSet kueue.PodSet
			for _, ps := range podSets {
				if ps.Name == "master" {
					masterPodSet = ps
					break
				}
			}
			Expect(masterPodSet.Name).To(Equal("master"))
			Expect(masterPodSet.Count).To(Equal(int32(1)))
		})
	})

	Context("Resources", func() {
		It("should calculate resources correctly", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeWorker: {
							Replicas: ptr.To[int32](2),
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
						pytorchv1.PyTorchReplicaTypeMaster: {
							Replicas: ptr.To[int32](1),
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
					},
				},
			}

			resources := pytorchJobController.Resources(ctx, job)
			Expect(resources).NotTo(BeNil())
			Expect(resources).To(HaveKey(v1.ResourceCPU))
			Expect(resources).To(HaveKey(v1.ResourceMemory))

			// Expected: 2 workers * 1 CPU + 1 master * 2 CPU = 4 CPU
			// Expected: 2 workers * 2Gi + 1 master * 4Gi = 8Gi
			expectedCPU := resource.MustParse("4")
			expectedMemory := resource.MustParse("8Gi")
			Expect(resources[v1.ResourceCPU].Equal(expectedCPU)).To(BeTrue())
			Expect(resources[v1.ResourceMemory].Equal(expectedMemory)).To(BeTrue())
		})
	})

	Context("Priority", func() {
		It("should return correct priority", func() {
			priority := int32(100)
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeWorker: {
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									PriorityClassName: "test-priority-class",
									Priority:          &priority,
								},
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
			className, priorityValue := pytorchJobController.Priority(ctx, job)
			Expect(className).To(Equal("test-priority-class"))
			Expect(priorityValue).To(Equal(&priority))
		})
	})

	Context("QueueUnitSuffix", func() {
		It("should return correct suffix when PAI_ENV is not set", func() {
			suffix := pytorchJobController.QueueUnitSuffix()
			Expect(suffix).To(Equal("py-qu"))
		})
	})

	Context("ManagedByQueue", func() {
		It("should return true when managing all jobs", func() {
			pytorchJobController.managedAllJobs = true
			job := &pytorchv1.PyTorchJob{}
			Expect(pytorchJobController.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return true when job has queuing condition", func() {
			job := &pytorchv1.PyTorchJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:   "Queuing",
							Status: v1.ConditionTrue,
						},
					},
				},
			}
			Expect(pytorchJobController.ManagedByQueue(ctx, job)).To(BeTrue())
		})

		It("should return false when job has no queuing condition and start time", func() {
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation: "false",
					},
				},
			}
			Expect(pytorchJobController.ManagedByQueue(ctx, job)).To(BeFalse())
		})
	})

	Context("GetJobStatus", func() {
		It("should return succeeded status", func() {
			now := metav1.Now()
			job := &pytorchv1.PyTorchJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:               commonv1.JobSucceeded,
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := pytorchJobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Succeeded))
			Expect(timestamp).To(Equal(now.Time))
		})

		It("should return failed status", func() {
			now := metav1.Now()
			job := &pytorchv1.PyTorchJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:               commonv1.JobFailed,
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := pytorchJobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Failed))
			Expect(timestamp).To(Equal(now.Time))
		})

		It("should return running status", func() {
			now := metav1.Now()
			job := &pytorchv1.PyTorchJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:               commonv1.JobRunning,
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := pytorchJobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Running))
			Expect(timestamp).To(Equal(now.Time))
		})

		It("should return queuing status", func() {
			now := metav1.Now()
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:               "Queuing",
							Status:             v1.ConditionTrue,
							LastTransitionTime: now,
						},
					},
				},
			}

			status, timestamp := pytorchJobController.GetJobStatus(ctx, job, fakeClient)
			Expect(status).To(Equal(framework.Queuing))
			Expect(timestamp).To(Equal(now.Time))
		})
	})

	Context("Reservation", func() {
		It("should generate reservations correctly", func() {
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeWorker: {
							Replicas: ptr.To[int32](2),
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"network-topology-job-name":      "test-net-topo",
										"network-topology-job-namespace": "test-ns",
									},
								},
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
											Resources: v1.ResourceRequirements{
												Requests: v1.ResourceList{
													v1.ResourceCPU: resource.MustParse("1"),
												},
											},
										},
									},
								},
							},
						},
						pytorchv1.PyTorchReplicaTypeMaster: {
							Replicas: ptr.To[int32](1),
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"network-topology-job-name":      "test-net-topo",
										"network-topology-job-namespace": "test-ns",
									},
								},
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
											Resources: v1.ResourceRequirements{
												Requests: v1.ResourceList{
													v1.ResourceCPU: resource.MustParse("2"),
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

			reservations, err := pytorchJobController.Reservation(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			Expect(reservations).To(HaveLen(3)) // 2 workers + 1 master

			// Check worker reservations
			workerReservations := 0
			masterReservations := 0
			for _, resv := range reservations {
				Expect(resv.Labels).To(HaveKeyWithValue("network-topology-job-name", "test-net-topo-py-qu"))
				Expect(resv.Labels).To(HaveKeyWithValue("network-topology-job-namespace", "test-ns"))
				Expect(resv.Spec.Owners).To(HaveLen(1))
				Expect(resv.Spec.Template.Spec.Containers).To(HaveLen(1))
				Expect(resv.Spec.Template.Spec.Containers[0].Resources.Requests).To(HaveKey(v1.ResourceCPU))

				if resv.Name == "test-job-worker-0" || resv.Name == "test-job-worker-1" {
					workerReservations++
					Expect(resv.Spec.Owners[0].LabelSelector.MatchExpressions).To(ContainElement(
						metav1.LabelSelectorRequirement{
							Key:      "pytorch-replica-type",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{"master"},
						}))
					Expect(resv.Spec.Owners[0].LabelSelector.MatchExpressions).To(ContainElement(
						metav1.LabelSelectorRequirement{
							Key:      "job-name",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"test-job"},
						}))
				} else if resv.Name == "test-job-master-0" {
					masterReservations++
					Expect(resv.Spec.Owners[0].LabelSelector.MatchExpressions).To(ContainElement(
						metav1.LabelSelectorRequirement{
							Key:      "pytorch-replica-type",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{"master"},
						}))
					Expect(resv.Spec.Owners[0].LabelSelector.MatchExpressions).To(ContainElement(
						metav1.LabelSelectorRequirement{
							Key:      "job-name",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"test-job"},
						}))
				}
			}
			Expect(workerReservations).To(Equal(2))
			Expect(masterReservations).To(Equal(1))
		})

		It("should handle AIMaster role correctly", func() {
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						"AIMaster": {
							Replicas: ptr.To[int32](1),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
										},
									},
								},
							},
						},
					},
				},
			}

			reservations, err := pytorchJobController.Reservation(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			Expect(reservations).To(HaveLen(0)) // AIMaster should be skipped
		})
	})

	Context("GetNetworkTopologyNamespaceName", func() {
		It("should return empty strings for non-PyTorchJob object", func() {
			obj := &v1.Pod{}
			ns, name := pytorchJobController.GetNetworkTopologyNamespaceName(ctx, obj)
			Expect(ns).To(BeEmpty())
			Expect(name).To(BeEmpty())
		})

		It("should return empty strings when master template is nil", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{},
				},
			}
			ns, name := pytorchJobController.GetNetworkTopologyNamespaceName(ctx, job)
			Expect(ns).To(BeEmpty())
			Expect(name).To(BeEmpty())
		})

		It("should return empty strings when network topology name is empty", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeMaster: {
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"network-topology-job-namespace": "test-ns",
										// network-topology-job-name is missing
									},
								},
							},
						},
					},
				},
			}
			ns, name := pytorchJobController.GetNetworkTopologyNamespaceName(ctx, job)
			Expect(ns).To(BeEmpty())
			Expect(name).To(BeEmpty())
		})

		It("should return correct namespace and name", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeMaster: {
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"network-topology-job-namespace": "test-ns",
										"network-topology-job-name":      "test-name",
									},
								},
							},
						},
					},
				},
			}
			ns, name := pytorchJobController.GetNetworkTopologyNamespaceName(ctx, job)
			Expect(ns).To(Equal("test-ns"))
			Expect(name).To(Equal("test-name"))
		})
	})

	Context("GetJobNetworkTopologyCR", func() {
		It("should return nil when network topology name is empty", func() {
			job := &pytorchv1.PyTorchJob{
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeMaster: {
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"network-topology-job-namespace": "test-ns",
										// network-topology-job-name is missing
									},
								},
							},
						},
					},
				},
			}
			jnt, err := pytorchJobController.GetJobNetworkTopologyCR(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
			Expect(jnt).To(BeNil())
		})

		// Cannot test successful case without importing network topology CRD
	})

	Context("Enqueue", func() {
		It("should enqueue job correctly", func() {
			job := &pytorchv1.PyTorchJob{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PyTorchJob",
					APIVersion: "kubeflow.org/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: pytorchv1.PyTorchJobSpec{
					PyTorchReplicaSpecs: map[pytorchv1.PyTorchReplicaType]*commonv1.ReplicaSpec{
						pytorchv1.PyTorchReplicaTypeWorker: {
							Replicas: ptr.To[int32](1),
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "pytorch",
											Image: "pytorch:latest",
										},
									},
								},
							},
						},
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := pytorchJobController.Enqueue(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &pytorchv1.PyTorchJob{}
			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())

			// Check that a Queuing condition was added
			hasQueuingCondition := false
			for _, condition := range updatedJob.Status.Conditions {
				if condition.Type == "Queuing" && condition.Status == v1.ConditionTrue {
					hasQueuingCondition = true
					break
				}
			}
			Expect(hasQueuingCondition).To(BeTrue())
		})
	})

	Context("Suspend", func() {
		It("should not suspend already suspended job", func() {
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			err := pytorchJobController.Suspend(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should suspend job correctly", func() {
			job := &pytorchv1.PyTorchJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := pytorchJobController.Suspend(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &pytorchv1.PyTorchJob{}
			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())
			Expect(updatedJob.Annotations[QueueAnnotation]).To(Equal("true"))
		})
	})

	Context("GenLabels", func() {
		It("should generate correct labels", func() {
			labels := pytorchJobController.GenLabels("test-job")
			Expect(labels).To(HaveKeyWithValue("group-name", "kubeflow.org"))
			Expect(labels).To(HaveKeyWithValue("job-name", "test-job"))
		})

		It("should handle job names with slashes", func() {
			labels := pytorchJobController.GenLabels("test/job")
			Expect(labels).To(HaveKeyWithValue("group-name", "kubeflow.org"))
			Expect(labels).To(HaveKeyWithValue("job-name", "test-job"))
		})
	})

	Context("FilterServicesForReplicaType", func() {
		It("should filter services correctly", func() {
			services := []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "master-service",
						Labels: map[string]string{
							"pytorch-replica-type": "master",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker-service",
						Labels: map[string]string{
							"pytorch-replica-type": "worker",
						},
					},
				},
			}

			filteredServices, err := pytorchJobController.FilterServicesForReplicaType(services, "master")
			Expect(err).NotTo(HaveOccurred())
			Expect(filteredServices).To(HaveLen(1))
			Expect(filteredServices[0].Name).To(Equal("master-service"))
		})
	})

	Context("Resume", func() {
		It("should not resume already resumed job", func() {
			job := &pytorchv1.PyTorchJob{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PyTorchJob",
					APIVersion: "kubeflow.org/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "false",
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())
			err := pytorchJobController.Resume(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should resume job correctly", func() {
			job := &pytorchv1.PyTorchJob{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PyTorchJob",
					APIVersion: "kubeflow.org/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						QueueAnnotation: "true",
					},
				},
			}

			Expect(fakeClient.Create(ctx, job)).To(Succeed())

			err := pytorchJobController.Resume(ctx, job, fakeClient)
			Expect(err).NotTo(HaveOccurred())

			// Check that the job was updated
			updatedJob := &pytorchv1.PyTorchJob{}
			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: "test-job", Namespace: "default"}, updatedJob)).To(Succeed())
			Expect(updatedJob.Annotations[QueueAnnotation]).To(Equal("false"))
		})
	})

	// Note: Methods like deleteJobResources, GetPodsForJob, GetServicesForJob, deletePodsAndServices, and DeletePodGroup
	// require more complex mocking and setup that would go beyond the scope of this task.
	// They involve actual Kubernetes API interactions that are difficult to test without
	// setting up extensive mock environments.
})
