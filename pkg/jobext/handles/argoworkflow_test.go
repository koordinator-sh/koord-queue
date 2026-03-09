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

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clientgofake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
)

var _ = Describe("ArgoWorkflow", func() {
	var (
		workflowReconciler *ArgoWorkflow
		scheme             *runtime.Scheme
		ctx                context.Context
		workflow           *wfv1.Workflow
	)

	BeforeEach(func() {
		ctx = context.Background()

		scheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(batchv1.AddToScheme(scheme)).To(Succeed())
		Expect(wfv1.AddToScheme(scheme)).To(Succeed())

		workflowReconciler = &ArgoWorkflow{
			c: clientgofake.NewClientBuilder().WithScheme(scheme).Build(),
		}

		workflow = &wfv1.Workflow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-workflow",
				Namespace: "default",
			},
			Spec: wfv1.WorkflowSpec{
				Entrypoint: "whalesay",
				Templates: []wfv1.Template{
					{
						Name: "whalesay",
						Container: &corev1.Container{
							Image: "docker/whalesay:latest",
							Command: []string{
								"cowsay",
							},
							Args: []string{
								"hello world",
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
					},
				},
			},
		}
	})

	Describe("Object", func() {
		Context("When getting object", func() {
			It("should return a Workflow object", func() {
				obj := workflowReconciler.Object()
				Expect(obj).To(BeAssignableToTypeOf(&wfv1.Workflow{}))
			})
		})
	})

	Describe("GVK", func() {
		Context("When getting GroupVersionKind", func() {
			It("should return the correct GVK for Workflow", func() {
				gvk := workflowReconciler.GVK()
				Expect(gvk.Kind).To(Equal("Workflow"))
				Expect(gvk.Group).To(Equal("argoproj.io"))
				Expect(gvk.Version).To(Equal("v1alpha1"))
			})
		})
	})

	Describe("QueueUnitSuffix", func() {
		Context("When getting QueueUnit suffix", func() {
			It("should return an empty string", func() {
				suffix := workflowReconciler.QueueUnitSuffix()
				Expect(suffix).To(Equal(""))
			})
		})
	})

	Describe("GetPodSetName", func() {
		Context("When getting PodSet name", func() {
			It("should return 'default'", func() {
				podSetName := workflowReconciler.GetPodSetName("test-workflow", &corev1.Pod{})
				Expect(podSetName).To(Equal("default"))
			})
		})
	})

	Describe("PodSet", func() {
		Context("When getting PodSet", func() {
			BeforeEach(func() {
				workflow.Annotations = map[string]string{
					"koord-queue/min-resources": `cpu: 100m
memory: 100Mi`,
				}
			})

			It("should return correct PodSet", func() {
				podSets := workflowReconciler.PodSet(ctx, workflow)
				Expect(podSets).To(HaveLen(1))
				Expect(podSets[0].Name).To(Equal("default"))
				Expect(podSets[0].Count).To(Equal(int32(1)))
			})
		})
	})

	Describe("ManagedByQueue", func() {
		Context("When checking if managed by queue", func() {
			It("should return false for workflow without koord-queue-suspend template", func() {
				managed := workflowReconciler.ManagedByQueue(ctx, workflow)
				Expect(managed).To(BeFalse())
			})
		})
	})

	Describe("isWorkflowSuspend", func() {
		Context("When checking if workflow is suspended", func() {
			It("should return false for nil workflow", func() {
				result := isWorkflowSuspend(nil)
				Expect(result).To(BeFalse())
			})

			It("should return false when workflow is not suspended", func() {
				workflow.Spec.Suspend = nil
				result := isWorkflowSuspend(workflow)
				Expect(result).To(BeFalse())
			})

			It("should return true when workflow spec is suspended", func() {
				workflow.Spec.Suspend = ptr.To(true)
				result := isWorkflowSuspend(workflow)
				Expect(result).To(BeTrue())
			})

			It("should return true when workflow has running suspend node", func() {
				workflow.Spec.Suspend = nil
				workflow.Status.Nodes = map[string]wfv1.NodeStatus{
					"node1": {
						Type:  wfv1.NodeTypeSuspend,
						Phase: wfv1.NodeRunning,
					},
				}
				result := isWorkflowSuspend(workflow)
				Expect(result).To(BeTrue())
			})
		})
	})

	Describe("checkWfConditions", func() {
		Context("When checking workflow conditions", func() {
			It("should return false when no conditions exist", func() {
				result := checkWfConditions(workflow, "Enqueued")
				Expect(result).To(BeFalse())
			})

			It("should return false when condition type doesn't match", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "SomeOtherCondition",
						Status: metav1.ConditionTrue,
					},
				}
				result := checkWfConditions(workflow, "Enqueued")
				Expect(result).To(BeFalse())
			})

			It("should return false when condition status is not true", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionFalse,
					},
				}
				result := checkWfConditions(workflow, "Enqueued")
				Expect(result).To(BeFalse())
			})

			It("should return true when condition type matches and status is true", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionTrue,
					},
				}
				result := checkWfConditions(workflow, "Enqueued")
				Expect(result).To(BeTrue())
			})
		})
	})

	Describe("getEntryTemp", func() {
		Context("When getting entry template", func() {
			It("should return nil when entrypoint is empty", func() {
				workflow.Spec.Entrypoint = ""
				result := workflowReconciler.getEntryTemp(workflow)
				Expect(result).To(BeNil())
			})

			It("should return nil when entrypoint template doesn't exist", func() {
				workflow.Spec.Entrypoint = "nonexistent"
				result := workflowReconciler.getEntryTemp(workflow)
				Expect(result).To(BeNil())
			})

			It("should return the entry template when it exists", func() {
				workflow.Spec.Entrypoint = "whalesay"
				result := workflowReconciler.getEntryTemp(workflow)
				Expect(result).ToNot(BeNil())
				Expect(result.Name).To(Equal("whalesay"))
			})
		})
	})

	Describe("Resources", func() {
		Context("When getting resources", func() {
			It("should return empty resource list for non-workflow object", func() {
				job := &batchv1.Job{}
				result := workflowReconciler.Resources(ctx, job)
				Expect(result).To(BeEmpty())
			})

			It("should return empty resource list when no min-resources annotation exists", func() {
				result := workflowReconciler.Resources(ctx, workflow)
				Expect(result).To(BeEmpty())
			})

			It("should return resource list from YAML annotation", func() {
				workflow.Annotations = map[string]string{
					"koord-queue/min-resources": `cpu: 100m
memory: 100Mi`,
				}
				result := workflowReconciler.Resources(ctx, workflow)
				Expect(result).ToNot(BeEmpty())
				Expect(result).To(HaveKey(corev1.ResourceCPU))
				Expect(result).To(HaveKey(corev1.ResourceMemory))
			})

			It("should return resource list from JSON annotation", func() {
				workflow.Annotations = map[string]string{
					"koord-queue/min-resources": `{"cpu": "100m", "memory": "100Mi"}`,
				}
				result := workflowReconciler.Resources(ctx, workflow)
				Expect(result).ToNot(BeEmpty())
				Expect(result).To(HaveKey(corev1.ResourceCPU))
				Expect(result).To(HaveKey(corev1.ResourceMemory))
			})
		})
	})

	Describe("Priority", func() {
		Context("When getting priority", func() {
			It("should return empty string and nil for non-workflow object", func() {
				job := &batchv1.Job{}
				pc, p := workflowReconciler.Priority(ctx, job)
				Expect(pc).To(Equal(""))
				Expect(p).To(BeNil())
			})

			It("should return workflow priority when set", func() {
				priority := int32(10)
				workflow.Spec.Priority = &priority
				pc, p := workflowReconciler.Priority(ctx, workflow)
				Expect(pc).To(Equal(""))
				Expect(p).To(Equal(&priority))
			})

			It("should return entry template priority class when workflow priority is not set", func() {
				workflow.Spec.Priority = nil
				workflow.Spec.Templates[0].PriorityClassName = "high-priority"
				workflow.Spec.Entrypoint = "whalesay"
				pc, p := workflowReconciler.Priority(ctx, workflow)
				Expect(pc).To(Equal("high-priority"))
				Expect(p).To(BeNil())
			})
		})
	})

	Describe("GetJobStatus", func() {
		Context("When getting job status", func() {
			It("should return Created for non-workflow object", func() {
				job := &batchv1.Job{}
				status, _ := workflowReconciler.GetJobStatus(ctx, job, nil)
				Expect(status).To(Equal(framework.Created))
			})

			It("should return Created when workflow is not enqueued", func() {
				status, _ := workflowReconciler.GetJobStatus(ctx, workflow, nil)
				Expect(status).To(Equal(framework.Created))
			})

			It("should return Queuing when workflow is enqueued but suspended", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionTrue,
					},
				}
				workflow.Spec.Suspend = ptr.To(true)
				status, _ := workflowReconciler.GetJobStatus(ctx, workflow, nil)
				Expect(status).To(Equal(framework.Queuing))
			})

			It("should return Succeeded when workflow succeeded", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionTrue,
					},
				}
				workflow.Spec.Suspend = nil
				workflow.Status.Phase = wfv1.WorkflowSucceeded
				status, _ := workflowReconciler.GetJobStatus(ctx, workflow, nil)
				Expect(status).To(Equal(framework.Succeeded))
			})

			It("should return Failed when workflow failed", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionTrue,
					},
				}
				workflow.Spec.Suspend = nil
				workflow.Status.Phase = wfv1.WorkflowFailed
				status, _ := workflowReconciler.GetJobStatus(ctx, workflow, nil)
				Expect(status).To(Equal(framework.Failed))
			})

			It("should return Running for other cases", func() {
				workflow.Status.Conditions = []wfv1.Condition{
					{
						Type:   "Enqueued",
						Status: metav1.ConditionTrue,
					},
				}
				workflow.Spec.Suspend = nil
				workflow.Status.Phase = wfv1.WorkflowRunning
				status, _ := workflowReconciler.GetJobStatus(ctx, workflow, nil)
				Expect(status).To(Equal(framework.Running))
			})
		})
	})

	Describe("Enqueue", func() {
		Context("When enqueueing workflow", func() {
			It("should do nothing for non-workflow object", func() {
				job := &batchv1.Job{}
				err := workflowReconciler.Enqueue(ctx, job, nil)
				Expect(err).To(BeNil())
			})

			It("should add enqueued condition and annotations to workflow", func() {
				err := workflowReconciler.Enqueue(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())

				updatedWorkflow := &wfv1.Workflow{}
				getErr := workflowReconciler.c.Get(ctx, types.NamespacedName{
					Name:      "test-workflow",
					Namespace: "default",
				}, updatedWorkflow)
				Expect(getErr).To(BeNil())

				Expect(updatedWorkflow.Annotations["koord-queue/job-has-enqueued"]).To(Equal("true"))
				Expect(updatedWorkflow.Annotations["koord-queue/job-enqueue-timestamp"]).ToNot(BeEmpty())

				found := false
				for _, condition := range updatedWorkflow.Status.Conditions {
					if condition.Type == "Enqueued" && condition.Status == metav1.ConditionTrue {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue())
			})
		})
	})

	Describe("Suspend", func() {
		Context("When suspending workflow", func() {
			It("should do nothing for non-workflow object", func() {
				job := &batchv1.Job{}
				err := workflowReconciler.Suspend(ctx, job, nil)
				Expect(err).To(BeNil())
			})

			It("should do nothing when workflow is already suspended", func() {
				workflow.Spec.Suspend = ptr.To(true)
				err := workflowReconciler.Suspend(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())

				// Should not have changed
				updatedWorkflow := &wfv1.Workflow{}
				getErr := workflowReconciler.c.Get(ctx, types.NamespacedName{
					Name:      "test-workflow",
					Namespace: "default",
				}, updatedWorkflow)
				Expect(getErr).To(BeNil())
				Expect(updatedWorkflow.Spec.Suspend).To(Equal(ptr.To(true)))
			})

			It("should suspend workflow when not already suspended", func() {
				workflow.Spec.Suspend = nil
				err := workflowReconciler.Suspend(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())

				updatedWorkflow := &wfv1.Workflow{}
				getErr := workflowReconciler.c.Get(ctx, types.NamespacedName{
					Name:      "test-workflow",
					Namespace: "default",
				}, updatedWorkflow)
				Expect(getErr).To(BeNil())
				Expect(updatedWorkflow.Spec.Suspend).To(Equal(ptr.To(true)))
			})
		})
	})

	Describe("Resume", func() {
		Context("When resuming workflow", func() {
			It("should do nothing for non-workflow object", func() {
				job := &batchv1.Job{}
				err := workflowReconciler.Resume(ctx, job, nil)
				Expect(err).To(BeNil())
			})

			It("should do nothing when workflow is not suspended", func() {
				workflow.Spec.Suspend = ptr.To(false)
				err := workflowReconciler.Resume(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())

				// Should not have changed
				updatedWorkflow := &wfv1.Workflow{}
				getErr := workflowReconciler.c.Get(ctx, types.NamespacedName{
					Name:      "test-workflow",
					Namespace: "default",
				}, updatedWorkflow)
				Expect(getErr).To(BeNil())
				Expect(updatedWorkflow.Spec.Suspend).To(Equal(ptr.To(false)))
			})

			It("should resume workflow when suspended", func() {
				workflow.Spec.Suspend = ptr.To(true)
				err := workflowReconciler.Resume(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())

				updatedWorkflow := &wfv1.Workflow{}
				getErr := workflowReconciler.c.Get(ctx, types.NamespacedName{
					Name:      "test-workflow",
					Namespace: "default",
				}, updatedWorkflow)
				Expect(getErr).To(BeNil())
				Expect(updatedWorkflow.Spec.Suspend).To(Equal(ptr.To(false)))
			})
		})
	})

	Describe("GetRelatedQueueUnit", func() {
		Context("When getting related QueueUnit", func() {
			It("should return error when QueueUnit doesn't exist", func() {
				qu, err := workflowReconciler.GetRelatedQueueUnit(ctx, workflow, workflowReconciler.c)
				Expect(err).To(HaveOccurred())
				Expect(qu).ToNot(BeNil())
			})

			It("should return QueueUnit when it exists", func() {
				// First create a QueueUnit
				queueUnit := &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-workflow",
						Namespace: "default",
					},
				}
				err := workflowReconciler.c.Create(ctx, queueUnit)
				Expect(err).To(BeNil())

				// Then try to get it
				qu, err := workflowReconciler.GetRelatedQueueUnit(ctx, workflow, workflowReconciler.c)
				Expect(err).To(BeNil())
				Expect(qu).ToNot(BeNil())
				Expect(qu.Name).To(Equal("test-workflow"))
				Expect(qu.Namespace).To(Equal("default"))
			})
		})
	})

	Describe("GetRelatedJob", func() {
		Context("When getting related job", func() {
			It("should return workflow object", func() {
				// First create a workflow
				err := workflowReconciler.c.Create(ctx, workflow)
				Expect(err).To(BeNil())

				// Create a QueueUnit to pass to the method
				queueUnit := &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-workflow",
						Namespace: "default",
					},
				}

				job := workflowReconciler.GetRelatedJob(ctx, queueUnit, workflowReconciler.c)
				Expect(job).ToNot(BeNil())
				Expect(job).To(BeAssignableToTypeOf(&wfv1.Workflow{}))
			})
		})
	})
})
