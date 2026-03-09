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

	"k8s.io/client-go/rest"

	mpiv1alpha1 "github.com/AliyunContainerService/mpi-operator/pkg/apis/kubeflow/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	commonv1 "gitlab.alibaba-inc.com/kubedlpro/apis/common/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MpiJobV1alpha1 object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
type MpiJobV1alpha1 struct {
	managedAllJobs bool

	framework.JobType_Default
}

func (j *MpiJobV1alpha1) Object() client.Object {
	return &mpiv1alpha1.MPIJob{}
}
func (j *MpiJobV1alpha1) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*mpiv1alpha1.MPIJob)
	return job.DeepCopy()
}

func (j *MpiJobV1alpha1) GVK() schema.GroupVersionKind {
	return mpiv1alpha1.SchemeGroupVersion.WithKind(mpiv1alpha1.SchemeGroupVersionKind.Kind)
}

func (j *MpiJobV1alpha1) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*mpiv1alpha1.MPIJob)
	jobResources := map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
		"worker": {
			Replicas: job.Spec.Replicas,
			Template: job.Spec.Template,
		},
	}
	return calculateTotalResources(jobResources)
}

func calculateTotalResources(specs map[commonv1.ReplicaType]*commonv1.ReplicaSpec) v1.ResourceList {
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	for _, replicaSpec := range specs {
		// get different roles and calculate the sum of the pods belongs to the same role
		count := int(*replicaSpec.Replicas)
		containers := replicaSpec.Template.Spec.Containers
		for _, container := range containers {
			// calculate the resource request of pods first (the pod count is decided by replicas's number)
			resources := container.Resources.Requests
			for resourceType := range resources {
				quantity := resources[resourceType]
				// scale the quantity by count
				replicaQuantity := resource.Quantity{}
				for i := 1; i <= count; i++ {
					replicaQuantity.Add(quantity)
				}
				// check if the resourceType is in totalResources
				if totalQuantity, ok := totalResources[resourceType]; !ok {
					// not in: set this replicaQuantity
					totalResources[resourceType] = replicaQuantity
				} else {
					// in: append this replicaQuantity and update
					totalQuantity.Add(replicaQuantity)
					totalResources[resourceType] = totalQuantity
				}
			}
		}
	}
	return totalResources
}

func (j *MpiJobV1alpha1) GetPodSetName(ownerName string, p *v1.Pod) string {
	return ownerName
}

func (j *MpiJobV1alpha1) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	job := obj.(*mpiv1alpha1.MPIJob)
	ps := []kueue.PodSet{}
	ps = append(ps, kueue.PodSet{
		Name:     job.Name,
		Template: job.Spec.Template,
		Count:    *job.Spec.Replicas,
	})
	return ps
}

func (j *MpiJobV1alpha1) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*mpiv1alpha1.MPIJob)
	var priorityClassName string
	var priority *int32
	priorityClassName = job.Spec.Template.Spec.PriorityClassName
	priority = job.Spec.Template.Spec.Priority
	return priorityClassName, priority
}

func (j *MpiJobV1alpha1) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*mpiv1alpha1.MPIJob)
	if job.Annotations["koord-queue/job-has-enqueued"] == "true" {
		return nil
	}

	old := job
	new := job.DeepCopy()
	new.Annotations = util.MapCopy(job.Annotations)
	new.Annotations["koord-queue/job-has-enqueued"] = "true"
	new.Annotations["koord-queue/job-enqueue-timestamp"] = time.Now().String()

	util.SetPodTemplateSpec(&new.Spec.Template, job.Namespace, job.Name, job.Name, j.QueueUnitSuffix())
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *MpiJobV1alpha1) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*mpiv1alpha1.MPIJob)

	old := &mpiv1alpha1.MPIJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new := &mpiv1alpha1.MPIJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new.Annotations = util.MapCopy(job.Annotations)
	if new.Annotations[QueueAnnotation] == "true" {
		return nil
	}
	if len(new.Annotations) == 0 {
		new.Annotations = map[string]string{}
	}
	new.Annotations[QueueAnnotation] = "true"
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *MpiJobV1alpha1) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*mpiv1alpha1.MPIJob)
	if job.Annotations[QueueAnnotation] != "true" {
		return nil
	}

	old := &mpiv1alpha1.MPIJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new := &mpiv1alpha1.MPIJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new.Annotations = util.MapCopy(job.Annotations)
	if len(new.Annotations) == 0 {
		new.Annotations = map[string]string{}
	} else {
		delete(new.Annotations, QueueAnnotation)
	}
	new.Annotations["koord-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *MpiJobV1alpha1) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*mpiv1alpha1.MPIJob)
	queuingTransTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-enqueue-timestamp"])
	if err != nil {
		queuingTransTime = time.Now()
	}
	dequeueTransTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-dequeue-timestamp"])
	if err != nil {
		dequeueTransTime = time.Now()
	}
	if job.Status.LauncherStatus == mpiv1alpha1.LauncherSucceeded {
		return framework.Succeeded, time.Now()
	}
	if job.Status.LauncherStatus == mpiv1alpha1.LauncherFailed {
		return framework.Failed, time.Now()
	}
	if job.Status.LauncherStatus == mpiv1alpha1.LauncherActive {
		return framework.Running, time.Now()
	}
	if value, ok := job.Annotations[QueueAnnotation]; !ok || (ok && value == "false") {
		return framework.Pending, dequeueTransTime
	}
	if job.Annotations["koord-queue/job-has-enqueued"] != "" {
		return framework.Queuing, queuingTransTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *MpiJobV1alpha1) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*mpiv1alpha1.MPIJob)

	if job.Annotations["koord-queue/job-has-enqueued"] != "" {
		return true
	}
	return job.Status.LauncherStatus == "" && job.Annotations[QueueAnnotation] == "true"
}

func NewMpiV1alpha1JobReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	j := &MpiJobV1alpha1{
		managedAllJobs: managedAllJobs}
	mpiv1alpha1.AddToScheme(scheme)
	extension := framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, false)
}

func (j *MpiJobV1alpha1) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "mpi-qu"
}
