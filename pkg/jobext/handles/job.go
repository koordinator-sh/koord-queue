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

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Job object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
type Job struct {
	podlister client.Client

	managedAllJobs bool

	framework.JobType_Default
}

func (j *Job) Object() client.Object {
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
	}
}
func (j *Job) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*batchv1.Job)
	return job.DeepCopy()
}

func (j *Job) GVK() schema.GroupVersionKind {
	return batchv1.SchemeGroupVersion.WithKind("Job")
}

func (j *Job) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*batchv1.Job)
	parallel := job.Spec.Parallelism
	request := util.PodRequestsAndLimits(&job.Spec.Template)
	scaledRequest := v1.ResourceList{}
	for i := 0; i < int(*parallel); i++ {
		util.AddResourceList(scaledRequest, request)
	}
	return scaledRequest
}

func (j *Job) GetPodSetName(ownerName string, p *v1.Pod) string {
	return ownerName
}

func (j *Job) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	job := obj.(*batchv1.Job)
	ps := []kueue.PodSet{}
	parallelism := 1
	if job.Spec.Parallelism != nil {
		parallelism = int(*job.Spec.Parallelism)
	}
	completion := parallelism
	if job.Spec.Completions != nil {
		completion = int(*job.Spec.Completions)
	}
	succeed := job.Status.Succeeded
	realParallelism := parallelism
	if completion-int(succeed) < parallelism {
		realParallelism = completion - int(succeed)
	}
	ps = append(ps, kueue.PodSet{
		Name:     job.Name,
		Template: job.Spec.Template,
		Count:    int32(realParallelism),
	})
	return ps
}

func (j *Job) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*batchv1.Job)
	priorityClassName, priority := job.Spec.Template.Spec.PriorityClassName, job.Spec.Template.Spec.Priority
	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.podlister.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("can not get PriorityClass %v from k8s for pytorchjob:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	return priorityClassName, priority
}

func (j *Job) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*batchv1.Job)
	if job.Annotations["koord-queue/job-has-enqueued"] != "" {
		return nil
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations["koord-queue/job-has-enqueued"] = "true"
	job.Annotations["koord-queue/job-enqueue-timestamp"] = time.Now().String()
	// template in job is immutable
	// util.SetPodTemplateSpec(&job.Spec.Template, job.Namespace, job.Name, job.Name, j.QueueUnitSuffix())
	return cli.Update(ctx, job)
}

func (j *Job) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*batchv1.Job)
	if job.Spec.Suspend != nil && *job.Spec.Suspend {
		return nil
	}
	job.Spec.Suspend = ptr.To(true)
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations["koord-queue/job-dequeue-timestamp"] = ""
	return cli.Update(ctx, job)
}

func (j *Job) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*batchv1.Job)
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		return nil
	}
	job.Spec.Suspend = ptr.To(false)
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations["koord-queue/job-dequeue-timestamp"] = time.Now().String()

	return cli.Update(ctx, job)
}

const timeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

func (j *Job) GetJobStatus(ctx context.Context, obj client.Object, cli client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*batchv1.Job)
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == v1.ConditionTrue {
			return framework.Succeeded, cond.LastTransitionTime.Time
		}
		if cond.Type == batchv1.JobFailed && cond.Status == v1.ConditionTrue {
			return framework.Failed, cond.LastTransitionTime.Time
		}
	}

	if job.Status.Active > 0 {
		pl := &v1.PodList{}
		err := j.podlister.List(ctx, pl, client.MatchingLabels{"batch.kubernetes.io/controller-uid": string(job.UID)})
		if err == nil {
			for _, pod := range pl.Items {
				if pod.Status.Phase == v1.PodRunning {
					return framework.Running, pod.Status.StartTime.Time
				}
			}
		}
		return framework.Pending, time.Now()
	}
	if job.Annotations["koord-queue/job-dequeue-timestamp"] != "" {
		transTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-dequeue-timestamp"])
		if err != nil {
			return framework.Pending, time.Now()
		}
		return framework.Pending, transTime
	}
	if job.Annotations["koord-queue/job-has-enqueued"] == "true" {
		transTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-enqueue-timestamp"])
		if err != nil {
			return framework.Queuing, time.Now()
		}
		return framework.Queuing, transTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *Job) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*batchv1.Job)
	if job.Annotations["koord-queue/job-has-enqueued"] != "" {
		return true
	}
	return job.Status.StartTime == nil && job.Spec.Suspend != nil && *job.Spec.Suspend
}

func (j *Job) QueueUnitSuffix() string {
	return ""
}

func NewJobReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	j := &Job{podlister: cli, managedAllJobs: managedAllJobs}
	batchv1.AddToScheme(scheme)
	extension := framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, false)
}
