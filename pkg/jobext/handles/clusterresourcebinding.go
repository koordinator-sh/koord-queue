package handles

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"gitlab.alibaba-inc.com/cos/karmada/pkg/apis/policy/v1alpha1"
	"gitlab.alibaba-inc.com/cos/karmada/pkg/apis/work/v1alpha2"
	common "github.com/kube-queue/kube-queue/pkg/jobext/apis/common/job_controller/v1"
	pytorchv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/pytorch/v1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	v1 "k8s.io/api/core/v1"
	schedv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type ClusterResourceBinding struct {
	c client.Client

	pj       PytorchJob
	sparkApp SparkApplication

	managedAllJobs bool

	framework.JobType_NonPod
}

func (r *ClusterResourceBinding) Object() client.Object {
	return &v1alpha2.ClusterResourceBinding{}
}

func (r *ClusterResourceBinding) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*v1alpha2.ClusterResourceBinding)
	return job.DeepCopy()
}

func (r *ClusterResourceBinding) GVK() schema.GroupVersionKind {
	return v1alpha2.SchemeGroupVersion.WithKind(v1alpha2.ResourceKindClusterResourceBinding)
}

// will not be used because no pods are created for ClusterResourceBinding
func (j *ClusterResourceBinding) GetPodSetName(ownerName string, p *v1.Pod) string {
	return ownerName
}

func (r *ClusterResourceBinding) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	_ = obj.(*v1alpha2.ClusterResourceBinding)
	ps := []kueue.PodSet{}
	// TODO: add parsing podset
	return ps
}

func (r *ClusterResourceBinding) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*v1alpha2.ClusterResourceBinding)
	totalResources := v1.ResourceList{}
	if job.Spec.ReplicaRequirements != nil {
		count := int(job.Spec.Replicas)
		for resourceType, resCount := range job.Spec.ReplicaRequirements.ResourceRequest {
			quantity := resCount
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
	// calculate the total resource request
	for _, replicaSpec := range job.Spec.Components {
		// get different roles and calculate the sum of the pods belongs to the same role
		count := int(replicaSpec.Replicas)
		resources := replicaSpec.ReplicaRequirements.ResourceRequest
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
	return totalResources
}

func (r *ClusterResourceBinding) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*v1alpha2.ClusterResourceBinding)
	if job.Spec.ReplicaRequirements != nil {
		pcn := job.Spec.ReplicaRequirements.PriorityClassName
		pc := &schedv1.PriorityClass{}
		err := r.c.Get(ctx, client.ObjectKey{Name: pcn}, pc)
		if err == nil {
			return pcn, &pc.Value
		}
	}
	for _, req := range job.Spec.Components {
		priorityClassName := req.ReplicaRequirements.PriorityClassName
		pc := &schedv1.PriorityClass{}
		err := r.c.Get(ctx, client.ObjectKey{Name: priorityClassName}, pc)
		if err == nil {
			return priorityClassName, &pc.Value
		}
	}
	return "", ptr.To(int32(0))
}

func (r *ClusterResourceBinding) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1alpha2.ClusterResourceBinding)

	old := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: *job.Status.DeepCopy()}
	var updated bool
	updated, new.Status.Conditions = setK8sCondition(new.Status.Conditions, "Queuing", metav1.ConditionTrue, "ClusterResourceBinding Enqueued", "Enqueued")
	if !updated {
		return nil
	}
	return cli.SubResource("status").Patch(ctx, new, client.MergeFrom(old))
}

func (r *ClusterResourceBinding) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "rb-qu"
}

func (r *ClusterResourceBinding) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1alpha2.ClusterResourceBinding)
	if !(job.Spec.Suspension != nil && job.Spec.Suspension.Scheduling != nil && *job.Spec.Suspension.Scheduling) {
		if job.Spec.Suspension == nil {
			job.Spec.Suspension = &v1alpha1.Suspension{
				Scheduling: ptr.To(true),
			}
		} else {
			job.Spec.Suspension.Scheduling = ptr.To(true)
		}
		if err := cli.Update(ctx, job); err != nil {
			return err
		}
	}
	old := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: *job.Status.DeepCopy()}
	var updated bool
	updated, new.Status.Conditions = setK8sCondition(new.Status.Conditions, "Queuing", metav1.ConditionTrue, "ClusterResourceBinding Enqueued", "Enqueued")
	if !updated {
		return nil
	}

	patch := client.MergeFrom(old)
	return cli.SubResource("status").Patch(ctx, new, patch)
}

func (r *ClusterResourceBinding) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1alpha2.ClusterResourceBinding)

	old := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &v1alpha2.ClusterResourceBinding{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: *job.Status.DeepCopy()}
	var updated bool
	updated, new.Status.Conditions = setK8sCondition(new.Status.Conditions, "Queuing", metav1.ConditionFalse, "ClusterResourceBinding Dequeued", "Dequeued")
	if updated {
		patch := client.MergeFrom(old)
		err := cli.SubResource("status").Patch(ctx, new, patch)
		if err != nil {
			return err
		}
	}
	if new.Spec.Suspension == nil || new.Spec.Suspension.Scheduling == nil || !*new.Spec.Suspension.Scheduling {
		return nil
	}
	if new.Spec.Suspension != nil {
		new.Spec.Suspension.Scheduling = ptr.To(false)
	}
	new.Annotations["kube-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (r *ClusterResourceBinding) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*v1alpha2.ClusterResourceBinding)
	if job.Spec.Suspension != nil && job.Spec.Suspension.Scheduling != nil && *job.Spec.Suspension.Scheduling {
		return framework.Queuing, time.Now()
	}

	jobtype := job.Spec.Resource.Kind
	switch jobtype {
	case "PyTorchJob":
		allItemsComplete := true
		allItemsSuccess := true
		for _, status := range job.Status.AggregatedStatus {
			pj := &pytorchv1.PyTorchJob{}
			st := &common.JobStatus{}
			json.Unmarshal(status.Status.Raw, st)
			pj.Status = *st
			pjstatus, _ := r.pj.GetJobStatus(ctx, pj, client)
			if pjstatus != framework.Failed && pjstatus != framework.Succeeded {
				allItemsComplete = false
				if pjstatus != framework.Succeeded {
					allItemsSuccess = false
				}
			}
			klog.V(4).Infof("cluster: %v, status: %++v, raw: %v", status.ClusterName, st, string(status.Status.Raw))
		}
		if allItemsComplete {
			if allItemsSuccess {
				return framework.Succeeded, time.Now()
			}
			return framework.Failed, time.Now()
		}
	case "SparkApplication":
		allItemsComplete := true
		allItemsSuccess := true
		for _, status := range job.Status.AggregatedStatus {
			sa := &v1beta2.SparkApplication{}
			st := &v1beta2.SparkApplicationStatus{}
			json.Unmarshal(status.Status.Raw, st)
			sa.Status = *st
			sastatus, _ := r.sparkApp.GetJobStatus(ctx, sa, client)
			if sastatus != framework.Failed && sastatus != framework.Succeeded {
				allItemsComplete = false
				if sastatus != framework.Succeeded {
					allItemsSuccess = false
				}
			}
			klog.V(4).Infof("cluster: %v, status: %++v, raw: %v", status.ClusterName, st, string(status.Status.Raw))
		}
		if allItemsComplete {
			if allItemsSuccess {
				return framework.Succeeded, time.Now()
			}
			return framework.Failed, time.Now()
		}
	}

	for _, c := range job.Status.AggregatedStatus {
		if c.Health == v1alpha2.ResourceHealthy {
			return framework.Running, time.Now()
		}
	}
	dequeued := false
	scheduled := false
	var dequeueTime time.Time
	var scheduleTime time.Time
	for _, cond := range job.Status.Conditions {
		if cond.Type == "Queuing" && cond.Status == metav1.ConditionTrue {
			return framework.Queuing, cond.LastTransitionTime.Time
		}
		if cond.Type == "Queuing" && cond.Status == metav1.ConditionFalse && cond.Reason == "Dequeued" {
			dequeued = true
			dequeueTime = cond.LastTransitionTime.Time
		}
		if cond.Type == "Scheduled" && cond.Status == metav1.ConditionFalse {
			if cond.LastTransitionTime.Time.After(dequeueTime) {
				dequeueTime = cond.LastTransitionTime.Time
			}
		}
		if cond.Type == "Scheduled" && cond.Status == metav1.ConditionTrue {
			if job.Spec.Suspension != nil && (job.Spec.Suspension.Dispatching != nil && *job.Spec.Suspension.Dispatching ||
				job.Spec.Suspension.DispatchingOnClusters != nil) {
				return framework.Running, cond.LastTransitionTime.Time
			}
			scheduled = true
			scheduleTime = cond.LastTransitionTime.Time
		}
	}
	if scheduled {
		return framework.Running, scheduleTime
	}
	if dequeued {
		return framework.Pending, dequeueTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (r *ClusterResourceBinding) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if r.managedAllJobs {
		return true
	}
	job := obj.(*v1alpha2.ClusterResourceBinding)
	for _, condition := range job.Status.Conditions {
		if condition.Type == "Queuing" {
			return true
		}
	}
	return job.Spec.Suspension != nil && job.Spec.Suspension.Scheduling != nil &&
		*job.Spec.Suspension.Scheduling && job.Status.LastScheduledTime == nil
}

func NewClusterResourceBindingController(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	var extension framework.GenericJobExtension
	j := &ClusterResourceBinding{
		managedAllJobs: managedAllJobs, c: cli,
		pj: PytorchJob{}, sparkApp: SparkApplication{},
	}
	v1alpha2.Install(scheme)
	extension = framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, true)
}
