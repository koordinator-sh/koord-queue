package handles

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/rest"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	kv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type RayJob struct {
	c client.Client

	managedAllJobs bool

	getPods func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error)
}

func (j *RayJob) Object() client.Object {
	return &rayv1.RayJob{}
}
func (j *RayJob) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*rayv1.RayJob)
	return job.DeepCopy()
}

func (j *RayJob) GVK() schema.GroupVersionKind {
	return rayv1.SchemeGroupVersion.WithKind("RayJob")
}

func (j *RayJob) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return j.getPods
}

func (j *RayJob) GetPodSetName(ownerName string, p *v1.Pod) string {
	if g := p.Labels["ray.io/group"]; g == "headgroup" {
		return "head"
	} else if g != "" {
		return g
	} else {
		// for PAI
		return "worker"
	}
}

func (job *RayJob) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	j := obj.(*rayv1.RayJob)
	podSets := make([]kueue.PodSet, len(j.Spec.RayClusterSpec.WorkerGroupSpecs)+1)

	// head
	podSets[0] = kueue.PodSet{
		Name:     headGroupPodSetName,
		Template: *j.Spec.RayClusterSpec.HeadGroupSpec.Template.DeepCopy(),
		Count:    1,
	}

	// workers
	for index := range j.Spec.RayClusterSpec.WorkerGroupSpecs {
		wgs := &j.Spec.RayClusterSpec.WorkerGroupSpecs[index]
		replicas := int32(1)
		if wgs.Replicas != nil {
			replicas = *wgs.Replicas
		}
		name := genPsName(wgs.GroupName)
		podSets[index+1] = kueue.PodSet{
			Name:     name,
			Template: *wgs.Template.DeepCopy(),
			Count:    replicas,
		}
	}

	rayClusterName := j.Status.RayClusterName
	if rayClusterName == "" {
		return podSets
	}
	rc := &rayv1.RayCluster{}
	if err := job.c.Get(ctx, types.NamespacedName{Namespace: j.Namespace, Name: rayClusterName}, rc); err != nil {
		return podSets
	}
	for _, wgs := range rc.Spec.WorkerGroupSpecs {
		name := genPsName(wgs.GroupName)
		replica := int32(1)
		if wgs.Replicas != nil {
			replica = *wgs.Replicas
		}
		for i, ps := range podSets {
			if ps.Name == name && ps.Count < replica {
				podSets[i].Count = replica
			}
		}
	}
	return podSets
}

func (j *RayJob) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*rayv1.RayJob)
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	headResource := util.GetPodRequestsAndLimits(&job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec)
	util.AddResourceList(totalResources, headResource)

	for _, workerGroup := range job.Spec.RayClusterSpec.WorkerGroupSpecs {
		if workerGroup.Replicas == nil {
			continue
		}
		replica := *workerGroup.Replicas
		resource := util.GetPodRequestsAndLimits(&workerGroup.Template.Spec)
		for i := 0; i < int(replica); i++ {
			util.AddResourceList(totalResources, resource)
		}
	}
	return totalResources
}

func (j *RayJob) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "ray-qu"
}

func (j *RayJob) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*rayv1.RayJob)
	priorityClassName := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.PriorityClassName
	priority := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Priority

	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.c.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("can not get PriorityClass %v from k8s for rayjob:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	return priorityClassName, priority
}

func (j *RayJob) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayJob)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"
	if job.Annotations["koord-queue/job-enqueue-timestamp"] != "" {
		return nil
	}

	old := job
	new := job.DeepCopy()
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.ObjectMeta.Annotations["koord-queue/job-enqueue-timestamp"] = time.Now().String()

	util.SetPodTemplateSpec(&new.Spec.RayClusterSpec.HeadGroupSpec.Template, job.Namespace, job.Name, "head", j.QueueUnitSuffix())
	new.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1/RayJob"
	new.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	for i, wg := range new.Spec.RayClusterSpec.WorkerGroupSpecs {
		name := genPsName(wg.GroupName)
		util.SetPodTemplateSpec(&new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template, job.Namespace, job.Name, name, j.QueueUnitSuffix())
		new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1/RayJob"
		new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	}

	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJob) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayJob)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"
	if job.Spec.Suspend {
		return nil
	}

	old := &rayv1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new := &rayv1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new.Spec.Suspend = true
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJob) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayJob)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"
	if !job.Spec.Suspend {
		return nil
	}

	old := &rayv1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new := &rayv1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new.Spec.Suspend = false
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.Annotations["koord-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJob) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*rayv1.RayJob)
	queuingTransTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-enqueue-timestamp"])
	if err != nil {
		queuingTransTime = time.Now()
	}
	switch job.Status.JobDeploymentStatus {
	case rayv1.JobDeploymentStatusComplete:
		if job.Status.RayClusterName != "" {
			rc := &rayv1.RayCluster{}
			if err := j.c.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RayClusterName}, rc); err == nil {
				return framework.Running, job.Status.EndTime.Time
			}
		}
		// if job.Status.RayClusterStatus.AvailableWorkerReplicas > 0 {
		// 	return framework.Running, time.Now()
		// }
		return framework.Succeeded, job.Status.EndTime.Time
	case rayv1.JobDeploymentStatusFailed:
		if job.Status.RayClusterName != "" {
			rc := &rayv1.RayCluster{}
			if err := j.c.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RayClusterName}, rc); err == nil {
				return framework.Running, job.Status.EndTime.Time
			}
		}
		// if job.Status.RayClusterStatus.AvailableWorkerReplicas > 0 {
		// 	return framework.Running, time.Now()
		// }
		return framework.Failed, job.Status.EndTime.Time
	case rayv1.JobDeploymentStatusRunning:
		return framework.Running, job.Status.StartTime.Time
	}

	if job.Annotations["koord-queue/job-dequeue-timestamp"] != "" {
		return framework.Pending, queuingTransTime
	}
	if job.Annotations["koord-queue/job-enqueue-timestamp"] != "" {
		transTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-enqueue-timestamp"])
		if err != nil {
			return framework.Queuing, time.Now()
		}
		return framework.Queuing, transTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *RayJob) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	job := obj.(*rayv1.RayJob)
	// TODO: rayjob with cluster selector should not be managed by queue
	if job.Annotations["koord-queue/job-enqueue-timestamp"] != "" {
		return true
	}
	return job.Status.StartTime == nil && job.Spec.Suspend
}

var headGroupPodSetName = "head"

func genPsName(wg string) string {
	name := strings.ToLower(wg)
	if name == "" {
		name = "worker"
	}
	return name

}

type RayJobV1alpha1 struct {
	c client.Client

	managedAllJobs bool

	getPods func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error)
}

func (j *RayJobV1alpha1) Object() client.Object {
	return &rayv1alpha1.RayJob{}
}
func (j *RayJobV1alpha1) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*rayv1alpha1.RayJob)
	return job.DeepCopy()
}

func (j *RayJobV1alpha1) GVK() schema.GroupVersionKind {
	return rayv1alpha1.SchemeGroupVersion.WithKind("RayJob")
}

func (j *RayJobV1alpha1) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return j.getPods
}

func (j *RayJobV1alpha1) GetPodSetName(ownerName string, p *v1.Pod) string {
	if g := p.Labels["ray.io/group"]; g == "headgroup" {
		return "head"
	} else if g != "" {
		return g
	} else {
		// for PAI
		return "worker"
	}
}

func (job *RayJobV1alpha1) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	j := obj.(*rayv1alpha1.RayJob)
	podSets := make([]kueue.PodSet, len(j.Spec.RayClusterSpec.WorkerGroupSpecs)+1)

	// head
	podSets[0] = kueue.PodSet{
		Name:     headGroupPodSetName,
		Template: *j.Spec.RayClusterSpec.HeadGroupSpec.Template.DeepCopy(),
		Count:    1,
	}

	// workers
	for index := range j.Spec.RayClusterSpec.WorkerGroupSpecs {
		wgs := &j.Spec.RayClusterSpec.WorkerGroupSpecs[index]
		replicas := int32(1)
		if wgs.Replicas != nil {
			replicas = *wgs.Replicas
		}
		name := genPsName(wgs.GroupName)
		podSets[index+1] = kueue.PodSet{
			Name:     name,
			Template: *wgs.Template.DeepCopy(),
			Count:    replicas,
		}
	}

	rayClusterName := j.Status.RayClusterName
	if rayClusterName == "" {
		return podSets
	}
	rc := &rayv1.RayCluster{}
	if err := job.c.Get(ctx, types.NamespacedName{Namespace: j.Namespace, Name: rayClusterName}, rc); err != nil {
		return podSets
	}
	for _, wgs := range rc.Spec.WorkerGroupSpecs {
		name := genPsName(wgs.GroupName)
		replica := int32(1)
		if wgs.Replicas != nil {
			replica = *wgs.Replicas
		}
		for i, ps := range podSets {
			if ps.Name == name && ps.Count < replica {
				podSets[i].Count = replica
			}
		}
	}
	return podSets
}

func (j *RayJobV1alpha1) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*rayv1alpha1.RayJob)
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	headResource := util.GetPodRequestsAndLimits(&job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec)
	util.AddResourceList(totalResources, headResource)

	for _, workerGroup := range job.Spec.RayClusterSpec.WorkerGroupSpecs {
		if workerGroup.Replicas == nil {
			continue
		}
		replica := *workerGroup.Replicas
		resource := util.GetPodRequestsAndLimits(&workerGroup.Template.Spec)
		for i := 0; i < int(replica); i++ {
			util.AddResourceList(totalResources, resource)
		}
	}
	return totalResources
}

func (j *RayJobV1alpha1) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "ray-qu"
}

func (j *RayJobV1alpha1) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*rayv1alpha1.RayJob)
	priorityClassName := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.PriorityClassName
	priority := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Priority

	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.c.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("can not get PriorityClass %v from k8s for rayjob:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	return priorityClassName, priority
}

func (j *RayJobV1alpha1) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1alpha1.RayJob)
	job.TypeMeta.APIVersion = rayv1alpha1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"

	old := job
	new := job.DeepCopy()
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.ObjectMeta.Annotations["koord-queue/job-enqueue-timestamp"] = time.Now().String()

	util.SetPodTemplateSpec(&new.Spec.RayClusterSpec.HeadGroupSpec.Template, job.Namespace, job.Name, "head", j.QueueUnitSuffix())
	new.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1alpha1/RayJob"
	new.Spec.RayClusterSpec.HeadGroupSpec.Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	for i, wg := range new.Spec.RayClusterSpec.WorkerGroupSpecs {
		name := genPsName(wg.GroupName)
		util.SetPodTemplateSpec(&new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template, job.Namespace, job.Name, name, j.QueueUnitSuffix())
		new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1alpha1/RayJob"
		new.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	}

	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJobV1alpha1) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1alpha1.RayJob)
	job.TypeMeta.APIVersion = rayv1alpha1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"

	old := &rayv1alpha1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new := &rayv1alpha1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new.Spec.Suspend = true
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJobV1alpha1) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1alpha1.RayJob)
	job.TypeMeta.APIVersion = rayv1alpha1.GroupVersion.String()
	job.TypeMeta.Kind = "RayJob"

	old := &rayv1alpha1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new := &rayv1alpha1.RayJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new.Spec.Suspend = false
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.Annotations["koord-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayJobV1alpha1) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*rayv1alpha1.RayJob)
	switch job.Status.JobDeploymentStatus {
	case rayv1alpha1.JobDeploymentStatusComplete:
		if job.Status.RayClusterStatus.AvailableWorkerReplicas > 0 {
			return framework.Running, time.Now()
		}
		return framework.Succeeded, job.Status.EndTime.Time
	case rayv1alpha1.JobDeploymentStatusRunning:
		return framework.Running, job.Status.StartTime.Time
	case rayv1alpha1.JobDeploymentStatusSuspended:
		return framework.Queuing, job.CreationTimestamp.Time
	}

	if job.Annotations["koord-queue/job-enqueue-timestamp"] != "" {
		transTime, err := time.Parse(timeFormat, job.Annotations["koord-queue/job-enqueue-timestamp"])
		if err != nil {
			return framework.Queuing, time.Now()
		}
		return framework.Queuing, transTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *RayJobV1alpha1) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*rayv1alpha1.RayJob)
	// TODO: rayjob with cluster selector should not be managed by queue
	if job.Annotations["koord-queue/job-enqueue-timestamp"] != "" {
		return true
	}
	return job.Status.StartTime == nil && job.Spec.Suspend
}

func NewRayJobReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, version string, args string) framework.JobHandle {
	var extension framework.GenericJobExtension
	if version == "v1" {
		j := &RayJob{
			managedAllJobs: managedAllJobs, c: cli, getPods: func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
				job := rayv1.RayJob{}
				if err := cli.Get(ctx, namespaceName, &job); client.IgnoreNotFound(err) != nil {
					return nil, err
				} else if errors.IsNotFound(err) {
					return nil, nil
				}
				if job.Status.RayClusterName == "" {
					return nil, nil
				}
				cluster := rayv1.RayCluster{}
				if err := cli.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RayClusterName}, &cluster); client.IgnoreNotFound(err) != nil {
					return nil, err
				} else if errors.IsNotFound(err) {
					return nil, nil
				}
				pl := &v1.PodList{}
				if err := cli.List(ctx, pl, client.InNamespace(job.Namespace), client.MatchingFields(map[string]string{util.PodsByOwnersCacheFields: "RayCluster/" + cluster.Name})); client.IgnoreNotFound(err) != nil {
					return nil, err
				}
				pods := []*v1.Pod{}
				for _, pod := range pl.Items {
					pods = append(pods, &pod)
				}
				return pods, nil
			}}
		rayv1.AddToScheme(scheme)
		extension = framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	} else {
		j := &RayJobV1alpha1{
			managedAllJobs: managedAllJobs, c: cli, getPods: func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
				job := rayv1alpha1.RayJob{}
				if err := cli.Get(ctx, namespaceName, &job); client.IgnoreNotFound(err) != nil {
					return nil, err
				} else if errors.IsNotFound(err) {
					return nil, nil
				}
				if job.Status.RayClusterName == "" {
					return nil, nil
				}
				cluster := rayv1alpha1.RayCluster{}
				if err := cli.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RayClusterName}, &cluster); client.IgnoreNotFound(err) != nil {
					return nil, err
				} else if errors.IsNotFound(err) {
					return nil, nil
				}
				pl := &v1.PodList{}
				if err := cli.List(ctx, pl, client.InNamespace(job.Namespace), client.MatchingFields(map[string]string{util.PodsByOwnersCacheFields: "RayCluster/" + cluster.Name})); client.IgnoreNotFound(err) != nil {
					return nil, err
				}
				pods := []*v1.Pod{}
				for _, pod := range pl.Items {
					pods = append(pods, &pod)
				}
				return pods, nil
			}}
		rayv1alpha1.AddToScheme(scheme)
		extension = framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	}
	return framework.NewJobHandle(0, 0, extension, false)
}

var _ framework.GenericReservationJobExtension = &RayJob{}

func (j *RayJob) ReservationStatus(ctx context.Context, obj client.Object, qu *kv1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
	msg := map[string]string{}
	allScheduled := true
	allSucceed := true
	for _, resv := range resvs {
		scheduled := false
		preempted := false
		hasFalse := false
		for _, cond := range resv.Status.Conditions {
			if cond.Type == "Preempted" && cond.Status == koordinatorschedulerv1alpha1.ConditionStatusTrue {
				preempted = true
			}
			if cond.Type != koordinatorschedulerv1alpha1.ReservationConditionScheduled {
				continue
			}
			reason := strings.Split(cond.Reason, "/")
			if len(reason) != 2 {
				if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
					hasFalse = true
					msg[resv.Name] = cond.Message
				}
				scheduled = true
				continue
			}
			revision, err := strconv.Atoi(reason[1])
			if err != nil {
				if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
					hasFalse = true
					msg[resv.Name] = cond.Message
				}
				scheduled = true
				continue
			}
			if int64(revision) != qu.Status.Attempts {
				continue
			}
			if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
				hasFalse = true
				msg[resv.Name] = cond.Message
			}
			scheduled = true
		}
		if hasFalse && preempted {
			scheduled = false
			hasFalse = false
		}
		if !scheduled {
			allScheduled = false
		} else if hasFalse {
			allSucceed = false
		}
	}
	retMsg := ""
	limit := 1
	count := 0
	for k, v := range msg {
		retMsg += fmt.Sprintf("{%v:%v},", k, v)
		count++
		if count >= limit {
			retMsg += "..."
			break
		}
	}
	if allScheduled && allSucceed {
		return framework.ReservationStatus_Succeed, ""
	} else if allScheduled && !allSucceed {
		return framework.ReservationStatus_Failed, retMsg
	} else {
		return framework.ReservationStatus_Pending, retMsg
	}
}

func (j *RayJob) Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error) {
	job := obj.(*rayv1.RayJob)
	if job.Status.RayClusterName == "" {
		return nil, fmt.Errorf("RayJob %v/%v has no RayClusterName in status", job.Namespace, job.Name)
	}
	if len(job.Spec.ClusterSelector) > 0 {
		return nil, nil
	}
	headTmpl := job.Spec.RayClusterSpec.HeadGroupSpec.Template
	headTmpl.Namespace = job.Namespace
	resvs := []koordinatorschedulerv1alpha1.Reservation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%v-%v-%v", job.Name, "head", 0),
			},
			Spec: koordinatorschedulerv1alpha1.ReservationSpec{
				Template: &headTmpl,
				Owners: []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"ray.io/cluster": job.Status.RayClusterName,
						"ray.io/group":   "headgroup",
					},
				}}},
			},
		},
	}
	for _, template := range job.Spec.RayClusterSpec.WorkerGroupSpecs {
		if template.GroupName == "AIMaster" {
			continue
		}
		if template.Replicas == nil || *template.Replicas == 0 {
			continue
		}
		replicas := int(*template.Replicas)

		labelSelector := &metav1.LabelSelector{}
		labelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
			{
				Key:      "ray.io/cluster",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{job.Status.RayClusterName},
			},
			{
				Key:      "ray.io/group",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{template.GroupName},
			},
		}
		templateCopy := template.Template.DeepCopy()
		for i, cont := range templateCopy.Spec.Containers {
			if len(cont.Resources.Requests) == 0 {
				templateCopy.Spec.Containers[i].Resources.Requests = templateCopy.Spec.Containers[i].Resources.Limits
			}
		}
		templateCopy.Namespace = job.Namespace
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "pod-group.scheduling.sigs.k8s.io/name", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "pod-group.scheduling.sigs.k8s.io", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "scheduling.x-k8s.io/pod-group", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "network-topology-job-name", framework.Suffix)
		for i := 0; i < replicas; i++ {
			resv := koordinatorschedulerv1alpha1.Reservation{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%v-%v-%v", job.Name, strings.ToLower(template.GroupName), i),
				},
				Spec: koordinatorschedulerv1alpha1.ReservationSpec{
					Template: templateCopy,
					Owners:   []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: labelSelector}},
				},
			}
			resvs = append(resvs, resv)
		}
	}
	return resvs, nil
}
