package handles

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	kv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
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

type RayCluster struct {
	c client.Client

	managedAllJobs bool

	framework.JobType_Default
}

func (j *RayCluster) Object() client.Object {
	return &rayv1.RayCluster{}
}
func (j *RayCluster) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*rayv1.RayCluster)
	return job.DeepCopy()
}

func (j *RayCluster) GVK() schema.GroupVersionKind {
	return rayv1.SchemeGroupVersion.WithKind("RayCluster")
}

func (j *RayCluster) GetPodSetName(ownerName string, p *v1.Pod) string {
	if g := p.Labels["ray.io/group"]; g == "headgroup" {
		return "head"
	} else if g != "" {
		return g
	} else {
		// for PAI
		return "worker"
	}
}

func (job *RayCluster) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	j := obj.(*rayv1.RayCluster)
	podSets := make([]kueue.PodSet, len(j.Spec.WorkerGroupSpecs)+1)

	// head
	podSets[0] = kueue.PodSet{
		Name:     headGroupPodSetName,
		Template: *j.Spec.HeadGroupSpec.Template.DeepCopy(),
		Count:    1,
	}

	// workers
	for index := range j.Spec.WorkerGroupSpecs {
		wgs := &j.Spec.WorkerGroupSpecs[index]
		replicas := int32(1)
		if wgs.Replicas != nil {
			replicas = *wgs.Replicas
		}
		podSets[index+1] = kueue.PodSet{
			Name:     genPsName(wgs.GroupName),
			Template: *wgs.Template.DeepCopy(),
			Count:    replicas,
		}
	}
	return podSets
}

func (j *RayCluster) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*rayv1.RayCluster)
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	headResource := util.GetPodRequestsAndLimits(&job.Spec.HeadGroupSpec.Template.Spec)
	util.AddResourceList(totalResources, headResource)

	for _, workerGroup := range job.Spec.WorkerGroupSpecs {
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

func (j *RayCluster) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "raycluster-qu"
}

func (j *RayCluster) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*rayv1.RayCluster)
	priorityClassName := job.Spec.HeadGroupSpec.Template.Spec.PriorityClassName
	priority := job.Spec.HeadGroupSpec.Template.Spec.Priority

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

func (j *RayCluster) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayCluster)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayCluster"
	if job.Annotations["kube-queue/job-enqueue-timestamp"] != "" {
		return nil
	}

	old := job
	new := job.DeepCopy()
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.ObjectMeta.Annotations["kube-queue/job-enqueue-timestamp"] = time.Now().String()

	util.SetPodTemplateSpec(&new.Spec.HeadGroupSpec.Template, job.Namespace, job.Name, headGroupPodSetName, j.QueueUnitSuffix())
	new.Spec.HeadGroupSpec.Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1/RayCluster"
	new.Spec.HeadGroupSpec.Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	for i, wg := range new.Spec.WorkerGroupSpecs {
		name := genPsName(wg.GroupName)
		util.SetPodTemplateSpec(&new.Spec.WorkerGroupSpecs[i].Template, job.Namespace, job.Name, name, j.QueueUnitSuffix())
		new.Spec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedAPIVersionKindAnnoKey] = "ray.io/v1/RayCluster"
		new.Spec.WorkerGroupSpecs[i].Template.Annotations[util.RelatedObjectAnnoKey] = job.Name
	}

	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayCluster) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayCluster)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayCluster"
	if job.Spec.Suspend != nil && *job.Spec.Suspend {
		return nil
	}

	old := &rayv1.RayCluster{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new := &rayv1.RayCluster{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new.Spec.Suspend = ptr.To(true)
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayCluster) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*rayv1.RayCluster)
	job.TypeMeta.APIVersion = rayv1.GroupVersion.String()
	job.TypeMeta.Kind = "RayCluster"
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		return nil
	}

	old := &rayv1.RayCluster{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new := &rayv1.RayCluster{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new.Spec.Suspend = ptr.To(false)
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.Annotations["kube-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *RayCluster) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*rayv1.RayCluster)
	queuingTransTime, err := time.Parse(timeFormat, job.Annotations["kube-queue/job-enqueue-timestamp"])
	if err != nil {
		queuingTransTime = time.Now()
	}
	if job.Status.ReadyWorkerReplicas > 0 {
		// TODO: how to know running time
		return framework.Running, time.Now()
	}

	if job.Annotations["kube-queue/job-enqueue-timestamp"] == "" {
		return framework.Created, job.CreationTimestamp.Time
	}

	if job.Annotations["kube-queue/job-dequeue-timestamp"] != "" && job.Status.ReadyWorkerReplicas == 0 {
		dequeueTransTime, err := time.Parse(timeFormat, job.Annotations["kube-queue/job-dequeue-timestamp"])
		if err != nil {
			dequeueTransTime = time.Now()
		}
		return framework.Pending, dequeueTransTime
	}
	return framework.Queuing, queuingTransTime
}

func (j *RayCluster) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	job := obj.(*rayv1.RayCluster)
	if len(job.OwnerReferences) > 0 && job.OwnerReferences[0].Kind == "RayJob" {
		return false
	}
	if j.managedAllJobs {
		return true
	}
	if job.Labels["ray.io/originated-from-crd"] == "RayCluster" {
		return false
	}
	// TODO: rayjob with cluster selector should not be managed by queue
	if job.Annotations["kube-queue/job-enqueue-timestamp"] != "" {
		return true
	}
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

func NewRayClusterReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	var extension framework.GenericJobExtension
	j := &RayCluster{
		managedAllJobs: managedAllJobs, c: cli}
	rayv1.AddToScheme(scheme)
	extension = framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, false)
}

var _ framework.GenericReservationJobExtension = &RayCluster{}

func (j *RayCluster) ReservationStatus(ctx context.Context, obj client.Object, qu *kv1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
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

func (j *RayCluster) Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error) {
	job := obj.(*rayv1.RayCluster)
	headTmpl := job.Spec.HeadGroupSpec.Template.DeepCopy()
	for i, cont := range headTmpl.Spec.Containers {
		if len(cont.Resources.Requests) == 0 {
			headTmpl.Spec.Containers[i].Resources.Requests = headTmpl.Spec.Containers[i].Resources.Limits
		}
	}
	headTmpl.Namespace = job.Namespace
	resvs := []koordinatorschedulerv1alpha1.Reservation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%v-%v-%v", job.Name, "head", 0),
			},
			Spec: koordinatorschedulerv1alpha1.ReservationSpec{
				Template: headTmpl,
				Owners: []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"ray.io/cluster": job.Name,
						"ray.io/group":   "headgroup",
					},
				}}},
			},
		},
	}
	for _, template := range job.Spec.WorkerGroupSpecs {
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
				Values:   []string{job.Name},
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
