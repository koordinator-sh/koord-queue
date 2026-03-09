package handles

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	kv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	commonv1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/common/job_controller/v1"
	networkv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/networkaware/apis/scheduling/v1alpha1"
	tfjobv1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/tensorflow/v1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"github.com/kubeflow/common/pkg/controller.v1/control"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

const QueueAnnotation = "scheduling.x-k8s.io/suspend"

type TfJob struct {
	c          client.Client
	podControl control.RealPodControl
	svcControl control.RealServiceControl

	managedAllJobs bool

	framework.JobType_Default
}

func (j *TfJob) Object() client.Object {
	return &tfjobv1.TFJob{}
}
func (j *TfJob) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*tfjobv1.TFJob)
	return job.DeepCopy()
}

func (j *TfJob) GVK() schema.GroupVersionKind {
	return tfjobv1.SchemeGroupVersion.WithKind(tfjobv1.Kind)
}

func (j *TfJob) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*tfjobv1.TFJob)
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	for _, replicaSpec := range job.Spec.TFReplicaSpecs {
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

func (j *TfJob) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "tf-qu"
}

func (j *TfJob) GetPodSetName(ownerName string, p *v1.Pod) string {
	return p.Labels["job-role"]
}

func (j *TfJob) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	job := obj.(*tfjobv1.TFJob)
	ps := []kueue.PodSet{}
	for role, template := range job.Spec.TFReplicaSpecs {
		ps = append(ps, kueue.PodSet{
			Name:     strings.ToLower(string(role)),
			Template: template.Template,
			Count:    *template.Replicas,
		})
	}
	return ps
}

func (j *TfJob) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*tfjobv1.TFJob)
	var priorityClassName string
	var priority *int32
	for role := range job.Spec.TFReplicaSpecs {
		if r := strings.ToLower(string(role)); r == util.AIMASTERROLENAME {
			klog.Infof("skip search priority in role %v for job %v", role, job.Name)
			continue
		}
		priorityClassName = job.Spec.TFReplicaSpecs[role].Template.Spec.PriorityClassName
		priority = job.Spec.TFReplicaSpecs[role].Template.Spec.Priority
		// By default, we think that the PriorityClassName and priority of all roles are the same,
		// so just take the value of one role and break
		break
	}

	// If there is a related priorityClassInstance in K8s, we use priorityClass's value instead of tfJob.Spec.TFReplicaSpecs[role].Template.Spec.Priority
	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.c.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("can not get PriorityClass %v from k8s for tfjob:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	return priorityClassName, priority
}

func (j *TfJob) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*tfjobv1.TFJob)
	for roleName, roleSpec := range job.Spec.TFReplicaSpecs {
		if roleName == util.AIMASTERROLENAME {
			continue
		}
		util.SetPodTemplateSpec(&roleSpec.Template, job.Namespace, job.Name, strings.ToLower(string(roleName)), j.QueueUnitSuffix())
	}
	if err := cli.Update(ctx, job, &client.UpdateOptions{}); err != nil {
		return err
	}

	old := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: commonv1.JobStatus{Conditions: util.SliceCopy(job.Status.Conditions)}}
	if !setCondition(&new.Status, "Queuing", v1.ConditionTrue, "Job Enqueued") {
		return nil
	}
	return cli.SubResource("status").Patch(ctx, new, client.MergeFrom(old))
}

func (j *TfJob) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*tfjobv1.TFJob)
	if job.Annotations[QueueAnnotation] == "true" {
		return nil
	}

	old := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new.Annotations = util.MapCopy(job.Annotations)
	if len(new.Annotations) == 0 {
		new.Annotations = map[string]string{}
	}
	new.Annotations[QueueAnnotation] = "true"
	err := cli.Patch(ctx, new, client.MergeFrom(old))
	if err != nil {
		return err
	}
	if !framework.EnablePodReclaim {
		return nil
	}
	return j.deleteJobResources(job)
}

func (tc *TfJob) deleteJobResources(tfjob *tfjobv1.TFJob) error {
	pods, err := tc.GetPodsForJob(tfjob)
	filteredPods := []*v1.Pod{}
	for _, p := range pods {
		if p.Labels["replica-type"] == util.AIMASTERROLENAME {
			continue
		}
		filteredPods = append(filteredPods, p)
	}

	if err != nil {
		klog.V(3).Infof("getPodsForTfjob error %v", err)
		return err
	}

	services, err := tc.GetServicesForJob(tfjob)

	if err != nil {
		klog.V(3).Infof("getServicesForTfjob error %v", err)
		return err
	}

	if err := tc.deletePodsAndServices(tfjob, filteredPods, services); err != nil {
		return err
	}

	if err := tc.DeletePodGroup(tfjob); err != nil {
		return err
	}
	return nil
}

func (jc *TfJob) GenLabels(jobName string) map[string]string {
	groupName := "kubeflow.org"
	return map[string]string{
		"group-name": groupName,
		"job-name":   strings.Replace(jobName, "/", "-", -1),
	}
}

func (r *TfJob) GetPodsForJob(obj interface{}) ([]*v1.Pod, error) {
	job, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	label := r.GenLabels(job.GetName())
	labelSelector := labels.SelectorFromSet(labels.Set(label))
	pods := v1.PodList{}
	err = r.c.List(context.Background(), &pods, client.InNamespace(job.GetNamespace()), client.MatchingLabels(label))
	if err != nil {
		return nil, err
	}
	podList := make([]*v1.Pod, len(pods.Items))
	for i, p := range pods.Items {
		podList[i] = p.DeepCopy()
	}

	canAdoptFunc := RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh := &tfjobv1.TFJob{}
		err := r.c.Get(context.Background(), types.NamespacedName{
			Namespace: job.GetNamespace(),
			Name:      job.GetName(),
		}, fresh)
		if err != nil {
			return nil, err
		}
		if fresh.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v", job.GetNamespace(), job.GetName(), fresh.GetUID(), job.GetUID())
		}
		return fresh, nil
	})
	cm := control.NewPodControllerRefManager(r.podControl, job, labelSelector, tfjobv1.SchemeGroupVersionKind, canAdoptFunc)
	return cm.ClaimPods(podList)
}

func (r *TfJob) GetServicesForJob(obj interface{}) ([]*v1.Service, error) {
	job, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	label := r.GenLabels(job.GetName())
	labelSelector := labels.SelectorFromSet(labels.Set(label))
	services := v1.ServiceList{}
	err = r.c.List(context.Background(), &services, client.InNamespace(job.GetNamespace()), client.MatchingLabels(label))
	if err != nil {
		return nil, err
	}
	servicesList := make([]*v1.Service, len(services.Items))
	for i, p := range services.Items {
		servicesList[i] = p.DeepCopy()
	}

	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing services (see #42639).
	canAdoptFunc := RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh := &tfjobv1.TFJob{}
		err := r.c.Get(context.Background(), types.NamespacedName{
			Namespace: job.GetNamespace(),
			Name:      job.GetName(),
		}, fresh)
		if err != nil {
			return nil, err
		}
		if fresh.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v", job.GetNamespace(), job.GetName(), fresh.GetUID(), job.GetUID())
		}
		return fresh, nil
	})
	cm := control.NewServiceControllerRefManager(r.svcControl, job, labelSelector, tfjobv1.SchemeGroupVersion.WithKind("TFJob"), canAdoptFunc)
	return cm.ClaimServices(servicesList)
}

// deletePodsAndServices deletes all the pods and master service.
func (pc *TfJob) deletePodsAndServices(job *tfjobv1.TFJob, pods []*v1.Pod, services []*v1.Service) error {
	if len(pods) == 0 {
		return nil
	}

	for _, pod := range pods {
		if err := pc.podControl.DeletePod(pod.Namespace, pod.Name, job); err != nil {
			return err
		}
	}

	rt := strings.ToLower(string(tfjobv1.TFReplicaTypeMaster))
	services, err := pc.FilterServicesForReplicaType(services, rt)
	if err != nil {
		return err
	}
	for _, service := range services {
		if err := pc.svcControl.DeleteService(service.Namespace, service.Name, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *TfJob) DeletePodGroup(job metav1.Object) error {
	klog.Infof("Deleting PodGroup %s", job.GetName())
	pg := v1alpha1.PodGroup{}
	pg.Name = job.GetName()
	pg.Namespace = job.GetNamespace()
	err := r.c.Delete(context.Background(), &pg)
	// Delete podGroup
	if !errors.IsNotFound(err) {
		return fmt.Errorf("unable to delete PodGroup: %v", err)
	}
	return nil
}

// FilterServicesForReplicaType returns service belong to a replicaType.
func (pc *TfJob) FilterServicesForReplicaType(services []*v1.Service, replicaType string) ([]*v1.Service, error) {
	var result []*v1.Service

	replicaSelector := &metav1.LabelSelector{
		MatchLabels: make(map[string]string),
	}

	replicaSelector.MatchLabels["tf-replica-type"] = replicaType

	for _, service := range services {
		selector, err := metav1.LabelSelectorAsSelector(replicaSelector)
		if err != nil {
			return nil, err
		}
		if !selector.Matches(labels.Set(service.Labels)) {
			continue
		}
		result = append(result, service)
	}
	return result, nil
}

func (j *TfJob) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*tfjobv1.TFJob)

	old := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &tfjobv1.TFJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: commonv1.JobStatus{Conditions: util.SliceCopy(job.Status.Conditions)}}
	if setCondition(&new.Status, "Queuing", v1.ConditionFalse, "Job Dequeued") {
		patch := client.MergeFrom(old)
		err := cli.SubResource("status").Patch(ctx, new, patch)
		if err != nil {
			return err
		}
	}

	if job.Annotations[QueueAnnotation] != "true" {
		return nil
	}
	new.Annotations = util.MapCopy(job.Annotations)
	if len(new.Annotations) == 0 {
		new.Annotations = map[string]string{}
	}
	if os.Getenv("PAI_ENV") != "" {
		delete(new.Annotations, QueueAnnotation)
	} else {
		new.ObjectMeta.Annotations[QueueAnnotation] = "false"
	}
	new.Annotations["koord-queue/job-dequeue-timestamp"] = time.Now().String()
	patch := client.MergeFrom(old)
	return cli.Patch(ctx, new, patch)
}

func (j *TfJob) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*tfjobv1.TFJob)
	var running, queuing bool = false, false
	var runningTransTime, queuingTransTime time.Time
	for _, cond := range job.Status.Conditions {
		if cond.Type == commonv1.JobSucceeded && cond.Status == v1.ConditionTrue {
			return framework.Succeeded, cond.LastTransitionTime.Time
		}
		if cond.Type == commonv1.JobFailed && cond.Status == v1.ConditionTrue {
			return framework.Failed, cond.LastTransitionTime.Time
		}
		if cond.Type == commonv1.JobRunning || cond.Type == "Scheduled" {
			running = true
			runningTransTime = cond.LastTransitionTime.Time
		}
		if cond.Type == "Queuing" {
			queuing = true
			queuingTransTime = cond.LastTransitionTime.Time
		}
	}

	if running {
		return framework.Running, runningTransTime
	}
	if value, ok := job.Annotations[QueueAnnotation]; !ok || (ok && value == "false") {
		return framework.Pending, queuingTransTime
	}
	if queuing {
		return framework.Queuing, queuingTransTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *TfJob) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*tfjobv1.TFJob)
	for _, condition := range job.Status.Conditions {
		if condition.Type == "Queuing" {
			return true
		}
	}
	return job.Status.StartTime == nil && job.Annotations[QueueAnnotation] == "true"
}

type tfOption struct {
	RunningTimeout *time.Duration `yaml:"runningTimeout,omitempty"`
	BackoffTimeout *time.Duration `yaml:"backoffTimeout,omitempty"`
}

func NewTfJobReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	c := kubernetes.NewForConfigOrDie(config)
	j := &TfJob{c: cli,
		managedAllJobs: managedAllJobs,
		podControl:     control.RealPodControl{KubeClient: c, Recorder: record.NewBroadcaster().NewRecorder(scheme, v1.EventSource{Component: "tf-opeartor-extension"})},
		svcControl:     control.RealServiceControl{KubeClient: c, Recorder: record.NewBroadcaster().NewRecorder(scheme, v1.EventSource{Component: "tf-opeartor-extension"})},
	}
	tfjobv1.AddToScheme(scheme)
	extension := framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)

	op := tfOption{}
	err := yaml.Unmarshal([]byte(args), &op)
	if err != nil {
		log.Fatalf("failed to parse args for tfjob extension, content:\n%v\n err: %v", args, err)
	}
	var rt time.Duration = 0
	var bt time.Duration = time.Minute
	if op.RunningTimeout != nil {
		rt = *op.RunningTimeout
	}
	if op.BackoffTimeout != nil {
		bt = *op.BackoffTimeout
	}

	return framework.NewJobHandle(rt, bt, extension, false)
}

var _ framework.GenericReservationJobExtension = &TfJob{}
var _ framework.NetworkAwareJobExtension = &TfJob{}

func (j *TfJob) ReservationStatus(ctx context.Context, obj client.Object, qu *kv1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
	return framework.ReservationStatus(ctx, obj, qu, resvs)
}

func (j *TfJob) Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error) {
	job := obj.(*tfjobv1.TFJob)
	resvs := []koordinatorschedulerv1alpha1.Reservation{}
	netNs, netName := j.GetNetworkTopologyNamespaceName(ctx, obj)
	for role, template := range job.Spec.TFReplicaSpecs {
		if role == "AIMaster" {
			continue
		}
		replicas := 1
		if template.Replicas != nil {
			replicas = int(*template.Replicas)
		}

		labelSelector := &metav1.LabelSelector{}
		switch role {
		default:
			labelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
				{
					Key:      "tf-replica-type",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{util.AIMASTERROLENAME},
				},
				{
					Key:      "job-name",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{strings.Replace(job.Name, "/", "-", -1)},
				},
			}
			// TODO:
			// labelSelector.MatchLabels = map[string]string{
			// 	"job-name":             strings.Replace(job.Name, "/", "-", -1),
			// 	"pytorch-replica-type": "worker",
			// }
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
					Name: fmt.Sprintf("%v-%v-%v", job.Name, strings.ToLower(string(role)), i),
				},
				Spec: koordinatorschedulerv1alpha1.ReservationSpec{
					Template: templateCopy,
					Owners:   []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: labelSelector}},
				},
			}
			if netName != "" {
				resv.Labels["network-topology-job-name"] = netName + "-" + framework.Suffix
				resv.Labels["network-topology-job-namespace"] = netNs
			}
			resvs = append(resvs, resv)
		}
	}
	return resvs, nil
}

func (j *TfJob) GetNetworkTopologyNamespaceName(ctx context.Context, o client.Object) (string, string) {
	job, ok := o.(*tfjobv1.TFJob)
	if !ok {
		return "", ""
	}
	masterTemplate := job.Spec.TFReplicaSpecs[tfjobv1.TFReplicaTypeMaster]
	if masterTemplate == nil {
		// TODO
		return "", ""
	}
	crNs, crName := masterTemplate.Template.Labels["network-topology-job-namespace"], masterTemplate.Template.Labels["network-topology-job-name"]
	if crName == "" {
		return "", ""
	}
	return crNs, crName
}

func (j *TfJob) GetJobNetworkTopologyCR(ctx context.Context, o client.Object, client client.Client) (jnt *networkv1alpha1.JobNetworkTopology, err error) {
	crNs, crName := j.GetNetworkTopologyNamespaceName(ctx, o)
	if crName == "" {
		return nil, nil
	}
	jnt = &networkv1alpha1.JobNetworkTopology{}
	err = client.Get(ctx, types.NamespacedName{Namespace: crNs, Name: crName}, jnt)
	if err != nil {
		return nil, err
	}
	return jnt, nil
}
