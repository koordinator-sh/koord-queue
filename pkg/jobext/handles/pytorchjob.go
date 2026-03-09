package handles

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kubeflow/common/pkg/controller.v1/control"
	networkv1alpha1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/networkaware/apis/scheduling/v1alpha1"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	commonv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/common/job_controller/v1"
	pytorchv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/pytorch/v1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
)

type PytorchJob struct {
	c          client.Client
	podControl control.RealPodControl
	svcControl control.RealServiceControl

	managedAllJobs bool

	framework.JobType_Default
}

func (j *PytorchJob) Object() client.Object {
	return &pytorchv1.PyTorchJob{}
}
func (j *PytorchJob) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*pytorchv1.PyTorchJob)
	return job.DeepCopy()
}

func (j *PytorchJob) GetPodSetName(ownerName string, p *v1.Pod) string {
	if _, ok := p.Labels["pytorch-replica-type"]; ok {
		return p.Labels["pytorch-replica-type"]
	}
	return p.Labels["job-role"]
}

func (j *PytorchJob) GVK() schema.GroupVersionKind {
	return pytorchv1.SchemeGroupVersion.WithKind(pytorchv1.Kind)
}

func (j *PytorchJob) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	job := obj.(*pytorchv1.PyTorchJob)
	ps := []kueue.PodSet{}
	for role, template := range job.Spec.PyTorchReplicaSpecs {
		ps = append(ps, kueue.PodSet{
			Name:     strings.ToLower(string(role)),
			Template: template.Template,
			Count:    *template.Replicas,
		})
	}
	return ps
}

var _ framework.GenericReservationJobExtension = &PytorchJob{}
var _ framework.NetworkAwareJobExtension = &PytorchJob{}

func (j *PytorchJob) ReservationStatus(ctx context.Context, obj client.Object, qu *kv1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
	return framework.ReservationStatus(ctx, obj, qu, resvs)
}

func (j *PytorchJob) Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error) {
	job := obj.(*pytorchv1.PyTorchJob)
	resvs := []koordinatorschedulerv1alpha1.Reservation{}
	netNs, netName := j.GetNetworkTopologyNamespaceName(ctx, obj)
	for role, template := range job.Spec.PyTorchReplicaSpecs {
		if role == "AIMaster" {
			continue
		}
		replicas := 1
		if template.Replicas != nil {
			replicas = int(*template.Replicas)
		}

		labelSelector := &metav1.LabelSelector{}
		switch role {
		// case util.AIMASTERROLENAME:
		// 	// TODO:
		// 	labelSelector.MatchLabels = map[string]string{
		// 		"job-name":             strings.Replace(job.Name, "/", "-", -1),
		// 		"pytorch-replica-type": util.AIMASTERROLENAME,
		// 	}
		case pytorchv1.PyTorchReplicaTypeMaster:
			labelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
				{
					Key:      "pytorch-replica-type",
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
			// 	"pytorch-replica-type": "master",
			// }
		case pytorchv1.PyTorchReplicaTypeWorker:
			labelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
				{
					Key:      "pytorch-replica-type",
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
		delete(templateCopy.Labels, "alibabacloud.com/schedule-admission")
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "pod-group.scheduling.sigs.k8s.io/name", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "pod-group.scheduling.sigs.k8s.io", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "scheduling.x-k8s.io/pod-group", framework.Suffix)
		modifyGroupLabelsOrAnnotations(templateCopy.Labels, "network-topology-job-name", framework.Suffix)
		for i := 0; i < replicas; i++ {
			resv := koordinatorschedulerv1alpha1.Reservation{
				ObjectMeta: metav1.ObjectMeta{
					Name:   fmt.Sprintf("%v-%v-%v", job.Name, strings.ToLower(string(role)), i),
					Labels: make(map[string]string),
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

func (j *PytorchJob) GetNetworkTopologyNamespaceName(ctx context.Context, o client.Object) (string, string) {
	job, ok := o.(*pytorchv1.PyTorchJob)
	if !ok {
		return "", ""
	}
	masterTemplate := job.Spec.PyTorchReplicaSpecs[pytorchv1.PyTorchReplicaTypeMaster]
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

func (j *PytorchJob) GetJobNetworkTopologyCR(ctx context.Context, o client.Object, client client.Client) (jnt *networkv1alpha1.JobNetworkTopology, err error) {
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

func (j *PytorchJob) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*pytorchv1.PyTorchJob)
	totalResources := v1.ResourceList{}
	// calculate the total resource request
	for _, replicaSpec := range job.Spec.PyTorchReplicaSpecs {
		// get different roles and calculate the sum of the pods belongs to the same role
		count := int(*replicaSpec.Replicas)
		req := util.GetPodRequestsAndLimits(&replicaSpec.Template.Spec)
		for i := 0; i < count; i++ {
			util.AddResourceList(totalResources, req)
		}
	}
	return totalResources
}

func (j *PytorchJob) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*pytorchv1.PyTorchJob)
	var priorityClassName string
	var priority *int32
	for role := range job.Spec.PyTorchReplicaSpecs {
		if r := strings.ToLower(string(role)); r == util.AIMASTERROLENAME {
			klog.Infof("skip search priority in role %v for job %v", role, job.Name)
			continue
		}
		priorityClassName = job.Spec.PyTorchReplicaSpecs[role].Template.Spec.PriorityClassName
		priority = job.Spec.PyTorchReplicaSpecs[role].Template.Spec.Priority
		// By default, we think that the PriorityClassName and priority of all roles are the same,
		// so just take the value of one role and break
		break
	}

	// If there is a related priorityClassInstance in K8s, we use priorityClass's value instead of pytorchjob.Spec.TFReplicaSpecs[role].Template.Spec.Priority
	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.c.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
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

func (j *PytorchJob) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*pytorchv1.PyTorchJob)
	for roleName, roleSpec := range job.Spec.PyTorchReplicaSpecs {
		if roleName == util.AIMASTERROLENAME {
			continue
		}
		util.SetPodTemplateSpec(&roleSpec.Template, job.Namespace, job.Name, strings.ToLower(string(roleName)), j.QueueUnitSuffix())
	}
	if err := cli.Update(ctx, job, &client.UpdateOptions{}); err != nil {
		return err
	}

	old := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: commonv1.JobStatus{Conditions: util.SliceCopy(job.Status.Conditions)}}
	if !setCondition(&new.Status, "Queuing", v1.ConditionTrue, "Job Enqueued") {
		return nil
	}
	return cli.SubResource("status").Patch(ctx, new, client.MergeFrom(old))
}

func (j *PytorchJob) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "py-qu"
}

func (j *PytorchJob) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*pytorchv1.PyTorchJob)
	if job.Annotations[QueueAnnotation] == "true" {
		return nil
	}

	old := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
	new := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta}
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
	if err := j.deleteJobResources(job); err.Error() == "pod has been scheduled" {
		new.Annotations[QueueAnnotation] = "false"
		return cli.Patch(ctx, new, client.MergeFrom(old))
	} else {
		return err
	}
}

func (r *PytorchJob) deleteJobResources(pytorchjob *pytorchv1.PyTorchJob) error {
	pods, err := r.GetPodsForJob(pytorchjob)
	filteredPods := []*v1.Pod{}
	for _, p := range pods {
		if p.Labels["replica-type"] == util.AIMASTERROLENAME {
			continue
		}
		if p.Spec.NodeName != "" {
			return fmt.Errorf("pod has been scheduled")
		}
		filteredPods = append(filteredPods, p)
	}

	if err != nil {
		klog.V(3).Infof("getPodsForpytorchjob error %v", err)
		return err
	}

	services, err := r.GetServicesForJob(pytorchjob)

	if err != nil {
		klog.V(3).Infof("getServicesForpytorchjob error %v", err)
		return err
	}

	if err := r.deletePodsAndServices(pytorchjob, filteredPods, services); err != nil {
		return err
	}

	if err := r.DeletePodGroup(pytorchjob); err != nil {
		return err
	}
	return nil
}

func (r *PytorchJob) GenLabels(jobName string) map[string]string {
	groupName := "kubeflow.org"
	return map[string]string{
		"group-name": groupName,
		"job-name":   strings.Replace(jobName, "/", "-", -1),
	}
}

func (r *PytorchJob) GetPodsForJob(obj interface{}) ([]*v1.Pod, error) {
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
		fresh := &pytorchv1.PyTorchJob{}
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
	cm := control.NewPodControllerRefManager(r.podControl, job, labelSelector, pytorchv1.SchemeGroupVersionKind, canAdoptFunc)
	return cm.ClaimPods(podList)
}

func (r *PytorchJob) GetServicesForJob(obj interface{}) ([]*v1.Service, error) {
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
		fresh := &pytorchv1.PyTorchJob{}
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
	cm := control.NewServiceControllerRefManager(r.svcControl, job, labelSelector, pytorchv1.SchemeGroupVersionKind, canAdoptFunc)
	return cm.ClaimServices(servicesList)
}

// deletePodsAndServices deletes all the pods and master service.
func (pc *PytorchJob) deletePodsAndServices(job *pytorchv1.PyTorchJob, pods []*v1.Pod, services []*v1.Service) error {
	if len(pods) == 0 {
		return nil
	}

	for _, pod := range pods {
		if err := pc.podControl.DeletePod(pod.Namespace, pod.Name, job); err != nil {
			return err
		}
	}

	rt := strings.ToLower(string(pytorchv1.PyTorchReplicaTypeMaster))
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

func (r *PytorchJob) DeletePodGroup(job metav1.Object) error {
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
func (pc *PytorchJob) FilterServicesForReplicaType(services []*v1.Service, replicaType string) ([]*v1.Service, error) {
	var result []*v1.Service

	replicaSelector := &metav1.LabelSelector{
		MatchLabels: make(map[string]string),
	}

	replicaSelector.MatchLabels["pytorch-replica-type"] = replicaType

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

func (j *PytorchJob) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*pytorchv1.PyTorchJob)

	old := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: job.Status}
	new := &pytorchv1.PyTorchJob{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Status: commonv1.JobStatus{Conditions: util.SliceCopy(job.Status.Conditions)}}
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
	new.Annotations["kube-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *PytorchJob) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*pytorchv1.PyTorchJob)
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

func (j *PytorchJob) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*pytorchv1.PyTorchJob)
	for _, condition := range job.Status.Conditions {
		if condition.Type == "Queuing" {
			return true
		}
	}
	return job.Status.StartTime == nil && job.Annotations[QueueAnnotation] == "true"
}

func NewPytorchJobReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	c := kubernetes.NewForConfigOrDie(config)
	j := &PytorchJob{c: cli,
		managedAllJobs: managedAllJobs,
		podControl:     control.RealPodControl{KubeClient: c, Recorder: record.NewBroadcaster().NewRecorder(scheme, v1.EventSource{Component: "pytorch-opeartor-extension"})},
		svcControl:     control.RealServiceControl{KubeClient: c, Recorder: record.NewBroadcaster().NewRecorder(scheme, v1.EventSource{Component: "pytorch-opeartor-extension"})},
	}
	pytorchv1.AddToScheme(scheme)
	extension := framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)

	op := tfOption{}
	err := yaml.Unmarshal([]byte(args), &op)
	if err != nil {
		log.Fatalf("failed to parse args for pytorchjob extension, content:%v", args)
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
