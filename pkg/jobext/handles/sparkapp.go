package handles

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	kv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type SparkApplication struct {
	c client.Client

	managedAllJobs bool

	getPods func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error)
}

func (j *SparkApplication) Object() client.Object {
	return &v1beta2.SparkApplication{}
}
func (j *SparkApplication) DeepCopy(o client.Object) client.Object {
	job, _ := o.(*v1beta2.SparkApplication)
	return job.DeepCopy()
}

func (j *SparkApplication) GVK() schema.GroupVersionKind {
	return v1beta2.SchemeGroupVersion.WithKind("SparkApplication")
}

func getExecutorRequestResource(app *v1beta2.SparkApplication) v1.ResourceList {
	minResource := v1.ResourceList{}

	//CoreRequest correspond to executor's core request
	if app.Spec.Executor.CoreRequest != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreRequest); err == nil {
			minResource[v1.ResourceCPU] = value
		}
	}

	//Use Core attribute if CoreRequest is empty
	if app.Spec.Executor.Cores != nil {
		if _, ok := minResource[v1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Executor.Cores)); err == nil {
				minResource[v1.ResourceCPU] = value
			}
		}
	}
	//Cores correspond to driver's core request
	if app.Spec.Driver.CoreRequest != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.CoreRequest); err == nil {
			minResource[v1.ResourceCPU] = value
		}
	}

	//CoreLimit correspond to executor's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Executor.CoreLimit != nil {
		if _, ok := minResource[v1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreLimit); err == nil {
				minResource[v1.ResourceCPU] = value
			}
		}
	}

	//Memory + MemoryOverhead correspond to executor's memory request
	if app.Spec.Executor.Memory != nil {
		*app.Spec.Executor.Memory = strings.Replace(*app.Spec.Executor.Memory, "m", "Mi", -1)
		*app.Spec.Executor.Memory = strings.Replace(*app.Spec.Executor.Memory, "g", "Gi", -1)
		*app.Spec.Executor.Memory = strings.Replace(*app.Spec.Executor.Memory, "t", "Ti", -1)
		if value, err := resource.ParseQuantity(*app.Spec.Executor.Memory); err == nil {
			minResource[v1.ResourceMemory] = value
		}
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		*app.Spec.Executor.MemoryOverhead = strings.Replace(*app.Spec.Executor.MemoryOverhead, "m", "Mi", -1)
		*app.Spec.Executor.MemoryOverhead = strings.Replace(*app.Spec.Executor.MemoryOverhead, "g", "Gi", -1)
		*app.Spec.Executor.MemoryOverhead = strings.Replace(*app.Spec.Executor.MemoryOverhead, "t", "Ti", -1)
		if value, err := resource.ParseQuantity(*app.Spec.Executor.MemoryOverhead); err == nil {
			if existing, ok := minResource[v1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[v1.ResourceMemory] = existing
			}
		}
	}

	resourceList := []v1.ResourceList{{}}
	executorsCount := int32(1)
	if app.Spec.Executor.Instances != nil {
		executorsCount = *app.Spec.Executor.Instances
	}
	if app.Spec.Executor.Instances == nil && app.Spec.DynamicAllocation != nil &&
		app.Spec.DynamicAllocation.InitialExecutors != nil {
		executorsCount = *app.Spec.DynamicAllocation.InitialExecutors
	}
	for i := int32(0); i < executorsCount; i++ {
		resourceList = append(resourceList, minResource)
	}
	return sumResourceList(resourceList)
}

func getDriverRequestResource(app *v1beta2.SparkApplication) v1.ResourceList {
	minResource := v1.ResourceList{}

	//Cores correspond to driver's core request
	if app.Spec.Driver.Cores != nil {
		if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Driver.Cores)); err == nil {
			minResource[v1.ResourceCPU] = value
		}
	}

	//Cores correspond to driver's core request
	if app.Spec.Driver.CoreRequest != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.CoreRequest); err == nil {
			minResource[v1.ResourceCPU] = value
		}
	}

	//CoreLimit correspond to driver's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Driver.CoreLimit != nil {
		if _, ok := minResource[v1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Driver.CoreLimit); err == nil {
				minResource[v1.ResourceCPU] = value
			}
		}
	}

	//Memory + MemoryOverhead correspond to driver's memory request
	if app.Spec.Driver.Memory != nil {
		*app.Spec.Driver.Memory = strings.Replace(*app.Spec.Driver.Memory, "m", "Mi", -1)
		*app.Spec.Driver.Memory = strings.Replace(*app.Spec.Driver.Memory, "g", "Gi", -1)
		*app.Spec.Driver.Memory = strings.Replace(*app.Spec.Driver.Memory, "t", "Ti", -1)
		if value, err := resource.ParseQuantity(*app.Spec.Driver.Memory); err == nil {
			minResource[v1.ResourceMemory] = value
		}
	}
	if app.Spec.Driver.MemoryOverhead != nil {
		*app.Spec.Driver.MemoryOverhead = strings.Replace(*app.Spec.Driver.MemoryOverhead, "m", "Mi", -1)
		*app.Spec.Driver.MemoryOverhead = strings.Replace(*app.Spec.Driver.MemoryOverhead, "g", "Gi", -1)
		*app.Spec.Driver.MemoryOverhead = strings.Replace(*app.Spec.Driver.MemoryOverhead, "t", "Ti", -1)
		if value, err := resource.ParseQuantity(*app.Spec.Driver.MemoryOverhead); err == nil {
			if existing, ok := minResource[v1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[v1.ResourceMemory] = existing
			}
		}
	}

	return minResource
}

func sumResourceList(list []v1.ResourceList) v1.ResourceList {
	totalResource := v1.ResourceList{}
	for _, l := range list {
		for name, quantity := range l {

			if value, ok := totalResource[name]; !ok {
				totalResource[name] = quantity.DeepCopy()
			} else {
				value.Add(quantity)
				totalResource[name] = value
			}
		}
	}
	return totalResource
}

type mergePolicy string

var ignore mergePolicy = "ignore"

func mergeMaps(into, in map[string]string, policy mergePolicy) {
	for k, v := range in {
		if _, ok := into[k]; policy == ignore && ok {
			continue
		}
		into[k] = v
	}
}

func getResourceRequestsFromDriverSpec(app *v1beta2.DriverSpec) v1.ResourceRequirements {
	res := v1.ResourceRequirements{Requests: make(v1.ResourceList), Limits: make(v1.ResourceList)}
	if app.CoreRequest != nil {
		res.Requests["cpu"] = resource.MustParse(*app.CoreRequest)
	}
	if app.CoreLimit != nil {
		res.Limits["cpu"] = resource.MustParse(*app.CoreLimit)
	}
	//Memory + MemoryOverhead correspond to driver's memory request
	if app.Memory != nil {
		if value, err := resource.ParseQuantity(*app.Memory); err == nil {
			res.Requests[v1.ResourceMemory] = value
		}
	}
	if app.MemoryOverhead != nil {
		if value, err := resource.ParseQuantity(*app.MemoryOverhead); err == nil {
			if existing, ok := res.Requests[v1.ResourceMemory]; ok {
				existing.Add(value)
				res.Requests[v1.ResourceMemory] = existing
			}
		}
	}
	if app.GPU != nil {
		res.Limits[v1.ResourceName(app.GPU.Name)] = *resource.NewQuantity(app.GPU.Quantity, resource.DecimalSI)
	}
	return res
}

func getResourceRequestsFromExecutorSpec(app *v1beta2.ExecutorSpec) v1.ResourceRequirements {
	res := v1.ResourceRequirements{Requests: make(v1.ResourceList), Limits: make(v1.ResourceList)}
	if app.CoreRequest != nil {
		res.Requests["cpu"] = resource.MustParse(*app.CoreRequest)
	}
	if app.CoreLimit != nil {
		res.Limits["cpu"] = resource.MustParse(*app.CoreLimit)
	}
	if app.GPU != nil {
		res.Limits[v1.ResourceName(app.GPU.Name)] = *resource.NewQuantity(app.GPU.Quantity, resource.DecimalSI)
	}
	return res
}

func buildPodTemplateFromSparkPodSpec(app *v1beta2.SparkPodSpec, temp *v1.PodTemplateSpec) {
	temp.Spec.NodeSelector = util.MapCopy(app.NodeSelector)
	temp.Spec.Affinity = app.Affinity.DeepCopy()
	temp.Spec.Tolerations = util.SliceCopy(app.Tolerations)
	temp.Spec.InitContainers = util.SliceCopy(app.InitContainers)
	temp.Spec.Containers = append(temp.Spec.Containers, app.Sidecars...)
	mergeMaps(temp.Labels, app.Labels, ignore)
	mergeMaps(temp.Annotations, app.Annotations, ignore)
}

func buildDriverPodTemplateFromSparkApp(app *v1beta2.DriverSpec) v1.PodTemplateSpec {
	res := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
	}

	res.Spec.Containers = []v1.Container{
		{
			Resources: getResourceRequestsFromDriverSpec(app),
			Ports:     []v1.ContainerPort{},
		},
	}
	for _, port := range app.Ports {
		res.Spec.Containers[0].Ports = append(res.Spec.Containers[0].Ports, v1.ContainerPort{
			ContainerPort: port.ContainerPort,
			Protocol:      v1.Protocol(port.Protocol),
			Name:          port.Name,
		})
	}
	buildPodTemplateFromSparkPodSpec(&app.SparkPodSpec, &res)
	return res
}

func buildExecutorPodTemplateFromSparkApp(app *v1beta2.ExecutorSpec) v1.PodTemplateSpec {
	res := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
	}

	res.Spec.Containers = []v1.Container{
		{
			Resources: getResourceRequestsFromExecutorSpec(app),
			Ports:     []v1.ContainerPort{},
		},
	}
	for _, port := range app.Ports {
		res.Spec.Containers[0].Ports = append(res.Spec.Containers[0].Ports, v1.ContainerPort{
			ContainerPort: port.ContainerPort,
			Protocol:      v1.Protocol(port.Protocol),
			Name:          port.Name,
		})
	}
	buildPodTemplateFromSparkPodSpec(&app.SparkPodSpec, &res)
	return res
}

func (j *SparkApplication) GetPodSetName(ownerName string, p *v1.Pod) string {
	return p.Labels["spark-role"]
}

func (j *SparkApplication) PodSet(ctx context.Context, obj client.Object) []kueue.PodSet {
	job := obj.(*v1beta2.SparkApplication)
	ps := []kueue.PodSet{}
	if job.Spec.Mode == v1beta2.ClusterMode {
		ps = append(ps, kueue.PodSet{
			Name:     "driver",
			Template: buildDriverPodTemplateFromSparkApp(&job.Spec.Driver),
			Count:    1,
		})
	}
	executorPodSet := kueue.PodSet{
		Name:     "executor",
		Count:    1,
		Template: buildExecutorPodTemplateFromSparkApp(&job.Spec.Executor),
	}
	if job.Spec.Executor.Instances != nil {
		executorPodSet.Count = *job.Spec.Executor.Instances
	}
	ps = append(ps, executorPodSet)
	// 使用实际的Pod数量计算请求的资源量
	pods := &v1.PodList{}
	replicasByPods := map[string]int32{}
	if err := j.c.List(ctx, pods, client.MatchingLabels{"spark-app-name": job.Name, "spark-role": "driver"}); err != nil {
		log := klog.FromContext(ctx)
		log.Error(err, "Failed to list driver pods")
		replicasByPods["driver"] = 1
	} else {
		replicasByPods["driver"] = int32(len(pods.Items))
	}
	if err := j.c.List(ctx, pods, client.MatchingLabels{"spark-app-name": job.Name, "spark-role": "executor"}); err != nil {
		log := klog.FromContext(ctx)
		log.Error(err, "Failed to list executor pods")
		replicasByPods["executor"] = executorPodSet.Count
	} else {
		count := int32(0)
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
				continue
			}
			count++
		}
		replicasByPods["executor"] = count
	}

	for _, p := range ps {
		if p.Count < replicasByPods[p.Name] {
			p.Count = replicasByPods[p.Name]
		}
	}
	return ps
}

func (j *SparkApplication) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	job := obj.(*v1beta2.SparkApplication)
	var totalResources v1.ResourceList
	// calculate the total resource request
	if job.Spec.Mode == v1beta2.ClientMode {
		totalResources = getExecutorRequestResource(job)
	} else {
		totalResources = sumResourceList([]v1.ResourceList{getExecutorRequestResource(job), getDriverRequestResource(job)})
	}
	return totalResources
}

func (j *SparkApplication) QueueUnitSuffix() string {
	if os.Getenv("PAI_ENV") != "" {
		return ""
	}
	return "spark-qu"
}

func (j *SparkApplication) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	job := obj.(*v1beta2.SparkApplication)
	var priorityClassName string
	var priority *int32
	if job.Spec.BatchSchedulerOptions != nil && job.Spec.BatchSchedulerOptions.PriorityClassName != nil {
		priorityClassName = *job.Spec.BatchSchedulerOptions.PriorityClassName
	}

	if priorityClassName != "" {
		var priorityClassInstance = &schedulingv1.PriorityClass{}
		err := j.c.Get(context.Background(), types.NamespacedName{Namespace: job.Namespace, Name: priorityClassName}, priorityClassInstance)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Infof("Can not find priority class name %v in k8s, we will ignore it", priorityClassName)
			} else {
				klog.Errorf("Can not get PriorityClass %v from k8s for sparkApp:%v/%v, err:%v", priorityClassName, job.Namespace, job.Name, err)
			}
		} else {
			priority = &priorityClassInstance.Value
		}
	}
	return priorityClassName, priority
}

func (j *SparkApplication) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1beta2.SparkApplication)
	job.TypeMeta.APIVersion = v1beta2.SchemeGroupVersion.String()
	job.TypeMeta.Kind = "SparkApplication"
	if job.Annotations["kube-queue/job-enqueue-timestamp"] != "" {
		return nil
	}

	old := job
	new := job.DeepCopy()
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.ObjectMeta.Annotations["kube-queue/job-enqueue-timestamp"] = time.Now().String()
	if len(new.Spec.Driver.SparkPodSpec.Labels) == 0 {
		new.Spec.Driver.SparkPodSpec.Labels = make(map[string]string)
	}
	new.Spec.Driver.SparkPodSpec.Labels[util.SchedulerAdmissionLabelKey] = "false"
	if len(new.Spec.Driver.SparkPodSpec.Annotations) == 0 {
		new.Spec.Driver.SparkPodSpec.Annotations = make(map[string]string)
	}
	if j.QueueUnitSuffix() != "" {
		new.Spec.Driver.SparkPodSpec.Annotations[util.RelatedQueueUnitAnnoKey] = job.Namespace + "/" + job.Name + "-" + j.QueueUnitSuffix()
	} else {
		new.Spec.Driver.SparkPodSpec.Annotations[util.RelatedQueueUnitAnnoKey] = job.Namespace + "/" + job.Name
	}
	new.Spec.Driver.SparkPodSpec.Annotations[util.RelatedPodSetAnnoKey] = "driver"
	if len(new.Spec.Executor.SparkPodSpec.Labels) == 0 {
		new.Spec.Executor.SparkPodSpec.Labels = make(map[string]string)
	}
	new.Spec.Executor.SparkPodSpec.Labels[util.SchedulerAdmissionLabelKey] = "false"
	if len(new.Spec.Executor.SparkPodSpec.Annotations) == 0 {
		new.Spec.Executor.SparkPodSpec.Annotations = make(map[string]string)
	}
	new.Spec.Executor.SparkPodSpec.Annotations[util.RelatedQueueUnitAnnoKey] = job.Namespace + "/" + job.Name + "-" + j.QueueUnitSuffix()
	new.Spec.Driver.SparkPodSpec.Annotations[util.RelatedPodSetAnnoKey] = "executor"
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *SparkApplication) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1beta2.SparkApplication)
	job.TypeMeta.APIVersion = v1beta2.SchemeGroupVersion.String()
	job.TypeMeta.Kind = "SparkApplication"
	if job.Annotations[QueueAnnotation] == "true" {
		return nil
	}

	old := &v1beta2.SparkApplication{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new := &v1beta2.SparkApplication{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec}
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	new.ObjectMeta.Annotations[QueueAnnotation] = "true"
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *SparkApplication) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	job := obj.(*v1beta2.SparkApplication)
	job.TypeMeta.APIVersion = v1beta2.SchemeGroupVersion.String()
	job.TypeMeta.Kind = "SparkApplication"
	if job.Annotations[QueueAnnotation] != "true" {
		return nil
	}

	old := &v1beta2.SparkApplication{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new := &v1beta2.SparkApplication{TypeMeta: job.TypeMeta, ObjectMeta: job.ObjectMeta, Spec: job.Spec, Status: job.Status}
	new.ObjectMeta.Annotations = util.MapCopy(job.ObjectMeta.Annotations)
	if len(new.Annotations) == 0 {
		new.Annotations = map[string]string{}
	}
	new.ObjectMeta.Annotations[QueueAnnotation] = "false"
	new.Annotations["kube-queue/job-dequeue-timestamp"] = time.Now().String()
	return cli.Patch(ctx, new, client.MergeFrom(old))
}

func (j *SparkApplication) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	job := obj.(*v1beta2.SparkApplication)
	switch job.Status.AppState.State {
	case v1beta2.CompletedState:
		return framework.Succeeded, job.Status.TerminationTime.Time
	case v1beta2.FailedState:
		return framework.Failed, job.Status.TerminationTime.Time
	case v1beta2.RunningState, v1beta2.FailingState, v1beta2.SucceedingState:
		return framework.Running, job.Status.LastSubmissionAttemptTime.Time
	}

	if value, ok := job.Annotations[QueueAnnotation]; !ok || (ok && value == "false") {
		dequeueTransTime, err := time.Parse(timeFormat, job.Annotations["kube-queue/job-dequeue-timestamp"])
		if err != nil {
			dequeueTransTime = time.Now()
		}
		return framework.Pending, dequeueTransTime
	}

	if job.Annotations["kube-queue/job-enqueue-timestamp"] != "" {
		queuingTransTime, err := time.Parse(timeFormat, job.Annotations["kube-queue/job-enqueue-timestamp"])
		if err != nil {
			queuingTransTime = time.Now()
		}
		return framework.Queuing, queuingTransTime
	}

	return framework.Created, job.CreationTimestamp.Time
}

func (j *SparkApplication) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	if j.managedAllJobs {
		return true
	}
	job := obj.(*v1beta2.SparkApplication)
	if job.Annotations["kube-queue/job-enqueue-timestamp"] != "" {
		return true
	}
	return job.Status.AppState.State == "" && job.Annotations[QueueAnnotation] == "true"
}

func (j *SparkApplication) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return j.getPods
}

func NewSparkAppReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	var extension framework.GenericJobExtension
	j := &SparkApplication{
		managedAllJobs: managedAllJobs, c: cli, getPods: func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
			pl := &v1.PodList{}
			if err := cli.List(ctx, pl, client.InNamespace(namespaceName.Namespace), client.MatchingLabels{"spark-app-name": namespaceName.Name}); client.IgnoreNotFound(err) != nil {
				return nil, err
			}
			pods := []*v1.Pod{}
			for _, pod := range pl.Items {
				pods = append(pods, &pod)
			}
			return pods, nil
		}}
	v1beta2.AddToScheme(scheme)
	extension = framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, false)
}

var _ framework.GenericReservationJobExtension = &SparkApplication{}

func (j *SparkApplication) ReservationStatus(ctx context.Context, obj client.Object, qu *kv1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
	return framework.ReservationStatus(ctx, obj, qu, resvs)
}

func (j *SparkApplication) Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error) {
	job := obj.(*v1beta2.SparkApplication)
	driverTmpl := buildDriverPodTemplateFromSparkApp(&job.Spec.Driver)
	for i, cont := range driverTmpl.Spec.Containers {
		if len(cont.Resources.Requests) == 0 {
			driverTmpl.Spec.Containers[i].Resources.Requests = driverTmpl.Spec.Containers[i].Resources.Limits
		}
	}
	driverTmpl.Namespace = job.Namespace
	resvs := []koordinatorschedulerv1alpha1.Reservation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%v-%v-%v", job.Name, "driver", 0),
			},
			Spec: koordinatorschedulerv1alpha1.ReservationSpec{
				Template: ptr.To(driverTmpl),
				Owners: []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"spark-app-name": job.Name,
						"spark-role":     "driver",
					},
				}}},
			},
		},
	}
	executorCount := int32(1)
	if job.Spec.Executor.Instances != nil {
		executorCount = *job.Spec.Executor.Instances
	}
	execTmpl := buildExecutorPodTemplateFromSparkApp(&job.Spec.Executor)
	for i, cont := range execTmpl.Spec.Containers {
		if len(cont.Resources.Requests) == 0 {
			execTmpl.Spec.Containers[i].Resources.Requests = execTmpl.Spec.Containers[i].Resources.Limits
		}
	}
	driverTmpl.Namespace = job.Namespace
	for i := int32(0); i < executorCount; i++ {
		labelSelector := &metav1.LabelSelector{}
		labelSelector.MatchLabels = map[string]string{
			"spark-app-name": job.Name,
			"spark-role":     "driver",
		}
		resv := koordinatorschedulerv1alpha1.Reservation{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%v-%v-%v", job.Name, "executor", i),
			},
			Spec: koordinatorschedulerv1alpha1.ReservationSpec{
				Template: ptr.To(execTmpl),
				Owners:   []koordinatorschedulerv1alpha1.ReservationOwner{{LabelSelector: labelSelector}},
			},
		}
		resv.Namespace = job.Namespace
		resvs = append(resvs, resv)
	}
	return resvs, nil
}
