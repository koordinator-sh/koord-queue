package framework

import (
	"context"
	"strings"
	"sync"
	"time"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/trace"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var EnableReservation bool
var EnableNetworkAware bool
var EnablePodReclaim bool
var DefaultRequeuePeriod = time.Second * 60
var Suffix = "re"

type resourceVersionWithTime struct {
	ResourceVersion string
	Time            time.Time
}

type GenericJobReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	jobHandles map[string]JobHandle

	lock                  sync.Mutex
	processedReservations map[string]int
	lastSeenResrvations   map[string]resourceVersionWithTime
	// ConstructQueueunits func(ctx context.Context, obj client.Object, client client.Client) []*v1alpha1.QueueUnit
	eventRecorder record.EventRecorder
}

func NewJobReconcilerWithJobExtension(cli client.Client, scheme *runtime.Scheme, handles ...JobHandle) *GenericJobReconciler {
	r := &GenericJobReconciler{
		client: cli,
		scheme: scheme,

		jobHandles:            map[string]JobHandle{},
		processedReservations: map[string]int{},
		lastSeenResrvations:   map[string]resourceVersionWithTime{},
	}

	for _, handle := range handles {
		r.jobHandles[handle.typeName] = handle
	}
	r.eventRecorder = record.NewBroadcaster().NewRecorder(scheme, corev1.EventSource{Component: "pytorch-opeartor-extension"})

	return r
}

// SetupWithManager sets up the controller with the Manager.
func (d *GenericJobReconciler) SetupWithManager(mgr ctrl.Manager, workers int, wqQPS int) error {
	d.processedReservations = map[string]int{}
	d.lastSeenResrvations = map[string]resourceVersionWithTime{}
	build := ctrl.NewControllerManagedBy(mgr).Named("generic-job-controller")
	for _, handle := range d.jobHandles {
		build.Watches(handle.genericJobExtension.Object(), handler.Funcs{
			CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				obj := tce.Object
				gvk := obj.GetObjectKind().GroupVersionKind()
				if gvk.Group == "" {
					err := d.client.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
					if err != nil {
						return
					}
					gvk = obj.GetObjectKind().GroupVersionKind()
				}
				jobTypeKey := gvk.Group + "/" + gvk.Version + "/" + gvk.Kind
				if jobHandle, ok := d.jobHandles[jobTypeKey]; ok {
					if jobHandle.genericJobExtension.ManagedByQueue(ctx, obj) {
						rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobTypeKey + "|" + obj.GetNamespace(), Name: obj.GetName()}})
					}
				}
			},
			UpdateFunc: func(ctx context.Context, tue event.TypedUpdateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				obj := tue.ObjectNew
				gvk := obj.GetObjectKind().GroupVersionKind()
				if gvk.Group == "" {
					err := d.client.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
					if err != nil {
						return
					}
					gvk = obj.GetObjectKind().GroupVersionKind()
				}
				jobTypeKey := gvk.Group + "/" + gvk.Version + "/" + gvk.Kind
				if jobHandle, ok := d.jobHandles[jobTypeKey]; ok {
					if jobHandle.genericJobExtension.ManagedByQueue(ctx, obj) {
						rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobTypeKey + "|" + obj.GetNamespace(), Name: obj.GetName()}})
					}
				}
			},
			DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				obj := tde.Object
				gvk := obj.GetObjectKind().GroupVersionKind()
				if gvk.Group == "" {
					err := d.client.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj)
					if err != nil {
						return
					}
					gvk = obj.GetObjectKind().GroupVersionKind()
				}
				jobTypeKey := gvk.Group + "/" + gvk.Version + "/" + gvk.Kind
				if jobHandle, ok := d.jobHandles[jobTypeKey]; ok {
					if jobHandle.genericJobExtension.ManagedByQueue(ctx, obj) {
						rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobTypeKey + "|" + obj.GetNamespace(), Name: obj.GetName()}})
					}
				}
			},
		})
	}
	return build.Watches(&v1alpha1.QueueUnit{}, handler.Funcs{
		CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tce.Object)
			if err != nil {
				return
			}
			if qu.Status.Phase == v1alpha1.Running {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
		UpdateFunc: func(ctx context.Context, tue event.TypedUpdateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tue.ObjectNew)
			if err != nil {
				return
			}
			if qu.Status.Phase == v1alpha1.Running {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
		DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tde.Object)
			if err != nil {
				return
			}
			if qu.Status.Phase == v1alpha1.Running {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
	}).WithOptions(controller.Options{MaxConcurrentReconciles: workers, RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](50*time.Millisecond, 1000*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(wqQPS), 1000)},
	)}).Complete(d)
}

func (d *GenericJobReconciler) updateQueueUnitReplicas(ctx context.Context, handle JobHandle, object client.Object, qu *v1alpha1.QueueUnit) (updated bool, err error) {
	// do not edit job spec after submit, otherwise this logic will not run formally
	expectPodSet := handle.genericJobExtension.PodSet(ctx, object)
	actualPodSet := qu.Spec.PodSets
	actualPodSetMap := map[string]int32{}
	for _, podSet := range actualPodSet {
		actualPodSetMap[podSet.Name] = podSet.Count
	}
	for _, podSet := range expectPodSet {
		if actualPodSetMap[podSet.Name] != podSet.Count {
			updated = true
		}
	}
	if !updated {
		return false, nil
	}

	for i, podSet := range expectPodSet {
		for j, ct := range podSet.Template.Spec.Containers {
			if len(ct.Resources.Requests) == 0 && len(ct.Resources.Limits) != 0 {
				expectPodSet[i].Template.Spec.Containers[j].Resources.Requests = ct.Resources.Limits
			}
		}
		for j, ct := range podSet.Template.Spec.InitContainers {
			if len(ct.Resources.Requests) == 0 && len(ct.Resources.Limits) != 0 {
				expectPodSet[i].Template.Spec.InitContainers[j].Resources.Requests = ct.Resources.Limits
			}
		}
	}
	qu.Spec.PodSets = expectPodSet
	if qu.Status.Phase != v1alpha1.Running {
		resource := handle.genericJobExtension.Resources(ctx, object)
		qu.Spec.Resource = resource
	}
	err = d.client.Update(ctx, qu)
	return updated, err
}

func (d *GenericJobReconciler) createQueueUnit(ctx context.Context, handle JobHandle, object client.Object, status v1alpha1.QueueUnitPhase, msg string) error {

	resources := handle.genericJobExtension.Resources(ctx, object)
	pc, pri := handle.genericJobExtension.Priority(ctx, object)
	suffix := handle.genericJobExtension.QueueUnitSuffix()
	if suffix != "" {
		suffix = "-" + suffix
	}
	queueUnit := &v1alpha1.QueueUnit{
		ObjectMeta: v1.ObjectMeta{
			Name:      object.GetName() + suffix,
			Namespace: object.GetNamespace(),
			OwnerReferences: []v1.OwnerReference{{
				APIVersion: object.GetObjectKind().GroupVersionKind().GroupVersion().String(),
				Kind:       object.GetObjectKind().GroupVersionKind().Kind,
				Name:       object.GetName(),
				UID:        object.GetUID(),
			}},
			Labels:      object.GetLabels(),
			Annotations: object.GetAnnotations(),
		},
		Spec: v1alpha1.QueueUnitSpec{
			ConsumerRef: &corev1.ObjectReference{
				APIVersion: object.GetObjectKind().GroupVersionKind().GroupVersion().String(),
				Kind:       object.GetObjectKind().GroupVersionKind().Kind,
				Name:       object.GetName(),
				Namespace:  object.GetNamespace(),
			},
			Resource:          resources,
			Request:           resources,
			PriorityClassName: pc,
			Priority:          pri,
			PodSets:           handle.genericJobExtension.PodSet(ctx, object),
		},
		Status: v1alpha1.QueueUnitStatus{
			Phase:          status,
			Message:        msg,
			LastUpdateTime: &v1.Time{Time: time.Now()},
		},
	}
	for i, podSet := range queueUnit.Spec.PodSets {
		for j, ct := range podSet.Template.Spec.Containers {
			if len(ct.Resources.Requests) == 0 && len(ct.Resources.Limits) != 0 {
				queueUnit.Spec.PodSets[i].Template.Spec.Containers[j].Resources.Requests = ct.Resources.Limits
			}
		}
		for j, ct := range podSet.Template.Spec.InitContainers {
			if len(ct.Resources.Requests) == 0 && len(ct.Resources.Limits) != 0 {
				queueUnit.Spec.PodSets[i].Template.Spec.InitContainers[j].Resources.Requests = ct.Resources.Limits
			}
		}
	}
	return d.client.Create(ctx, queueUnit)
}

func (d *GenericJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("job", req.String())
	infos := strings.Split(req.Namespace, "|")
	if len(infos) != 2 {
		log.Info("invalid request", "req", req.String())
		return ctrl.Result{}, nil
	}
	jobTypeKey := infos[0]
	log.WithValues("gvk", jobTypeKey)

	namespace := infos[1]
	name := req.Name
	req = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
	handle, ok := d.jobHandles[jobTypeKey]
	if !ok {
		log.Info("unknown job type", "gvk", jobTypeKey)
		return ctrl.Result{}, nil
	}

	object := handle.genericJobExtension.Object()
	log.V(4).Info("reconcile start")
	defer log.V(4).Info("reconcile end")

	err := d.client.Get(ctx, req.NamespacedName, object)
	if apierrors.IsNotFound(err) || !object.GetDeletionTimestamp().IsZero() {
		log.Info("obj is deleted", "job", req.String())
		qu := &v1alpha1.QueueUnit{}
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name + "-" + handle.genericJobExtension.QueueUnitSuffix()}, qu); err == nil {
			log.V(3).Info("try to delete queueunit", "queueunit", req.String())
			err = d.client.Delete(context.Background(), qu)
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{}, nil
	}

	if !handle.genericJobExtension.ManagedByQueue(ctx, object) {
		log.V(2).Info("job is not managed by queue", "job", object.GetName())
		return ctrl.Result{}, nil
	}

	jobStatus, _ := handle.genericJobExtension.GetJobStatus(ctx, object, d.client)
	log.V(3).Info("job status", "status", jobStatus)
	if jobStatus == Created {
		if err := handle.genericJobExtension.Enqueue(ctx, object, d.client); err != nil {
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, client.IgnoreNotFound(err)
		}

		log.V(0).Info("create queue unit for created job")
		err = d.createQueueUnit(ctx, handle, object, v1alpha1.Enqueued, "Enqueued")
		if client.IgnoreAlreadyExists(err) != nil {
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, err
		}
		return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
	}

	queueUnit, err := handle.genericJobExtension.GetRelatedQueueUnit(ctx, object, d.client)
	if err != nil {
		if jobStatus != Succeeded && jobStatus != Failed {
			status := v1alpha1.Enqueued
			msg := "Enqueued"
			if jobStatus != Queuing {
				status = v1alpha1.Dequeued
				msg = "Deuqueued"
			}
			log.V(0).Info("create queue unit for created job")
			err = d.createQueueUnit(ctx, handle, object, status, msg)
			if client.IgnoreAlreadyExists(err) != nil {
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, err
			}
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}
		log.V(0).Info("will not create queueunit for terminated job")
		return ctrl.Result{}, nil
	}
	// queueUnit = queueUnit.DeepCopy()
	if updated, err := d.updateQueueUnitReplicas(ctx, handle, object, queueUnit); updated || err != nil {
		return ctrl.Result{Requeue: true}, err
	}
	log = log.WithValues("queueunit", queueUnit.Name, "queueUnitStatus", queueUnit.Status.Phase, "jobStatus", jobStatus)
	trace := trace.New("jobReconciling")
	defer trace.LogIfLong(100 * time.Millisecond)
	switch jobStatus {
	case Queuing:
		if queueUnit.Status.Phase == "Preempted" {
			log.V(0).Info("success to suspend and delete job resources, set job status to Enqueued")
			return ctrl.Result{Requeue: true, RequeueAfter: DefaultRequeuePeriod}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Enqueued, "Enqueued after preempted", d.client)

		}
		if queueUnit.Status.Phase == v1alpha1.SchedReady {
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}
		if queueUnit.Status.Phase == v1alpha1.SchedFailed {
			log.V(2).Info("wait koord-queue to schedule the job again")
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}
		if queueUnit.Status.Phase == v1alpha1.SchedSucceed {
			d.eventRecorder.Event(object, corev1.EventTypeNormal, "Resume", "Job is resumed by job-extension")
			log.V(1).Info("resume generic job due to related queueunit schedSucceed")
			return ctrl.Result{RequeueAfter: handle.runningTimeout}, handle.genericJobExtension.Resume(ctx, object, d.client)
		}
		if queueUnit.Status.Phase == v1alpha1.Dequeued {
			log.V(1).Info("resume generic job due to related queueunit dequeued")
			return ctrl.Result{RequeueAfter: handle.runningTimeout}, handle.genericJobExtension.Resume(ctx, object, d.client)
		}
		var lastUpdateTime time.Time = time.Now()
		if queueUnit.Status.LastUpdateTime != nil {
			lastUpdateTime = queueUnit.Status.LastUpdateTime.Time
		}
		duration := time.Since(lastUpdateTime)
		if queueUnit.Status.Phase == v1alpha1.Backoff && duration > handle.backoffTime {
			err := handle.requeueJobExtension.OnQueueUnitBackoffTimeout(ctx, object, queueUnit, d.client)
			if err != nil {
				return ctrl.Result{}, err
			}
			if queueUnit.Status.Phase == v1alpha1.Enqueued {
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
			}
			queueUnit.Status.Phase = v1alpha1.Enqueued
			queueUnit.Status.Message = "Enqueued because backoff timeout"
			queueUnit.Status.LastUpdateTime = &v1.Time{Time: time.Now()}
			log.V(1).Info("job backoff timeout")
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, d.client.SubResource("status").Update(ctx, queueUnit)
		} else if queueUnit.Status.Phase == v1alpha1.Backoff && duration < handle.backoffTime {
			log.V(1).Info("job will requeue backoff timeout", "after", handle.backoffTime-duration)
			return ctrl.Result{RequeueAfter: handle.backoffTime - duration}, nil
		}
		// status == Enqueued
		if queueUnit.Status.Phase == v1alpha1.Enqueued {
			existingResvs := &koordinatorschedulerv1alpha1.ReservationList{}
			d.client.List(ctx, existingResvs, client.MatchingLabels{
				"reserved-job-namespace": object.GetNamespace(),
				"reserved-job-name":      object.GetName(),
			})
			trace.Step("ListReservations")
			deleted := []string{}
			for _, existingResv := range existingResvs.Items {
				if existingResv.Status.Phase == "Failed" || existingResv.Status.Phase == koordinatorschedulerv1alpha1.ReservationAvailable {
					d.client.Delete(ctx, &existingResv)
					deleted = append(deleted, existingResv.Name)
				}
			}
			if len(deleted) > 0 {
				log.V(1).Info("delete outdated reservations", "toDeleteResv", deleted)
				return ctrl.Result{Requeue: true}, nil
			}
		}
	case Pending:
		log.V(3).Info("job is pending")
		if queueUnit.Status.Phase == v1alpha1.SchedFailed || queueUnit.Status.Phase == v1alpha1.SchedSucceed {
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}
		if handle.requeueJobExtension == nil {
			log.V(2).Info("job do not implement requeue interface")
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}
		if queueUnit.Status.Phase == "Preempted" {
			err := handle.requeueJobExtension.OnQueueUnitRunningTimeout(ctx, object, queueUnit, d.client)
			if err != nil {
				log.V(2).Info("failed to suspend and delete job resources")
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, err
			}

			log.V(2).Info("success to suspend and delete job resources, set job status to Enqueued")
			return ctrl.Result{Requeue: true, RequeueAfter: DefaultRequeuePeriod}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Enqueued, "Enqueued after preempted", d.client)
		}
		if handle.runningTimeout == 0 {
			log.V(3).Info("job's requeue timeout is 0'")
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}

		var lastUpdateTime time.Time
		if queueUnit.Status.LastUpdateTime == nil {
			lastUpdateTime = time.Now()
		} else {
			lastUpdateTime = queueUnit.Status.LastUpdateTime.Time
		}
		duration := time.Since(lastUpdateTime)
		if queueUnit.Status.Phase == v1alpha1.Dequeued {
			if duration >= handle.runningTimeout {
				log.V(0).Info("job running timeout")
				if err := util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Backoff, "Backoff due to running timeout", d.client); err != nil {
					return ctrl.Result{}, client.IgnoreNotFound(err)
				}
				err := handle.requeueJobExtension.OnQueueUnitRunningTimeout(ctx, object, queueUnit, d.client)
				if err != nil {
					return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, err
				}
				return ctrl.Result{Requeue: true, RequeueAfter: handle.backoffTime}, nil
			} else {
				log.V(2).Info("job will requeue running timeout", "after", handle.runningTimeout-duration)
				return ctrl.Result{RequeueAfter: handle.runningTimeout - duration}, nil
			}
		}

		if queueUnit.Status.Phase == v1alpha1.Backoff {
			// update job failed in last reconcile
			err := handle.requeueJobExtension.OnQueueUnitRunningTimeout(ctx, object, queueUnit, d.client)
			if err != nil {
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, err
			}
			// try update queue unit status
			if duration > handle.backoffTime {
				log.V(0).Info("job backoff timeout")
				err := handle.requeueJobExtension.OnQueueUnitBackoffTimeout(ctx, object, queueUnit, d.client)
				if err != nil {
					return ctrl.Result{}, err
				}
				if queueUnit.Status.Phase == v1alpha1.Enqueued {
					return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
				}
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Enqueued, "Enqueued because backoff timeout", d.client)
			} else {
				log.V(2).Info("job will requeue backoff timeout", "after", handle.backoffTime-duration)
				return ctrl.Result{RequeueAfter: handle.backoffTime - duration}, nil
			}
		}

		if queueUnit.Status.Phase == v1alpha1.SchedReady {
			return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		}

		if handle.enableRunningToPending && queueUnit.Status.Phase == v1alpha1.Running {
			log.V(0).Info("set queue unit to dequeued")
			if err := util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Dequeued, "Job is in Pending state", d.client); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
	case Running:
		log.V(3).Info("job is running")
		if EnableReservation && handle.reservationJobExtension != nil {
			if queueUnit.Status.Phase == v1alpha1.SchedReady || queueUnit.Status.Phase == v1alpha1.SchedFailed {
				return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
			}
		}
		if queueUnit.Status.Phase != v1alpha1.Running {
			log.V(0).Info("set queue unit to running")
			return ctrl.Result{}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Running, "Running", d.client)
		}
		return ctrl.Result{}, nil
		// return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
	case Succeeded:
		log.V(3).Info("job has succeeded")
		if queueUnit.Status.Phase != v1alpha1.Succeed {
			log.V(0).Info("set queue unit to succeeded")
			if err := util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Succeed, "Succeeded", d.client); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	case Failed:
		log.V(3).Info("job has failed")
		if queueUnit.Status.Phase != v1alpha1.Failed {
			log.V(0).Info("set queue unit to failed")
			if err := util.UpdateQueueUnitStatus(queueUnit, v1alpha1.Failed, "Failed", d.client); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	default:
		log.Info("unrecongnize status")
		return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
	}
	return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
}
