package reservation

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	networkv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/networkaware/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ReservationController struct {
	cli client.Client

	recorder record.EventRecorder

	genericJobs map[string]framework.GenericJob

	reservationHandles     map[string]framework.GenericReservationJobExtension
	networktopologyHandles map[string]framework.NetworkAwareJobExtension
}

func NewReservationController(cli client.Client, evtRecorder record.EventRecorder, jobHandles ...framework.JobHandle) *ReservationController {
	rc := &ReservationController{
		cli:                    cli,
		recorder:               evtRecorder,
		genericJobs:            make(map[string]framework.GenericJob),
		reservationHandles:     make(map[string]framework.GenericReservationJobExtension),
		networktopologyHandles: make(map[string]framework.NetworkAwareJobExtension),
	}
	for _, jobHandle := range jobHandles {
		jobTypeKey := jobHandle.GetJobKey()
		rc.genericJobs[jobTypeKey] = jobHandle.GetHandle()
		if rh := jobHandle.GetResvHandle(); rh != nil {
			rc.reservationHandles[jobTypeKey] = rh
		}
		if nh := jobHandle.GetNetHandle(); nh != nil {
			rc.networktopologyHandles[jobTypeKey] = nh
		}
	}
	return rc
}

func (r *ReservationController) filter(obj client.Object) bool {
	qu, err := util.ToQueueUnit(obj)
	if err != nil {
		klog.Error(err, "failed to convert object to QueueUnit")
		return false
	}
	switch qu.Status.Phase {
	case v1alpha1.SchedReady, v1alpha1.Running, v1alpha1.SchedFailed, v1alpha1.SchedSucceed:
		return true
	default:
		return false
	}
}

func (r *ReservationController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	names := strings.Split(req.Name, "|")
	var jobName string
	if len(names) == 2 {
		req.Name = names[0]
		// 删除场景下由于无法获取Qu，只能从Reservation中查到JobName
		jobName = names[1]
	} else {
		jobName = req.Name
	}
	queueUnit := &v1alpha1.QueueUnit{}
	logger := ctrl.LoggerFrom(ctx, "queueUnit", req.String())
	logger.V(4).Info("reservation reconcile start")
	defer logger.V(4).Info("reservation reconcile end")
	if err := r.cli.Get(ctx, req.NamespacedName, queueUnit); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("delete networkTopology and reservations for queueUnit")
			name := req.Name + "-" + framework.Suffix
			jn := &networkv1alpha1.JobNetworkTopology{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: "dlc"}}
			if err := r.cli.Get(ctx, client.ObjectKeyFromObject(jn), jn); err == nil {
				if err := r.cli.Delete(ctx, &networkv1alpha1.JobNetworkTopology{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: "dlc"}}); err != nil {
					return ctrl.Result{}, err
				}
			}
			resvs := &koordinatorschedulerv1alpha1.ReservationList{}
			if err := r.cli.List(ctx, resvs, client.MatchingLabels{"reserved-job-namespace": req.Namespace, "reserved-job-name": jobName}); err == nil {
				for _, resv := range resvs.Items {
					if err := r.cli.Delete(ctx, &resv); err != nil {
						return ctrl.Result{}, err
					}
				}
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !r.filter(queueUnit) {
		return ctrl.Result{}, nil
	}

	jobTypeKey := queueUnit.Spec.ConsumerRef.APIVersion + "/" + queueUnit.Spec.ConsumerRef.Kind
	jobInterface := r.genericJobs[jobTypeKey]
	if jobInterface == nil {
		logger.Error(fmt.Errorf("unsupported job type %v", jobTypeKey), "")
		return ctrl.Result{}, nil
	}
	object := jobInterface.Object()
	if err := r.cli.Get(context.Background(), types.NamespacedName{Namespace: queueUnit.Spec.ConsumerRef.Namespace, Name: queueUnit.Spec.ConsumerRef.Name}, object); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if _, ok := r.reservationHandles[jobTypeKey]; !ok {
		if queueUnit.Status.Phase == v1alpha1.SchedReady {
			logger.V(1).Info("resume generic job due to related queueunit schedReady")
			return ctrl.Result{}, jobInterface.Resume(context.Background(), object, r.cli)
		}
		return ctrl.Result{}, nil
	}

	if queueUnit.Status.Phase == v1alpha1.SchedFailed {
		existingResvs := &koordinatorschedulerv1alpha1.ReservationList{}
		if err := r.cli.List(ctx, existingResvs, client.MatchingLabels{
			"reserved-job-namespace": object.GetNamespace(),
			"reserved-job-name":      object.GetName(),
		}); err != nil {
			logger.V(1).Info("failed to list reservations", "error", err)
		}
		deleted := []string{}
		for _, existingResv := range existingResvs.Items {
			if existingResv.Status.Phase == "Failed" || existingResv.Status.Phase == koordinatorschedulerv1alpha1.ReservationAvailable {
				if err := r.cli.Delete(ctx, &existingResv); err != nil {
					logger.V(1).Info("failed to delete reservation", "reservation", existingResv.Name, "error", err)
				}
				deleted = append(deleted, existingResv.Name)
			}
		}
		if len(deleted) > 0 {
			logger.V(1).Info("delete outdated/available reservations", "toDeleteResv", deleted)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil
	}

	if queueUnit.Status.Phase == v1alpha1.SchedSucceed {
		existingResvs := &koordinatorschedulerv1alpha1.ReservationList{}
		if err := r.cli.List(ctx, existingResvs, client.MatchingLabels{
			"reserved-job-namespace": object.GetNamespace(),
			"reserved-job-name":      object.GetName(),
		}); err != nil {
			logger.V(1).Info("failed to list reservations", "error", err)
		}
		deleted := []string{}
		for _, existingResv := range existingResvs.Items {
			if existingResv.Status.Phase == "Failed" {
				if err := r.cli.Delete(ctx, &existingResv); err != nil {
					logger.V(1).Info("failed to delete reservation", "reservation", existingResv.Name, "error", err)
				}
				deleted = append(deleted, existingResv.Name)
			}
		}
		if len(deleted) > 0 {
			logger.V(1).Info("delete outdated reservations", "toDeleteResv", deleted)
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, nil
	}

	if queueUnit.Status.Phase == v1alpha1.Running {
		if framework.EnableNetworkAware {
			name := req.Name + "-" + framework.Suffix
			jn := &networkv1alpha1.JobNetworkTopology{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: "dlc"}}
			if err := r.cli.Get(ctx, client.ObjectKeyFromObject(jn), jn); err == nil {
				if err := r.cli.Delete(ctx, &networkv1alpha1.JobNetworkTopology{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: "dlc"}}); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		existingResvs := &koordinatorschedulerv1alpha1.ReservationList{}
		if err := r.cli.List(ctx, existingResvs, client.MatchingLabels{"reserved-job-namespace": object.GetNamespace(), "reserved-job-name": object.GetName()}); err == nil {
			for _, resv := range existingResvs.Items {
				if resv.Status.Phase == koordinatorschedulerv1alpha1.ReservationAvailable {
					logger.V(1).Info("some reservations not match, this is not expected to happen", "reservation", resv.Name)
				}
			}
		}
		logger.V(2).Info("delete reservations with labels", "matchLabels", fmt.Sprintf("%v=%v,%v=%v", "reserved-job-namespace", req.Namespace, "reserved-job-name", object.GetName()))
		for _, resv := range existingResvs.Items {
			if err := r.cli.Delete(ctx, &resv); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if framework.EnableNetworkAware {
		err := r.ReconcileJobNetworkTopology(ctx, logger, object, queueUnit)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	needRequeue, reservationStatus, msg, err := r.ReconcileReservations(ctx, logger, object, queueUnit)
	if err != nil {
		return ctrl.Result{}, err
	}
	if needRequeue {
		r.recorder.Event(queueUnit, corev1.EventTypeNormal, "CreateOrUpdateReservation", "Update reservations succeed")
		return ctrl.Result{Requeue: true}, nil
	}
	logger.V(1).Info("update reservation status for job", "msg", msg, "reservation status", reservationStatus)
	switch reservationStatus {
	case framework.ReservationStatus_Failed:
		if queueUnit.Status.Phase != v1alpha1.SchedFailed {
			r.recorder.Event(queueUnit, corev1.EventTypeNormal, "ReservationsScheduling", "Failed to reserve resources for job")
			return ctrl.Result{}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.SchedFailed, msg, r.cli)
		}
		return ctrl.Result{}, nil
	case framework.ReservationStatus_Succeed:
		if queueUnit.Status.Phase != v1alpha1.SchedSucceed {
			r.recorder.Event(queueUnit, corev1.EventTypeNormal, "ReservationsSchedSucceed", "Scheduling succeed")
			return ctrl.Result{}, util.UpdateQueueUnitStatus(queueUnit, v1alpha1.SchedSucceed, msg, r.cli)
		}
		return ctrl.Result{}, nil
	case framework.ReservationStatus_Pending:
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReservationController) SetupWithManager(mgr ctrl.Manager, workers int, wqQPS int) error {
	log := mgr.GetLogger().WithName("ReservationController")
	r.cli = mgr.GetClient()
	build := ctrl.NewControllerManagedBy(mgr).WithOptions(controller.Options{MaxConcurrentReconciles: workers}).
		For(&v1alpha1.QueueUnit{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(ce event.CreateEvent) bool { return r.filter(ce.Object) },
			UpdateFunc: func(ue event.UpdateEvent) bool { return r.filter(ue.ObjectNew) },
			DeleteFunc: func(de event.DeleteEvent) bool { return true }})).
		Watches(&koordinatorschedulerv1alpha1.Reservation{},
			handler.Funcs{
				CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					obj, ok := tce.Object.(*koordinatorschedulerv1alpha1.Reservation)
					if !ok {
						return
					}
					jobNamespace := obj.Labels["reserved-job-namespace"]
					jobName := obj.Labels["reserved-job-name"]
					queueUnitName := obj.Annotations["reserved-queueunit-name"]
					rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobNamespace, Name: queueUnitName + "|" + jobName}})
				},
				UpdateFunc: func(ctx context.Context, tue event.TypedUpdateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					oldobj, ok := tue.ObjectOld.(*koordinatorschedulerv1alpha1.Reservation)
					if !ok {
						return
					}
					newobj, ok := tue.ObjectNew.(*koordinatorschedulerv1alpha1.Reservation)
					if !ok {
						return
					}
					if newobj.Status.Phase == koordinatorschedulerv1alpha1.ReservationPending || newobj.Status.Phase == "" {
						oldCond := koordinatorschedulerv1alpha1.ReservationCondition{}
						for _, cond := range oldobj.Status.Conditions {
							if cond.Type == koordinatorschedulerv1alpha1.ReservationConditionScheduled {
								oldCond = cond
							}
						}
						newCond := koordinatorschedulerv1alpha1.ReservationCondition{}
						for _, cond := range newobj.Status.Conditions {
							if cond.Type == koordinatorschedulerv1alpha1.ReservationConditionScheduled {
								newCond = cond
							}
						}
						if oldCond.Reason == newCond.Reason && oldCond.LastTransitionTime == newCond.LastTransitionTime && oldCond.LastProbeTime == newCond.LastProbeTime {
							return
						}
					}
					jobNamespace := newobj.Labels["reserved-job-namespace"]
					jobName := newobj.Labels["reserved-job-name"]
					queueUnitName := newobj.Annotations["reserved-queueunit-name"]
					log.V(3).Info("Reservation status changed", "reservation", newobj.Name, "phase", newobj.Status.Phase)
					rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobNamespace, Name: queueUnitName + "|" + jobName}})
				},
				DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					obj, ok := tde.Object.(*koordinatorschedulerv1alpha1.Reservation)
					if !ok {
						return
					}
					jobNamespace := obj.Labels["reserved-job-namespace"]
					jobName := obj.Labels["reserved-job-name"]
					queueUnitName := obj.Annotations["reserved-queueunit-name"]
					rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: jobNamespace, Name: queueUnitName + "|" + jobName}})
				},
			})
	return build.Complete(r)
}
