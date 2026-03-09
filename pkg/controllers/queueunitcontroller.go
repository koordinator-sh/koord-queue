package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/kube-queue/api/pkg/client/listers/scheduling/v1alpha1"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
)

// QueueUnitController will update QueueUnit Status to Dequeued once QueueUnit's all admissionChecks
// are ready.
type QueueUnitController struct {
	queueUnitClient   versioned.Interface
	queueUnitInformer cache.SharedIndexInformer
	queueUnitLister   externalv1alpha1.QueueUnitLister

	enableStrictDequeueMode bool

	ControllerBaseImpl
}

func NewQueueUnitController(workers int64, enableStrictDequeueMode bool, cli versioned.Interface, informer cache.SharedIndexInformer, lister externalv1alpha1.QueueUnitLister) *QueueUnitController {
	logger := klogr.New().WithName("QueueUnit controller")
	quc := &QueueUnitController{
		queueUnitClient:   cli,
		queueUnitInformer: informer,
		queueUnitLister:   lister,

		enableStrictDequeueMode: enableStrictDequeueMode,
	}

	quc.ControllerBaseImpl = NewControllerBase(int(workers), logger, []cache.InformerSynced{informer.HasSynced}, quc.reconcile, workqueue.DefaultTypedItemBasedRateLimiter[string]())

	informer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			qu, ok := obj.(*v1alpha1.QueueUnit)
			if !ok {
				return false
			}
			return qu.Status.Phase == v1alpha1.Reserved
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				qu, ok := obj.(*v1alpha1.QueueUnit)
				if !ok {
					return
				}
				quc.Wq.AddRateLimited(qu.Namespace + "/" + qu.Name)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oqu, ok := oldObj.(*v1alpha1.QueueUnit)
				if !ok {
					return
				}
				nqu, ok := newObj.(*v1alpha1.QueueUnit)
				if !ok {
					return
				}
				if !reflect.DeepEqual(oqu.Status.AdmissionChecks, nqu.Status.AdmissionChecks) {
					quc.Wq.AddRateLimited(nqu.Namespace + "/" + nqu.Name)
				}
			},
		},
	})
	return quc
}

func getNamespaceName(key string) (string, string, error) {
	strs := strings.Split(key, "/")
	if len(strs) != 2 {
		return "", "", fmt.Errorf("failed to get namespace and name from key: %v", key)
	}

	return strs[0], strs[1], nil
}

type State int

const (
	StateReady    State = 0
	StatePending  State = 1
	StateRejected State = 2
	StateRetry    State = 3
)

func (q *QueueUnitController) reconcile(key string) (Result, error) {
	q.logger.V(1).Info("reconcile start", "queueUnit", key)
	defer q.logger.V(1).Info("reconcile end", "queueUnit", key)
	namespace, name, err := getNamespaceName(key)
	if err != nil {
		q.logger.Error(err, "")
		return Result{Requeue: false}, nil
	}

	queueUnit, err := q.queueUnitLister.QueueUnits(namespace).Get(name)
	if err != nil {
		q.logger.Error(err, "failed to get queue unit", "queueUnit", key)
		return Result{Requeue: false}, nil
	}

	if queueUnit.Status.Phase != v1alpha1.Reserved {
		q.logger.V(5).Info("queue unit is not in Reserved phase")
		return Result{Requeue: false}, nil
	}

	queueUnitCp := queueUnit.DeepCopy()
	needUpdate := updateQueueUnitStatus(queueUnitCp, q.enableStrictDequeueMode, q.logger)
	if !needUpdate {
		return Result{}, nil
	}

	_, err = q.queueUnitClient.SchedulingV1alpha1().QueueUnits(namespace).UpdateStatus(context.Background(), queueUnitCp, metav1.UpdateOptions{})
	if err != nil {
		q.logger.Error(err, "failed to update status for queue unit", "queueUnit", key)
		return Result{}, err
	}
	return Result{}, nil
}

func updateQueueUnitStatus(queueUnitCp *v1alpha1.QueueUnit, enableStrictDequeueMode bool, logger logr.Logger) bool {
	var state State = StateReady
	now := metav1.Now()
	pendingAdmissionChecks := []string{}
	for _, check := range queueUnitCp.Status.AdmissionChecks {
		if check.State == v1beta1.CheckStateRejected {
			state = StateRejected
			break
		} else if check.State == v1beta1.CheckStateRetry {
			state = StateRetry
			break
		} else if check.State == v1beta1.CheckStatePending {
			state = StatePending
			pendingAdmissionChecks = append(pendingAdmissionChecks, check.Name)
		}
	}

	needUpdate := false
	if state > StatePending {
		// if any admissionCheck need retry or is rejected, set queue unit to backoff
		// TODO: we may set queue unit to false if any admissionCheck is rejected.
		queueUnitCp.Status.Phase = v1alpha1.Backoff
		queueUnitCp.Status.LastUpdateTime = &now
		queueUnitCp.Status.Message = "admission check failed"
		needUpdate = true
	} else if state == StateReady && !enableStrictDequeueMode {
		queueUnitCp.Status.Phase = v1alpha1.Dequeued
		queueUnitCp.Status.LastUpdateTime = &now
		queueUnitCp.Status.Message = "all admission checks success"
		needUpdate = true
	} else if state == StateReady && enableStrictDequeueMode {
		queueUnitCp.Status.Phase = v1alpha1.SchedReady
		queueUnitCp.Status.LastUpdateTime = &now
		queueUnitCp.Status.Message = "all admission checks success"
		needUpdate = true
	} else if queueUnitCp.Status.Phase != v1alpha1.Reserved {
		logger.V(1).Info("some admission checks are not ready, but queueUnit is not in Reserved state. should never happen",
			"queueUnit", klog.KObj(queueUnitCp), "state", queueUnitCp.Status.Phase, "pendingChecks", pendingAdmissionChecks)
	}
	return needUpdate
}
