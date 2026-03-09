package framework

import (
	"context"
	"reflect"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func NewDefaultEventHandler(cli client.Client, adaptor GenericJobExtension) handler.EventHandler {
	return &DefaultQueueUnitEventHandler{
		client:  cli,
		adaptor: adaptor,
	}
}

type DefaultQueueUnitEventHandler struct {
	client  client.Client
	adaptor GenericJobExtension
}

func (m *DefaultQueueUnitEventHandler) Create(ctx context.Context, evt event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	qu := evt.Object.(*v1alpha1.QueueUnit)
	if qu.Spec.ConsumerRef.APIVersion != m.adaptor.GVK().GroupVersion().String() {
		return
	}
	if qu.Status.Phase == v1alpha1.Running {
		q.AddAfter(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}}, time.Minute*1)
		return
	}
	q.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
}

func (m *DefaultQueueUnitEventHandler) Delete(ctx context.Context, evt event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	qu := evt.Object.(*v1alpha1.QueueUnit)
	if qu.Spec.ConsumerRef.APIVersion != m.adaptor.GVK().GroupVersion().String() {
		return
	}
	q.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
}

func (m *DefaultQueueUnitEventHandler) Update(ctx context.Context, evt event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	qu := evt.ObjectNew.(*v1alpha1.QueueUnit)
	oqu := evt.ObjectOld.(*v1alpha1.QueueUnit)
	if qu.Spec.ConsumerRef.APIVersion != m.adaptor.GVK().GroupVersion().String() {
		if rand.Intn(100) > 99 {
			klog.V(0).InfoS("APIVersion not matched", "job", qu.Spec.ConsumerRef.APIVersion, "new", m.adaptor.GVK().GroupVersion().String())
		}
		return
	}
	// SchedFailed是自己设置的，所以不需要重新尝试
	if qu.Status.Phase == v1alpha1.SchedFailed {
		return
	}
	newStatus := v1alpha1.QueueUnitStatus{
		Phase:           qu.Status.Phase,
		AdmissionChecks: qu.Status.AdmissionChecks,
	}
	oldStatus := v1alpha1.QueueUnitStatus{
		Phase:           oqu.Status.Phase,
		AdmissionChecks: oqu.Status.AdmissionChecks,
	}
	if reflect.DeepEqual(newStatus, oldStatus) {
		if qu.Spec.Priority != nil && *qu.Spec.Priority == 3 {
			klog.V(3).InfoS("Job phase not updated", "job", qu.Spec.ConsumerRef.Name, "new", newStatus.Phase)
		}
		return
	}
	klog.V(3).InfoS("Job phase updated, try to reconcile again", "job", qu.Spec.ConsumerRef.Name, "old", oldStatus.Phase, "new", newStatus.Phase)
	// add a delay to avoid the previous reconciliation not finished.
	q.AddAfter(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}}, time.Millisecond*50)
}

func (m *DefaultQueueUnitEventHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}
