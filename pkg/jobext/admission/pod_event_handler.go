package admissioncontroller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
)

func buildPodEventhandler(a *AdmissionControlelr) handler.Funcs {
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			po, ok := tce.Object.(*corev1.Pod)
			if !ok {
				return
			}
			if len(po.OwnerReferences) == 0 {
				return
			}
			nsName := a.jm.GetRelatedQueueUnit(ctx, po)
			if nsName.Name == "" {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: nsName})
		},
		DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			po, ok := tde.Object.(*corev1.Pod)
			if !ok {
				return
			}
			if len(po.OwnerReferences) == 0 {
				return
			}
			nsName := a.jm.GetRelatedQueueUnit(ctx, po)
			if nsName.Name == "" {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: nsName})
		},
	}
}
