/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nscontroller

import (
	"context"
	"encoding/json"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/go-logr/logr"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	elasticquotatree "github.com/kube-queue/kube-queue/pkg/framework/plugins/elasticquota"
)

// NamespaceReconciler reconciles a Namespace object
type NamespaceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	lock sync.RWMutex
	// these fields will be updated in Reconcile
	quotasToNamepaces map[string]sets.Set[string]
	namesapceToQuotas map[string]sets.Set[string]
	// immutable in Reconcile, these fields should be updated by event handler
	queueToQuotas map[string]sets.Set[string]
	// * represents a special quota, which means all quotas can use the queue
	quotaToQueues map[string]sets.Set[string]
}

var Log logr.Logger

func removeAvailableQueuesIfExists(ctx context.Context, client client.Client, ns *corev1.Namespace, log logr.Logger) error {
	if q := elasticquotatree.GetAvailableQueueString(ns.Annotations); q != "" {
		log.V(2).Info("no available quota in namespace, remove queue annotation")
		elasticquotatree.RemoveQueueStr(ns.Annotations)
		return client.Update(ctx, ns)
	}
	log.V(4).Info("no available quota in namespace")
	return nil
}

// +kubebuilder:rbac:groups=my.domain,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=my.domain,resources=namespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=my.domain,resources=namespaces/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Namespace object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *NamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := Log.WithValues("namespace", req.NamespacedName.Name)

	ns := corev1.Namespace{}
	err := r.Client.Get(ctx, req.NamespacedName, &ns)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	availableQuotas := elasticquotatree.GetAvailableQuotasInNamespace(&ns)
	if len(availableQuotas) == 0 {
		r.lock.Lock()
		for quota := range r.namesapceToQuotas[ns.Name] {
			r.quotasToNamepaces[quota].Delete(ns.Name)
		}
		delete(r.namesapceToQuotas, ns.Name)
		r.lock.Unlock()
		return ctrl.Result{}, removeAvailableQueuesIfExists(ctx, r.Client, &ns, log)
	}
	if len(availableQuotas) == 1 && availableQuotas[0] == "*" {
		// TODO: 增加一个event透出
		log.V(1).Info("* is not available in namespace's annotation.")
		r.lock.Lock()
		for quota := range r.namesapceToQuotas[ns.Name] {
			r.quotasToNamepaces[quota].Delete(ns.Name)
		}
		delete(r.namesapceToQuotas, ns.Name)
		r.lock.Unlock()
		return ctrl.Result{}, removeAvailableQueuesIfExists(ctx, r.Client, &ns, log)
	}

	needUpdateQuotas := false
	avaiQuotaSet := sets.New(availableQuotas...)
	r.lock.RLock()
	if r.namesapceToQuotas[ns.Name] == nil || !r.namesapceToQuotas[ns.Name].Equal(avaiQuotaSet) {
		needUpdateQuotas = true
	}

	// update controller cache
	if needUpdateQuotas {
		r.lock.RUnlock()
		r.lock.Lock()
		quotaSet := r.namesapceToQuotas[ns.Name]
		if quotaSet == nil {
			quotaSet = sets.New[string]()
		}
		log.V(1).Info("update available quota", "old", quotaSet.UnsortedList(), "new", availableQuotas)
		diff := quotaSet.Difference(avaiQuotaSet)
		for quota := range diff {
			r.quotasToNamepaces[quota].Delete(ns.Name)
		}
		for quota := range avaiQuotaSet {
			if r.quotasToNamepaces[quota] == nil {
				r.quotasToNamepaces[quota] = sets.New[string]()
			}
			r.quotasToNamepaces[quota].Insert(ns.Name)
		}
		r.namesapceToQuotas[ns.Name] = avaiQuotaSet
		r.lock.Unlock()
		r.lock.RLock()
	}
	defer r.lock.RUnlock()

	// key is queue name, value is the list of available quota.
	availableQueues := map[string][]string{}
	for quota := range r.namesapceToQuotas[ns.Name] {
		if queueSet := r.quotaToQueues[quota]; queueSet != nil {
			for queue := range queueSet {
				if len(availableQueues[queue]) == 0 {
					availableQueues[queue] = []string{quota}
				} else {
					availableQueues[queue] = append(availableQueues[queue], quota)
				}
			}
		}
	}
	if queueSet := r.quotaToQueues["*"]; queueSet != nil {
		for queue := range queueSet {
			availableQueues[queue] = r.namesapceToQuotas[ns.Name].UnsortedList()
		}
	}

	str := ""
	if len(availableQueues) > 0 {
		for queue, quotaset := range availableQueues {
			slices.Sort(quotaset)
			availableQueues[queue] = quotaset
		}
		bytes, err := json.Marshal(availableQueues)
		if err != nil {
			return ctrl.Result{}, err
		}
		str = string(bytes)
	}
	if cstr := elasticquotatree.GetAvailableQueueString(ns.Annotations); str != cstr {
		log.V(1).Info("update available queues", "old", cstr, "new", str)
		if str == "" {
			elasticquotatree.RemoveQueueStr(ns.Annotations)
		} else {
			if ns.Annotations == nil {
				ns.Annotations = make(map[string]string)
			}
			elasticquotatree.AddQueueStr(ns.Annotations, str)
		}
		return ctrl.Result{}, r.Client.Update(ctx, &ns)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.queueToQuotas = make(map[string]sets.Set[string])
	r.quotaToQueues = make(map[string]sets.Set[string])
	r.quotasToNamepaces = make(map[string]sets.Set[string])
	r.namesapceToQuotas = make(map[string]sets.Set[string])

	log := mgr.GetLogger()
	Log = log
	return ctrl.NewControllerManagedBy(mgr).
		// Uncomment the following line adding a pointer to an instance of the controlled resource as an argument
		// For().
		For(&corev1.Namespace{}).
		Watches(&v1alpha1.Queue{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			queue, ok := o.(*v1alpha1.Queue)
			if !ok {
				return nil
			}
			log = log.WithValues("queue", queue.Name)
			newQueue := &v1alpha1.Queue{}
			quotas := []string{}
			err := r.Client.Get(ctx, types.NamespacedName{Name: queue.Name, Namespace: queue.Namespace}, newQueue)
			r.lock.RLock()
			if err == nil && queue.DeletionTimestamp == nil {
				quotas = elasticquotatree.GetAvailableQuotasInQueue(queue)
			} else if err != nil && !errors.IsNotFound(err) {
				quotasInQueue := r.queueToQuotas[queue.Name]
				if len(quotasInQueue) == 0 {
					quotasInQueue = sets.Set[string]{}
				}
				log.Error(err, "cannot get queue from client, remove all quotas from it", "quotas", quotasInQueue.UnsortedList())
			} else {
				quotasInQueue := r.queueToQuotas[queue.Name]
				if len(quotasInQueue) == 0 {
					quotasInQueue = sets.Set[string]{}
				}
				log.V(1).Info("queue is deleted, remove all quotas from it", "quotas", quotasInQueue.UnsortedList())
			}
			allowAllQuotas := len(quotas) == 1 && quotas[0] == "*"
			// build related namespaces, do not need update in this section
			storedQuotas := r.queueToQuotas[queue.Name]
			allowAllQuotasBefore := storedQuotas.Has("*")
			relatedNamespacs := sets.Set[reconcile.Request]{}
			needUpdate := len(quotas) != len(storedQuotas)
			if storedQuotas == nil {
				if allowAllQuotas {
					// all namespace can submit to this queue
					for ns := range r.namesapceToQuotas {
						relatedNamespacs.Insert(reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
					}
				} else {
					for _, quota := range quotas {
						for ns := range r.quotasToNamepaces[quota] {
							relatedNamespacs.Insert(reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
						}
					}
				}
			} else {
				if allowAllQuotas && !allowAllQuotasBefore {
					needUpdate = true
					for ns := range r.namesapceToQuotas {
						relatedNamespacs.Insert(reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
					}
				} else if !allowAllQuotas && allowAllQuotasBefore {
					needUpdate = true
					for ns := range r.namesapceToQuotas {
						relatedNamespacs.Insert(reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
					}
				} else {
					relatedQuota := sets.New(quotas...)
					union := relatedQuota.Union(storedQuotas)
					if needUpdate = len(relatedQuota) != len(storedQuotas) || !union.Equal(storedQuotas); needUpdate {
						for quota := range union {
							for ns := range r.quotasToNamepaces[quota] {
								relatedNamespacs.Insert(reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
							}
						}
					}
				}
			}
			r.lock.RUnlock()
			if needUpdate {
				// build controller cache, need update in this section
				r.lock.Lock()
				new := sets.New(quotas...)
				current := r.queueToQuotas[queue.Name]
				if current == nil {
					current = sets.New[string]()
				}
				log.V(1).Info("update available quotas", "queue", queue.Name, "old", current.UnsortedList(), "new", quotas)
				for quota := range current.Difference(new) {
					r.quotaToQueues[quota].Delete(queue.Name)
				}
				for quota := range new {
					if len(r.quotaToQueues[quota]) == 0 {
						r.quotaToQueues[quota] = sets.New[string](queue.Name)
					} else {
						r.quotaToQueues[quota].Insert(queue.Name)
					}
				}
				r.queueToQuotas[queue.Name] = new.Clone()
				r.lock.Unlock()
			}
			related := relatedNamespacs.UnsortedList()
			if len(related) > 0 {
				log.V(1).Info("add related ns to queue", "queue", queue.Name, "related", related)
			}
			return related
		}), builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				annos := e.Object.GetAnnotations()
				if len(annos) == 0 {
					return false
				}
				return elasticquotatree.GetAvailableQuotaStringInQueue(annos) != ""
			},
			DeleteFunc: func(tde event.TypedDeleteEvent[client.Object]) bool { return true },
			UpdateFunc: func(tue event.TypedUpdateEvent[client.Object]) bool {
				oldAnnos := tue.ObjectOld.GetAnnotations()
				newAnnos := tue.ObjectNew.GetAnnotations()
				if len(oldAnnos) == 0 && len(newAnnos) == 0 {
					return false
				}
				return elasticquotatree.GetAvailableQuotaStringInQueue(oldAnnos) != elasticquotatree.GetAvailableQuotaStringInQueue(newAnnos)
			},
			GenericFunc: func(tge event.TypedGenericEvent[client.Object]) bool { return false },
		})).
		Complete(r)
}
