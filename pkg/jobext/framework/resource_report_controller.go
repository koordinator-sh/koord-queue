package framework

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type ResourceReporter struct {
	client client.Client

	scheme *runtime.Scheme

	jobHandles map[string]JobHandle
	jm         JobManager

	processedReservations map[string]int
	lastSeenResrvations   map[string]resourceVersionWithTime

	// partialRunningFirstSeen records, per admission, when Running first fell short of
	// Replicas, so the shortfall can be timed out instead of held forever.
	partialRunningFirstSeen map[string]time.Time

	// observedPods records, per admission (keyed by "<ns>/<qu>/<podset>"), the pod
	// names the controller has actually seen occupying an admitted slot. A pod that
	// was observed and is now gone is a confirmed external deletion (e.g. scheduler
	// preemption); a slot for which no pod ever appeared is simply not created yet.
	// This lets reconcilePodDeletion tell the two apart without a time window whose
	// correct length depends on job size and controller throughput.
	//
	// Guarded by observedPodsMu because the reconciler may run with more than one
	// worker. Entries are pruned when the QueueUnit is deleted (see SetupWithManager).
	observedPodsMu sync.Mutex
	observedPods   map[string]sets.Set[string]
}

func NewResourceReporter(cli client.Client, scheme *runtime.Scheme, handles ...JobHandle) *ResourceReporter {
	r := &ResourceReporter{
		client: cli,
		scheme: scheme,

		jobHandles:            map[string]JobHandle{},
		processedReservations: map[string]int{},
		lastSeenResrvations:   map[string]resourceVersionWithTime{},

		partialRunningFirstSeen: map[string]time.Time{},
		observedPods:            map[string]sets.Set[string]{},
	}

	for _, handle := range handles {
		r.jobHandles[handle.typeName] = handle
	}
	r.jm = &manager{jobHandles: r.jobHandles, cli: cli}

	return r
}

// SetupWithManager sets up the controller with the Manager.
func (d *ResourceReporter) SetupWithManager(mgr ctrl.Manager, workers int, wqQPS int) error {
	d.processedReservations = map[string]int{}
	d.lastSeenResrvations = map[string]resourceVersionWithTime{}
	build := ctrl.NewControllerManagedBy(mgr).Named("job-resource-reporter")
	return build.Watches(&v1alpha1.QueueUnit{}, handler.Funcs{
		CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tce.Object)
			if err != nil {
				return
			}
			if qu.Status.Phase == v1alpha1.Enqueued || qu.Status.Phase == "" {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
		UpdateFunc: func(ctx context.Context, tue event.TypedUpdateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tue.ObjectNew)
			if err != nil {
				return
			}
			if qu.Status.Phase == v1alpha1.Enqueued || qu.Status.Phase == "" {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
		DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			qu, err := util.ToQueueUnit(tde.Object)
			if err != nil {
				return
			}
			// Forget the deleted QueueUnit's observed-pod state; it is keyed by
			// QueueUnit and would otherwise leak once the QueueUnit is gone.
			d.forgetObservedPods(qu.Namespace + "/" + qu.Name)
			if qu.Status.Phase == v1alpha1.Enqueued || qu.Status.Phase == "" {
				return
			}
			rli.Add(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind + "|" + qu.Spec.ConsumerRef.Namespace, Name: qu.Spec.ConsumerRef.Name}})
		},
	}).Watches(&corev1.Pod{}, handler.Funcs{
		CreateFunc: func(ctx context.Context, tce event.TypedCreateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			obj, ok := tce.Object.(*corev1.Pod)
			if !ok {
				return
			}
			owner := d.jm.FindWorkloadFromPod(context.Background(), obj)
			if owner == nil {
				return
			}
			typeKey := owner.groupVersionKind
			rli.AddAfter(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: typeKey + "|" + obj.Namespace, Name: owner.name}}, time.Millisecond*50)
		},
		UpdateFunc: func(ctx context.Context, tue event.TypedUpdateEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			obj, ok := tue.ObjectNew.(*corev1.Pod)
			if !ok {
				return
			}
			oldObj, ok := tue.ObjectOld.(*corev1.Pod)
			if !ok {
				return
			}
			if oldObj.Spec.NodeName == obj.Spec.NodeName {
				return
			}
			owner := d.jm.FindWorkloadFromPod(context.Background(), obj)
			if owner == nil {
				return
			}
			typeKey := owner.groupVersionKind
			rli.AddAfter(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: typeKey + "|" + obj.Namespace, Name: owner.name}}, time.Millisecond*50)
		},
		DeleteFunc: func(ctx context.Context, tde event.TypedDeleteEvent[client.Object], rli workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			obj, ok := tde.Object.(*corev1.Pod)
			if !ok {
				return
			}
			owner := d.jm.FindWorkloadFromPod(context.Background(), obj)
			if owner == nil {
				return
			}
			typeKey := owner.groupVersionKind
			rli.AddAfter(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: typeKey + "|" + obj.Namespace, Name: owner.name}}, time.Millisecond*50)
		},
	}).WithOptions(controller.Options{MaxConcurrentReconciles: workers, RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](50*time.Millisecond, 1000*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(wqQPS), 1000)},
	)}).Complete(d)
}

func (d *ResourceReporter) syncInFlightWorkers(ctx context.Context, log logr.Logger, qu *v1alpha1.QueueUnit, pods []*corev1.Pod, podsByPs map[string][]*corev1.Pod) (ctrl.Result, error) {
	if qu.Labels["alibabacloud.com/disable-resource-recycle"] == "true" {
		return ctrl.Result{}, nil
	}
	if len(qu.OwnerReferences) > 0 && (qu.OwnerReferences[0].Kind == "ResourceBinding" || qu.OwnerReferences[0].Kind == "ClusterResourceBinding") {
		return ctrl.Result{}, nil
	}

	total := corev1.ResourceList{}
	pending := 0
	running := 0
	pd2Res := map[string]corev1.ResourceList{}
	for _, pod := range pods {
		// 	return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		// }
		res := util.GetPodRequestsAndLimits(&pod.Spec)
		pd2Res[pod.Namespace+"/"+pod.Name] = res
		util.AddResourceList(total, res)
		// A pod is counted as running once it has been scheduled (NodeName set),
		// even if it has not entered the Running phase yet (e.g. pulling image).
		// This way wait-for-pods-running queues are not blocked by image pull failures.
		if pod.Spec.NodeName == "" {
			pending++
		} else {
			running++
		}
	}

	// Only sync qu.Spec.Resource from actual pods when the QueueUnit is Running.
	// During Dequeued, pods may not have been created yet; syncing here would
	// incorrectly clear the requested resources.
	if qu.Status.Phase == v1alpha1.Running {
		if len(total) == len(qu.Spec.Resource) {
			needUpdate := false
			for k, v := range total {
				if vv, ok := qu.Spec.Resource[k]; !ok || !vv.Equal(v) {
					needUpdate = true
					break
				}
			}
			if needUpdate {
				log.V(1).Info("update resources in queueunit", "before", qu.Spec.Resource, "after", total)
				qu.Spec.Resource = total
				return ctrl.Result{}, d.client.Update(ctx, qu)
			}
		} else {
			log.V(1).Info("update resources in queueunit", "before", qu.Spec.Resource, "after", total)
			qu.Spec.Resource = total
			return ctrl.Result{}, d.client.Update(ctx, qu)
		}
	}
	runningByPs := map[string]int64{}
	resByPs := map[string]corev1.ResourceList{}
	for ps, pss := range podsByPs {
		// 	return ctrl.Result{RequeueAfter: DefaultRequeuePeriod}, nil
		// }
		// log.V(3).Info("sync podset", "podset", ps, "podcount", len(pss))
		runningByPs[ps] = 0
		for _, pod := range pss {
			if pod.Spec.NodeName != "" {
				runningByPs[ps]++
				if len(resByPs[ps]) == 0 {
					resByPs[ps] = corev1.ResourceList{}
				}
				if res, ok := pd2Res[pod.Namespace+"/"+pod.Name]; ok {
					util.AddResourceList(resByPs[ps], res)
				} else {
					util.AddResourceList(resByPs[ps], util.GetPodRequestsAndLimits(&pod.Spec))
				}
			}
		}
	}

	podState := v1alpha1.PodState{
		Pending: pending,
		Running: running,
	}
	updated := false
	// Only sync Admission.Resources from actual pod requests when the QueueUnit
	// is Running. During Dequeued, keep quota accounting purely admission-based
	// (Spec.PodSets template x Replicas) so that used is never affected by
	// actual pods before the job really runs.
	syncRes := qu.Status.Phase == v1alpha1.Running
	for k, r := range runningByPs {
		found := false
		for i, podset := range qu.Status.Admissions {
			if podset.Name == k {
				if r != podset.Running {
					qu.Status.Admissions[i].Running = r
					updated = true
				}
				if qu.Status.Admissions[i].Replicas < r {
					qu.Status.Admissions[i].Replicas = r
					updated = true
				}
				if syncRes && !util.Equal(qu.Status.Admissions[i].Resources, resByPs[k]) {
					updated = true
					qu.Status.Admissions[i].Resources = resByPs[k]
				}
				found = true
				break
			}
		}
		if !found {
			ad := v1alpha1.Admission{
				Name:     k,
				Running:  r,
				Replicas: r,
			}
			if syncRes {
				ad.Resources = resByPs[k]
			}
			qu.Status.Admissions = append(qu.Status.Admissions, ad)
			updated = true
		}
	}
	if qu.Status.PodState.Pending != podState.Pending || qu.Status.PodState.Running != podState.Running || updated {
		log.V(1).Info("update podState in queueunit", "before", qu.Status.PodState, "after", podState, "admissionsAfter", qu.Status.Admissions)
		qu.Status.PodState = podState
		return ctrl.Result{}, d.client.Status().Update(ctx, qu)
	}
	log.V(3).Info("syncInFlightWorkers and no need to update")
	return ctrl.Result{}, nil
}

func (d *ResourceReporter) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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
		return ctrl.Result{}, nil
	}

	object := handle.genericJobExtension.Object()
	err := d.client.Get(ctx, req.NamespacedName, object)
	if apierrors.IsNotFound(err) || !object.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}

	if !handle.genericJobExtension.ManagedByQueue(ctx, object) {
		return ctrl.Result{}, nil
	}

	queueUnit, err := handle.genericJobExtension.GetRelatedQueueUnit(ctx, object, d.client)
	if err != nil {
		return ctrl.Result{}, err
	}

	podSet := handle.genericJobExtension.PodSet(ctx, object)
	if reconcilePodSets(queueUnit, podSet) {
		if log.V(1).Enabled() {
			updatedReplicas := []string{}
			for _, ps := range podSet {
				updatedReplicas = append(updatedReplicas, fmt.Sprintf("%s:%d", ps.Name, ps.Count))
			}
			log.Info("update podset replicas in job", "replicas", fmt.Sprintf("{%v}", strings.Join(updatedReplicas, ",")))
		}

		// If podset spec need to be updated, update the spec and requeue this job.
		return ctrl.Result{}, client.IgnoreNotFound(d.client.Update(ctx, queueUnit))
	}

	pods, err := d.jm.GetRelatedPods(ctx, queueUnit)
	if err != nil {
		log.Error(err, "failed to get related pods")
		return ctrl.Result{}, err
	}
	podsByPs := map[string][]*corev1.Pod{}
	for _, po := range pods {
		psn := d.jm.GetPodSetName(ctx, queueUnit, po)
		if psn == "" {
			continue
		}
		if len(podsByPs[psn]) == 0 {
			podsByPs[psn] = []*corev1.Pod{po}
		} else {
			podsByPs[psn] = append(podsByPs[psn], po)
		}
	}

	if updatePodSets := d.reconcileReclaim(queueUnit.Status.Admissions, podsByPs); len(updatePodSets) > 0 {
		for i, ad := range queueUnit.Status.Admissions {
			reclaimed := len(updatePodSets[ad.Name])
			if ad.Replicas < int64(reclaimed) {
				ad.Replicas = 0
			} else {
				ad.Replicas -= int64(reclaimed)
			}
			ad.ReclaimState = nil
			queueUnit.Status.Admissions[i] = ad
		}
		return reconcile.Result{}, d.client.Status().Update(ctx, queueUnit)
	}

	if updated := d.reconcilePodDeletion(queueUnit, podsByPs); updated {
		return reconcile.Result{}, d.client.Status().Update(ctx, queueUnit)
	}

	if updated, err := d.reconcileOveradmission(ctx, log, queueUnit, podsByPs); updated {
		return reconcile.Result{}, err
	}

	if handle.partialRunningTimeout > 0 {
		if updated, requeueAfter := d.reconcilePartialRunningTimeout(log, queueUnit, handle.partialRunningTimeout); updated {
			return reconcile.Result{}, d.client.Status().Update(ctx, queueUnit)
		} else if requeueAfter > 0 {
			return reconcile.Result{RequeueAfter: requeueAfter}, nil
		}
	}

	jobStatus, _ := handle.genericJobExtension.GetJobStatus(ctx, object, d.client)
	if jobStatus != Running && jobStatus != Pending {
		return ctrl.Result{}, nil
	}

	// Also run when the QueueUnit is Dequeued: Admissions.Running counts scheduled
	// pods (NodeName set), so wait-for-pods-running queues can be released as soon
	// as pods are scheduled, without waiting for them to reach the Running phase
	// (e.g. blocked by image pull failures).
	if queueUnit.Status.Phase != v1alpha1.Running && queueUnit.Status.Phase != v1alpha1.Dequeued {
		return ctrl.Result{}, nil
	}

	return d.syncInFlightWorkers(ctx, log, queueUnit, pods, podsByPs)
}

// reconcilePodDeletion handles pods that are deleted externally (e.g. by the
// scheduler's preemption) without going through ReclaimState. For a Dequeued
// QueueUnit it shrinks Admission.Replicas to release the freed quota.
//
// The hard part is that, while a QueueUnit is Dequeued, "Replicas > live pods"
// is ambiguous: the missing pods may have been deleted, or the job controller
// may simply not have created them yet. Instead of guessing with a time window
// (whose correct length depends on job size and controller throughput), we
// remember which pods we have actually observed occupying an admitted slot. A
// pod we saw before and no longer see is a confirmed deletion; a slot whose pod
// never appeared is left untouched. On controller restart the memory is empty,
// so at worst we keep Replicas too high for a while — that only over-counts
// quota usage, never under-counts it, so it cannot cause over-admission.
func (d *ResourceReporter) reconcilePodDeletion(qu *v1alpha1.QueueUnit, podsByPs map[string][]*corev1.Pod) bool {
	if qu.Status.Phase != v1alpha1.Dequeued {
		return false
	}
	d.observedPodsMu.Lock()
	defer d.observedPodsMu.Unlock()

	quKey := qu.Namespace + "/" + qu.Name
	updated := false
	for i, ad := range qu.Status.Admissions {
		if ad.ReclaimState != nil {
			continue // already handled by reconcileReclaim
		}
		key := quKey + "/" + ad.Name

		live := sets.New[string]()
		for _, pod := range podsByPs[ad.Name] {
			if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
				live.Insert(pod.Name)
			}
		}

		observed, ok := d.observedPods[key]
		if !ok {
			observed = sets.New[string]()
			d.observedPods[key] = observed
		}
		// Pods currently occupying a slot are (re)recorded as observed.
		observed.Insert(live.UnsortedList()...)

		// Pods we recorded before but no longer see are confirmed deletions.
		gone := observed.Difference(live)
		if gone.Len() == 0 {
			continue
		}
		reduceBy := int64(gone.Len())
		if reduceBy > ad.Replicas {
			reduceBy = ad.Replicas
		}
		qu.Status.Admissions[i].Replicas = ad.Replicas - reduceBy
		// Stop counting the confirmed-gone pods on subsequent reconciles.
		observed.Delete(gone.UnsortedList()...)
		updated = true
	}
	return updated
}

// forgetObservedPods drops all observed-pod state for a QueueUnit. Called from
// the QueueUnit delete handler so the map does not grow without bound.
func (d *ResourceReporter) forgetObservedPods(quKey string) {
	d.observedPodsMu.Lock()
	defer d.observedPodsMu.Unlock()
	prefix := quKey + "/"
	for key := range d.observedPods {
		if strings.HasPrefix(key, prefix) {
			delete(d.observedPods, key)
		}
	}
}

// reconcilePartialRunningTimeout checks if Running < Replicas has persisted beyond the configured timeout.
// If so, it reduces Replicas to Running to release unused quota.
// Returns (updated, requeueAfter): updated=true means Replicas was changed;
// requeueAfter>0 means a timeout is pending and reconcile should be retried.
func (d *ResourceReporter) reconcilePartialRunningTimeout(
	log logr.Logger,
	qu *v1alpha1.QueueUnit,
	timeout time.Duration,
) (bool, time.Duration) {
	if qu.Status.Phase != v1alpha1.Running && qu.Status.Phase != v1alpha1.Dequeued {
		return false, 0
	}
	updated := false
	var minRemaining time.Duration
	quKey := qu.Namespace + "/" + qu.Name
	for i, ad := range qu.Status.Admissions {
		key := quKey + "/" + ad.Name
		if ad.Running >= ad.Replicas {
			delete(d.partialRunningFirstSeen, key)
			continue
		}
		// Running < Replicas
		firstSeen, exists := d.partialRunningFirstSeen[key]
		if !exists {
			d.partialRunningFirstSeen[key] = time.Now()
			firstSeen = time.Now()
		}
		elapsed := time.Since(firstSeen)
		if elapsed >= timeout {
			log.Info("partial running timeout: reducing replicas to running count",
				"podset", ad.Name, "replicas", ad.Replicas, "running", ad.Running,
				"timeout", timeout, "elapsed", elapsed)
			qu.Status.Admissions[i].Replicas = ad.Running
			delete(d.partialRunningFirstSeen, key)
			updated = true
		} else {
			remaining := timeout - elapsed
			if minRemaining == 0 || remaining < minRemaining {
				minRemaining = remaining
			}
		}
	}
	return updated, minRemaining
}

func (d *ResourceReporter) reconcileReclaim(admission []v1alpha1.Admission, admitted map[string][]*corev1.Pod) (xorAdmit map[string][]*corev1.Pod) {
	xorAdmit = map[string][]*corev1.Pod{}
	for _, ad := range admission {
		if ad.ReclaimState == nil {
			continue
		}
		pods := admitted[ad.Name]
		remainToReclaim := ad.ReclaimState.Replicas
		for _, pod := range pods {
			if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				continue
			}
			if pod.Spec.NodeName != "" {
				continue
			}
			if remainToReclaim > 0 {
				if len(xorAdmit[ad.Name]) == 0 {
					xorAdmit[ad.Name] = make([]*corev1.Pod, 0, len(pods))
				}
				xorAdmit[ad.Name] = append(xorAdmit[ad.Name], pod)
				remainToReclaim--
			}
			if remainToReclaim == 0 {
				break
			}
		}
	}
	return xorAdmit
}

// if request > admitted, then check if the number of running and pending pods is less than request
func (d *ResourceReporter) reconcileOveradmission(ctx context.Context, log logr.Logger, qu *v1alpha1.QueueUnit, podsByPs map[string][]*corev1.Pod) (needRequeue bool, err error) {
	requestPodSet := map[string]int32{}
	for _, ps := range qu.Spec.PodSets {
		requestPodSet[ps.Name] = ps.Count
	}
	needRequeue = false
	for i, ad := range qu.Status.Admissions {
		if requestPodSet[ad.Name] < int32(ad.Replicas) {
			log.V(4).Info(fmt.Sprintf("reclaim resource for podset %v, admitted %v, request %v", ad.Name, ad.Replicas, requestPodSet[ad.Name]))
			pods := podsByPs[ad.Name]
			runAnsPending := int32(0)
			for _, pod := range pods {
				if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
					continue
				}
				runAnsPending++
			}
			if runAnsPending >= requestPodSet[ad.Name] && runAnsPending < int32(ad.Replicas) {
				needRequeue = true
				qu.Status.Admissions[i].Replicas = int64(runAnsPending)
			} else if runAnsPending < requestPodSet[ad.Name] && requestPodSet[ad.Name] < int32(ad.Replicas) {
				needRequeue = true
				qu.Status.Admissions[i].Replicas = int64(requestPodSet[ad.Name])
			}
		}
	}

	if needRequeue {
		log.V(1).Info(fmt.Sprintf("updated admission after reclaim: %s", util.AdmissionReplicaString(qu.Status.Admissions)))
		return true, d.client.Status().Update(ctx, qu)
	}
	return false, nil
}

func reconcilePodSets(qu *v1alpha1.QueueUnit, ps []kueue.PodSet) (needUpdate bool) {
	needUpdate = false

	expectPs := map[string]int32{}
	for _, p := range ps {
		expectPs[p.Name] = p.Count
	}
	curPs := map[string]int32{}
	for _, p := range qu.Spec.PodSets {
		curPs[p.Name] = p.Count
	}
	for n := range curPs {
		if ep, ok := expectPs[n]; ok {
			if curPs[n] != ep {
				needUpdate = true
				curPs[n] = ep
			}
			delete(expectPs, n)
		} else {
			needUpdate = true
			delete(curPs, n)
		}
	}
	if len(expectPs) > 0 {
		needUpdate = true
	}

	newPodSet := []kueue.PodSet{}
	for i := range qu.Spec.PodSets {
		if _, ok := curPs[qu.Spec.PodSets[i].Name]; !ok {
			continue
		}
		qu.Spec.PodSets[i].Count = curPs[qu.Spec.PodSets[i].Name]
		newPodSet = append(newPodSet, qu.Spec.PodSets[i])
	}

	for n := range expectPs {
		p := kueue.PodSet{}
		for _, np := range ps {
			if np.Name == n {
				p = np
				break
			}
		}
		newPodSet = append(newPodSet, p)
	}

	qu.Spec.PodSets = newPodSet
	return
}
