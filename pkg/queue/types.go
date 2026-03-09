package queue

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"sigs.k8s.io/yaml"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	schedv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
)

// Queue is a Queue wrapper with additional information related to the Queue
type Queue struct {
	lock sync.RWMutex

	fw              framework.Handle
	queueUnitLister externalv1alpha1.QueueUnitLister
	queueImpl       queuepolicies.SchedulingQueue

	// Name is namespace
	name   string
	policy string
	run    bool
	queue  *v1alpha1.Queue

	interval time.Duration
	stopCtx  context.CancelFunc
	// When Queue.Pop() is called, the queue unit will be set to this field
	// When Delete or AddUnschedulableIfNotPresent is called, this field will be cleaned
	// It should be noted that Delete will be called when the queueUnit is not pending
	// anymore, either reserved, schedReady, dequeued or deleted.
	scheduling *framework.QueueUnitInfo

	// We store all queueunits that reserved the quota but not running in this field
	assumed              map[types.UID]struct{}
	waitingForScheduling map[types.UID]*framework.QueueUnitInfo

	admissionChecks map[string]labels.Selector
}

func NewQueue(fw framework.MultiQueueHandle, queueUnitLister externalv1alpha1.QueueUnitLister, name, policy string, queue *v1alpha1.Queue, args map[string]string) (*Queue, error) {
	queueImpl, err := CreateSchedulingQueue(name, policy, queue, fw, queueUnitLister, args)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		name:                 name,
		policy:               policy,
		queueImpl:            queueImpl,
		queue:                queue.DeepCopy(),
		fw:                   fw,
		interval:             time.Second * 15,
		assumed:              make(map[types.UID]struct{}),
		waitingForScheduling: map[types.UID]*framework.QueueUnitInfo{},
	}

	q.admissionChecks = ConvertToLabelSelector(queue.Spec.AdmissionChecks)
	return q, nil
}

func (q *Queue) DequeueSuccess(qu *v1alpha1.QueueUnit) {
	q.queueImpl.DequeueSuccess(qu)
}

func (q *Queue) Complete(qu *apiv1alpha1.QueueUnit) {
	q.queueImpl.Complete(qu)
}

func (q *Queue) GetAdmissionChecks() map[string]labels.Selector {
	q.lock.RLock()
	defer q.lock.RUnlock()

	snapshot := map[string]labels.Selector{}
	for name, sel := range q.admissionChecks {
		snapshot[name] = sel.DeepCopySelector()
	}
	return snapshot
}

func (q *Queue) SetQueueImplForTest(queueImpl queuepolicies.SchedulingQueue) {
	q.queueImpl = queueImpl
}

func (q *Queue) GetQueueImplForTest() queuepolicies.SchedulingQueue {
	return q.queueImpl
}

func (q *Queue) sync() {
	if _, ok := q.queue.Annotations["koord-queue/disable-show-queue-items"]; ok {
		if q.stopCtx != nil {
			q.stopCtx()
			q.stopCtx = nil
			klog.InfoS("disable show queue items", "queue", q.name)
		}
		return
	}
	needRestart := false
	if t, ok := q.queue.Annotations["koord-queue/queue-items-refresh-interval"]; ok {
		i, err := time.ParseDuration(t)
		if err == nil {
			q.interval = i
			needRestart = true
		} else {
			klog.ErrorS(err, "failed to parse interval", "len", t)
		}
	}
	if needRestart || !q.run {
		if q.stopCtx != nil {
			q.stopCtx()
			q.stopCtx = nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		q.stopCtx = cancel
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			q.lock.Lock()
			if q.queueImpl == nil {
				klog.ErrorS(nil, "queue impl in nil, which is not expected", "queue", q.name)
				q.lock.Unlock()
				return
			}
			scheduling := []*framework.QueueUnitInfo{}
			if q.scheduling != nil {
				scheduling = append(scheduling, q.scheduling)
			}
			for wfs, qui := range q.waitingForScheduling {
				if q.scheduling != nil && q.scheduling.Unit.UID == wfs {
					continue
				}
				scheduling = append(scheduling, qui)
			}
			maplist := q.queueImpl.SortedList(scheduling...)
			q.lock.Unlock()
			err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				return q.fw.UpdateQueueStatus(q.queue.Name, maplist)
			})
			if err != nil {
				klog.ErrorS(err, "failed to update queue status", "queue", q.name)
			}
		}, q.interval)
	}
}

func (q *Queue) Run() {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.run {
		return
	}
	q.sync()
	q.queueImpl.Run()
	q.run = true
}

func ConvertToLabelSelector(ac []schedv1alpha1.AdmissionCheckWithSelector) map[string]labels.Selector {
	acs := make(map[string]labels.Selector)
	for _, ad := range ac {
		if ad.Selector == nil {
			acs[ad.Name] = labels.Everything()
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(ad.Selector)
		if err != nil {
			continue
		}
		acs[ad.Name] = selector
	}
	return acs
}

func (q *Queue) Sync(newQueue *v1alpha1.Queue) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.admissionChecks = ConvertToLabelSelector(newQueue.Spec.AdmissionChecks)
	q.queueImpl.UpdateQueueCr(newQueue)

	var err error = nil
	found := false
	strategies := q.queueImpl.SupportedPolicy()
	for _, s := range strategies {
		if newQueue.Spec.QueuePolicy == v1alpha1.QueuePolicy(s) {
			found = true
			q.queueImpl.ChangePolicy(q.policy, string(newQueue.Spec.QueuePolicy))
			break
		}
	}
	if !found {
		items := q.queueImpl.List()

		argsStr := q.queue.Annotations[queuepolicies.QueueArgsAnnotationKey]
		args := make(map[string]string)
		yaml.Unmarshal([]byte(argsStr), args)

		q.queueImpl.Close()

		q.queueImpl, err = CreateSchedulingQueue(newQueue.Name, string(newQueue.Spec.QueuePolicy), newQueue, q.fw, q.queueUnitLister, args, items...)
	}
	q.policy = string(newQueue.Spec.QueuePolicy)
	q.queue = newQueue.DeepCopy()
	q.sync()
	return err
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) SetQueueCr4Test(queue *v1alpha1.Queue) {
	q.queue = queue
}

func (q *Queue) Queue() *v1alpha1.Queue {
	return q.queue.DeepCopy()
}

func (q *Queue) WaitingForSchedulingLen() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.waitingForScheduling)
}

func (q *Queue) AddUnschedulableIfNotPresent(ctx context.Context, qu *framework.QueueUnitInfo, doNotBlockQuota bool) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.scheduling = nil
	if _, ok := q.waitingForScheduling[qu.Unit.UID]; ok {
		delete(q.waitingForScheduling, qu.Unit.UID)
		klog.V(3).InfoS("delete from waitingForScheduling", "queue", q.name, "qu", qu.Name, "len", len(q.waitingForScheduling), "st", qu.Unit.Status.Phase)
	}
	return q.queueImpl.AddUnschedulableIfNotPresent(ctx, qu, doNotBlockQuota)
}

func (q *Queue) AddQueueUnitInfo(qu *framework.QueueUnitInfo) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	qu.Queue = q.Name()
	if !utils.IsQueueUnitPodsScheduleFinished(qu.Unit) {
		if _, ok := q.waitingForScheduling[qu.Unit.UID]; !ok {
			q.waitingForScheduling[qu.Unit.UID] = framework.NewQueueUnitInfo(qu.Unit)
			klog.V(3).InfoS("delete from waitingForScheduling", "queue", q.name, "qu", qu.Unit.Name, "len", len(q.waitingForScheduling), "st", qu.Unit.Status.Phase)
		}
	}
	_, err := q.queueImpl.AddQueueUnitInfo(qu)
	return err
}

// means the scheduling of the qu has finished
func (q *Queue) Reserve(ctx context.Context, qu *schedv1alpha1.QueueUnit) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.scheduling != nil && qu.Name == q.scheduling.Unit.Name {
		q.scheduling = nil
	}
	if !utils.IsQueueUnitPodsScheduleFinished(qu) {
		if _, ok := q.waitingForScheduling[qu.UID]; !ok {
			q.assumed[qu.UID] = struct{}{}
			q.waitingForScheduling[qu.UID] = framework.NewQueueUnitInfo(qu)
			klog.V(3).InfoS("insert to waitingForScheduling", "queue", q.name, "qu", qu.Name, "len", len(q.waitingForScheduling), "st", qu.Status.Phase)
		}
	}
	return q.queueImpl.Reserve(ctx, framework.NewQueueUnitInfo(qu))
}

func (q *Queue) Delete(qu *schedv1alpha1.QueueUnit) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.scheduling != nil && qu.Name == q.scheduling.Unit.Name {
		q.scheduling = nil
	}
	delete(q.assumed, qu.UID)
	if _, ok := q.waitingForScheduling[qu.UID]; ok {
		delete(q.waitingForScheduling, qu.UID)
		klog.V(3).InfoS("delete from waitingForScheduling", "queue", q.name, "qu", qu.Name, "len", len(q.waitingForScheduling), "st", qu.Status.Phase)
	}
	// deleteDequeuedQueueUnit should be handled in this func
	return q.queueImpl.Delete(qu)
}

func (q *Queue) Update(oldQu, newQu *schedv1alpha1.QueueUnit) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if !utils.IsQueueUnitPodsScheduleFinished(newQu) {
		delete(q.assumed, newQu.UID)
		if _, ok := q.waitingForScheduling[newQu.UID]; !ok {
			q.waitingForScheduling[newQu.UID] = framework.NewQueueUnitInfo(newQu)
			klog.V(3).InfoS("insert to waitingForScheduling", "queue", q.name, "qu", newQu.Name, "len", len(q.waitingForScheduling), "st", newQu.Status.Phase)
		}
	} else if _, ok := q.assumed[newQu.UID]; !ok {
		delete(q.waitingForScheduling, newQu.UID)
		klog.V(3).InfoS("delete from waitingForScheduling", "queue", q.name, "qu", newQu.Name, "len", len(q.waitingForScheduling), "st", newQu.Status.Phase)
	} else {
		klog.V(3).InfoS("qu is assumed, skip to delete from waitingForScheduling", "queue", q.name, "qu", newQu.Name, "len", len(q.waitingForScheduling), "st", newQu.Status.Phase)
	}
	return q.queueImpl.Update(oldQu, newQu)
}

func (q *Queue) UpdateDequeuedQueueUnit(newQu *framework.QueueUnitInfo) {
	q.lock.Lock()
	defer q.lock.Unlock()

}

func (q *Queue) Preempt(ctx context.Context, qi *framework.QueueUnitInfo) error {
	return q.queueImpl.Preempt(ctx, qi)
}

func (q *Queue) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string) {
	q.queueImpl.UpdateQueueLimitByParent(limit, limitParentQuotas)
}

func (q *Queue) Next(ctx context.Context) (*framework.QueueUnitInfo, error) {
	// Next will be blocked, so make sure the queue is not blocked.
	info, err := q.queueImpl.Next(ctx)
	if err != nil {
		return info, err
	}

	q.lock.Lock()
	defer q.lock.Unlock()
	q.scheduling = info
	return info, err
}

// will be called when queue strategy is changed
func (q *Queue) List() []*framework.QueueUnitInfo {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.queueImpl == nil {
		return nil
	}
	return q.queueImpl.List()
}

// will be called when quota info is changed
func (q *Queue) Fix(f framework.QueueUnitMappingFunc) []*framework.QueueUnitInfo {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.queueImpl == nil {
		return nil
	}
	return q.queueImpl.Fix(f)
}

func (q *Queue) PendingLength() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.queueImpl == nil {
		return 0
	}
	return q.queueImpl.PendingLength()
}

func (q *Queue) Length() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.queueImpl == nil {
		return 0
	}
	return q.queueImpl.Length()
}

func (q *Queue) Close() {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if !q.run {
		return
	}
	q.run = false
	q.queueImpl.Close()
}

func (q *Queue) GetQueueDebugInfo() queuepolicies.QueueDebugInfo {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.queueImpl.GetQueueDebugInfo()
}

func (q *Queue) GetUserQuotaDebugInfo() queuepolicies.UserQuotaDebugInfo {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.queueImpl.GetUserQuotaDebugInfo()
}
