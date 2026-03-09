package schedulingqueuev2

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/util"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
	"k8s.io/klog/v2"
)

const (
	WaitForPodsRunningAnnotation = "koord-queue/wait-for-pods-running"
	EnableQueueUnitPreemption    = "koord-queue/enable-queueunit-preemption"
	MaxDepthAnnotation           = "koord-queue/max-depth"
)

func less(a, b *framework.QueueUnitInfo) int {
	prioA := 0
	prioB := 0
	if a.Unit.Spec.Priority != nil {
		prioA = int(*a.Unit.Spec.Priority)
	}
	if b.Unit.Spec.Priority != nil {
		prioB = int(*b.Unit.Spec.Priority)
	}
	if a.Unit.UID == b.Unit.UID {
		return 0
	}
	if prioA < prioB {
		return 1
	} else if prioA > prioB {
		return -1
	} else if util.IsJobPreemptible(a) != util.IsJobPreemptible(b) {
		if util.IsJobPreemptible(a) {
			return 1
		} else {
			return -1
		}
	} else if a.InitialAttemptTimestamp.After(b.InitialAttemptTimestamp) {
		return 1
	} else if a.InitialAttemptTimestamp.Before(b.InitialAttemptTimestamp) {
		return -1
	} else if a.Name < b.Name {
		return -1
	} else if a.Name > b.Name {
		return 1
	}
	return 0
}

func NewPriorityQueue(name string,
	q *v1alpha1.Queue,
	fw framework.Handle,
	client versioned.Interface,
	queueUnitLister externalv1alpha1.QueueUnitLister,
	args map[string]string,
	items ...*framework.QueueUnitInfo) queuepolicies.SchedulingQueue {
	pq := &PriorityQueue{
		name:    name,
		queueCr: q,
		fw:      fw,

		nextIdx:  0,
		maxDepth: -1,

		queue:      make([]*framework.QueueUnitInfo, 0),
		assumed:    make(map[string]struct{}),
		updating:   make(map[string]struct{}),
		queueUnits: make(map[string]*framework.QueueUnitInfo),

		blocked:      q != nil && q.Spec.QueuePolicy == "Block",
		blockedQuota: syncInt{},

		lessFunc: less,

		lastResetTime: time.Now(),
	}
	if q.Annotations[MaxDepthAnnotation] != "" {
		maxDepth, err := strconv.ParseInt(q.Annotations[MaxDepthAnnotation], 10, 32)
		if err == nil {
			pq.maxDepth = int32(maxDepth)
		}
	}

	pq.cond = sync.NewCond(&pq.lock)
	pq.waitPodsRunning = q.Annotations[WaitForPodsRunningAnnotation] == "true"
	pq.enablePreempt = q.Annotations[EnableQueueUnitPreemption] == "true"
	klog.V(1).Infof("create new queue %v, policy %v, waitPodsRunning %v, maxDepth %v", pq.name, pq.queueCr.Spec.QueuePolicy, pq.waitPodsRunning, pq.maxDepth)
	go func() {
		pq.fixQueue(klog.Background())
		time.Sleep(30 * time.Second)
	}()
	return pq
}

type PriorityQueue struct {
	lock sync.RWMutex
	cond *sync.Cond
	fw   framework.Handle
	// When we call cond.Wait(), we will set a time with timeout to wake up the waiting goroutine.
	wakeUpTimer *time.Timer

	name string
	// The next queueUnit to be scheduled
	resetNextIdxFlag bool
	lastResetTime    time.Time
	nextIdx          int32
	maxDepth         int32
	sessionId        int64
	// Add your fields here, such as queue name, internal data structures, etc.
	queue []*framework.QueueUnitInfo
	// assumed queueUnits are queue units that have reserved the quota but not run yet
	assumed map[string]struct{}
	// updating queueUnits are queue units that reserved and updating process not complete
	updating   map[string]struct{}
	queueUnits map[string]*framework.QueueUnitInfo
	// queueUnits will be sorted by priority and initialAttemptTimestamp
	lessFunc func(a, b *framework.QueueUnitInfo) int

	queueCr *v1alpha1.Queue
	// When queue.spec.queuePolicy == Block, blocked will be set to true.
	blocked                bool
	blockedQuota           syncInt
	lastScheduledTime      time.Time
	lastScheduledQueueUnit string
	//
	waitPodsRunning bool
	enablePreempt   bool
	// only one preempt go routine will be running at one time
	preemptFlag atomic.Int32
}

var _ queuepolicies.SchedulingQueue = &PriorityQueue{}

func (q *PriorityQueue) getQueueMaxResetDuration() time.Duration {
	l := 0
	if len(q.queueUnits) > int(q.maxDepth) {
		l = int(q.maxDepth)
	} else {
		l = len(q.queueUnits)
	}
	if l > 500 {
		return time.Minute * 5
	}
	return time.Minute * 3
}

// lock should be obtained before calling this function.
// q.nextId will change in this func
func (q *PriorityQueue) findNextQueueUnit() (int32, *framework.QueueUnitInfo) {
	var qu *framework.QueueUnitInfo
	now := time.Now()
	if q.resetNextIdxFlag || now.Sub(q.lastResetTime) > q.getQueueMaxResetDuration() {
		q.nextIdx = 0
		q.resetNextIdxFlag = false
		q.lastResetTime = now
	}
	for {
		if q.nextIdx >= int32(len(q.queue)) || (q.maxDepth > 0 && q.nextIdx >= q.maxDepth) {
			return q.nextIdx, nil
		}
		qu = q.queue[q.nextIdx]
		if utils.IsQueueUnitSatisfied(qu.Unit) {
			q.nextIdx++
			continue
		}
		// if _, ok := q.assumed[qu.Name]; ok {
		// 	q.nextIdx++
		// 	continue
		// }
		quotas, err := q.fw.GetQueueUnitQuotaName(qu.Unit)
		if err == nil && len(quotas) > 0 {
			if _, ok := q.blockedQuota.Load(quotas[0]); ok {
				q.nextIdx++
				continue
			}
		}
		break
	}
	return q.nextIdx, qu
}

func (q *PriorityQueue) fixQueue(log logr.Logger) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.queue) == 0 {
		return
	}
	last := q.queue[0]
	needReset := false
	for i := 1; i < len(q.queue); i++ {
		if q.lessFunc(last, q.queue[i]) == 1 {
			needReset = true
			log.Error(fmt.Errorf("queue is not sorted"), "pos", i, "pre", last.Name, "cur", q.queue[i].Name)
		}
		last = q.queue[i]
	}
	if needReset {
		slices.SortFunc(q.queue, q.lessFunc)
	}
}

func (q *PriorityQueue) Top(ctx context.Context) *framework.QueueUnitInfo {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.queue) == 0 {
		return nil
	}
	return q.queue[0]
}

// Next will be called to get the next queueUnit to be scheduled.
func (q *PriorityQueue) Next(ctx context.Context) (*framework.QueueUnitInfo, error) {
	var qu *framework.QueueUnitInfo
	q.lock.Lock()
	defer q.lock.Unlock()
	logger := klog.FromContext(ctx)
	klog.Infof("len %v, nextid %v", len(q.queue), q.nextIdx)
	if q.blocked {
		for {
			shouldBlock := false
			shouldRejudge := false
			_, qu = q.findNextQueueUnit()
			reason := ""
			if qu == nil {
				block := []string{}
				q.blockedQuota.Range(func(key, value any) bool {
					block = append(block, key.(string))
					return true
				})
				assmued := []string{}
				for qustr := range q.assumed {
					assmued = append(assmued, qustr)
				}
				reason = fmt.Sprintf("queue %s is blocked because no more queueUnits to schedule, blockedQuotas=(%v), assumed=(%v)", q.name, block, assmued)
				if time.Since(q.lastScheduledTime) > time.Minute*3 && len(block) > 0 {
					q.resetNextIdxFlag = true
					q.blockedQuota = syncInt{}
					shouldRejudge = true
					logger.V(1).Info("clear q.blockedQuota to avoid queue hang", "lastScheduledTime", q.lastScheduledTime)
				} else {
					q.resetNextIdxFlag = true
					shouldBlock = true
					logger.V(3).Info("Set timer to wake up the queue after 30 seconds")
					q.wakeUpTimer = time.AfterFunc(time.Second*30, func() {
						logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", q.name))
						q.cond.Broadcast()
					})
				}
			} else if time.Since(q.lastScheduledTime) < 5*time.Second &&
				qu.Unit.Namespace+"/"+qu.Name == q.lastScheduledQueueUnit {
				reason = fmt.Sprintf("queue %s is blocked because of throttling", q.name)
				q.resetNextIdxFlag = true
				shouldBlock = true
				logger.V(3).Info("Set timer to wake up the queue after 5 seconds")
				q.wakeUpTimer = time.AfterFunc(time.Second*5, func() {
					logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", q.name))
					q.cond.Broadcast()
				})
			}
			if shouldBlock {
				logger.V(3).Info(reason)
				q.cond.Wait()
				if q.wakeUpTimer != nil {
					q.wakeUpTimer.Stop()
				}
			} else if shouldRejudge {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			_, qu = q.findNextQueueUnit()
			if qu == nil {
				block := []string{}
				q.blockedQuota.Range(func(key, value any) bool {
					block = append(block, key.(string))
					return true
				})
				assmued := []string{}
				for qustr := range q.assumed {
					assmued = append(assmued, qustr)
				}
				logger.V(3).Info(fmt.Sprintf("queue %s is blocked because no more queueUnits to schedule, blockedQuotas=(%v), assumed=(%v)", q.name, block, assmued))
				if len(block) > 0 {
					q.resetNextIdxFlag = true
					q.blockedQuota = syncInt{}
				} else {
					q.resetNextIdxFlag = true
					q.wakeUpTimer = time.AfterFunc(time.Minute, func() {
						logger.V(3).Info(fmt.Sprintf("queue %s is released because of throttling", q.name))
						q.cond.Broadcast()
					})
					q.cond.Wait()
					if q.wakeUpTimer != nil {
						q.wakeUpTimer.Stop()
					}
				}
			} else {
				break
			}
		}
	}
	q.lastScheduledQueueUnit = qu.Unit.Namespace + "/" + qu.Name
	q.lastScheduledTime = time.Now()
	klog.Infof("**************next %v", qu.Name)
	return qu, nil
}

// AddQueueUnitInfo will be called when new queueUnit is added to the queue.
func (q *PriorityQueue) AddQueueUnitInfo(qu *framework.QueueUnitInfo) (bool, error) {
	if utils.IsQueueUnitDequeued(qu.Unit) {
		return false, nil
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	key := qu.Unit.Namespace + "/" + qu.Unit.Name
	if utils.IsQueueUnitReservedNotRun(qu.Unit) {
		q.assumed[key] = struct{}{}
	}
	if _, ok := q.queueUnits[key]; ok {
		return false, fmt.Errorf("queueUnit %s already exists", key)
	}

	q.queueUnits[key] = qu
	index, found := slices.BinarySearchFunc(q.queue, qu, q.lessFunc)
	if found {
		return true, nil
	}
	q.resetNextIdxFlag = true
	q.queue = slices.Insert(q.queue, index, qu)
	// When new queueUnit is added, wake up the scheduler.
	klog.V(1).Infof("queueUnit add event %v in queue %s", key, q.name)
	q.sessionId++
	q.cond.Broadcast()
	return true, nil
}

func (q *PriorityQueue) deleteWithoutLock(qu *v1alpha1.QueueUnit) error {
	key := qu.Namespace + "/" + qu.Name
	qi, ok := q.queueUnits[key]
	if !ok {
		qi = framework.NewQueueUnitInfo(qu)
	}
	idx, found := slices.BinarySearchFunc(q.queue, qi, q.lessFunc)
	delete(q.queueUnits, key)
	delete(q.assumed, key)
	delete(q.updating, key)
	if q.blocked {
		quotas, err := q.fw.GetQueueUnitQuotaName(qu)
		if err == nil && len(quotas) > 0 {
			klog.V(2).Infof("delete event for %v, quota %v no more blocked in queue %v", klog.KObj(qu), quotas[0], q.name)
			q.blockedQuota.Delete(quotas[0])
		}
	}
	if found {
		q.queue = slices.Delete(q.queue, idx, idx+1)
	}
	return nil
}

// Delete will be called when queueUnits are delete, or the queueUnits change to another queue.
func (q *PriorityQueue) Delete(qu *v1alpha1.QueueUnit) error {
	q.lock.Lock()
	defer func() {
		q.lock.Unlock()
	}()

	// When reserved queueUnit is deleted, wake up the scheduler.
	if _, ok := q.assumed[qu.Name]; ok || utils.IsQueueUnitReservedAnyResource(qu) {
		q.sessionId++
	}
	q.resetNextIdxFlag = true
	q.deleteWithoutLock(qu)
	klog.V(2).Infof("queueUnit %v delete event in queue queue %s", klog.KObj(qu), q.name)
	q.cond.Broadcast()
	return nil
}

// return false if old and new are equal
func podSetsChanged(old, new *v1alpha1.QueueUnit) bool {
	oldAdmission := map[string]int32{}
	newAdmission := map[string]int32{}
	for _, ps := range old.Spec.PodSets {
		oldAdmission[ps.Name] = ps.Count
	}
	for _, ps := range new.Spec.PodSets {
		newAdmission[ps.Name] = ps.Count
	}
	if len(oldAdmission) != len(newAdmission) {
		return true
	}
	for k, v := range oldAdmission {
		if newAdmission[k] != v {
			return true
		}
	}
	return false
}

func isAdmissionChanged(old, new *v1alpha1.QueueUnit) bool {
	if len(old.Status.Admissions) != len(new.Status.Admissions) {
		return true
	}
	oldAds := map[string]v1alpha1.Admission{}
	for _, ad := range old.Status.Admissions {
		oldAds[ad.Name] = ad
	}
	for _, ad := range new.Status.Admissions {
		if oldAds[ad.Name].Name != ad.Name {
			return true
		}
		if oldAds[ad.Name].Running != ad.Running {
			return true
		}
		if oldAds[ad.Name].Replicas != ad.Replicas {
			return true
		}
	}
	return false
}

// Update will be called when queueUnits are updated.
func (q *PriorityQueue) Update(old, new *v1alpha1.QueueUnit) error {
	q.lock.Lock()
	defer func() {
		key := new.Namespace + "/" + new.Name
		qi, ok := q.queueUnits[key]
		if !ok {
			q.lock.Unlock()
			return
		}
		if qi.Unit.ResourceVersion != new.ResourceVersion {
			qi.Unit = new.DeepCopy()
		}

		if utils.IsResourceReleased(old, new) {
			// reset nextIdx
			if new.Status.Phase != v1alpha1.SchedFailed {
				q.resetNextIdxFlag = true
				slices.SortFunc(q.queue, q.lessFunc)
			}
			klog.V(2).Infof("queue %s is released because job %v release quota", q.name, klog.KObj(new))
			if q.blocked {
				quotas, err := q.fw.GetQueueUnitQuotaName(new)
				if err == nil && len(quotas) > 0 {
					klog.V(2).Infof("update event for %v, quota %v no more blocked in queue %v", klog.KObj(new), quotas[0], q.name)
					q.blockedQuota.Delete(quotas[0])
				}
			}
			q.cond.Broadcast()
		} else if podSetsChanged(old, new) {
			klog.V(2).Infof("queue %s is released because job %v's request changed", q.name, klog.KObj(new))
			q.cond.Broadcast()
		}
		q.lock.Unlock()
	}()

	if old.Status.Phase == new.Status.Phase && old.Spec.Priority == new.Spec.Priority && !podSetsChanged(old, new) && !isAdmissionChanged(old, new) {
		return nil
	}

	key := new.Namespace + "/" + new.Name
	if utils.IsQueueUnitDequeued(new) {
		if _, ok := q.assumed[key]; ok {
			delete(q.assumed, key)
			q.resetNextIdxFlag = true
			if !utils.IsQueueUnitDequeued(old) && q.waitPodsRunning {
				// when queueUnit convert from reserved to dequeued, we release all quotas
				blockSlice := make([]string, 0)
				q.blockedQuota.Range(func(key, value any) bool {
					blockSlice = append(blockSlice, key.(string))
					return true
				})
				q.blockedQuota = syncInt{}
				klog.V(2).InfoS(fmt.Sprintf("quota (%s) is released because queueUnit %v dequeued in queue %v", blockSlice, klog.KObj(new), q.name))
			}
			defer func() {
				klog.V(2).Infof("queue %s is released because job %v become running", q.name, klog.KObj(new))
				q.cond.Broadcast()
			}()
		}

		idx, found := slices.BinarySearchFunc(q.queue, framework.NewQueueUnitInfo(new), q.lessFunc)
		delete(q.queueUnits, key)
		if found {
			q.queue = slices.Delete(q.queue, idx, idx+1)
		}
		return nil
	}
	// add queueUnit to queue if not present
	if _, ok := q.queueUnits[key]; !ok || old.Spec.Priority != new.Spec.Priority {
		oqi := framework.NewQueueUnitInfo(old)
		qi := framework.NewQueueUnitInfo(new)
		index, found := slices.BinarySearchFunc(q.queue, oqi, q.lessFunc)
		if found {
			q.queue = slices.Delete(q.queue, index, index+1)
		}
		index, _ = slices.BinarySearchFunc(q.queue, qi, q.lessFunc)
		q.queue = slices.Insert(q.queue, index, qi)
		q.queueUnits[key] = qi
	}
	_, updating := q.updating[key]
	//
	if !utils.IsQueueUnitReservedAnyResource(new) {
		klog.V(2).InfoS("delete qu from assumed", "queue", q.name, "qu", klog.KObj(new), "reason", "not reserved")
		delete(q.assumed, key)
	} else if !updating && utils.IsQueueUnitAllRunning(new) {
		klog.V(2).InfoS("delete qu from assumed", "queue", q.name, "qu", klog.KObj(new), "reason", "all running")
		q.resetNextIdxFlag = true
		delete(q.assumed, key)
	} else if !utils.IsQueueUnitAllRunning(new) {
		klog.V(2).InfoS("insert qu to assumed", "queue", q.name, "qu", klog.KObj(new), "reason", "not all running")
		q.assumed[key] = struct{}{}
	}
	return nil
}

// Reserve will be called when a queueUnit is scheduled
func (q *PriorityQueue) Reserve(ctx context.Context, qi *framework.QueueUnitInfo) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	logger := klog.FromContext(ctx)
	// no more queueunit can be scheduled in this queue
	if q.waitPodsRunning && len(q.assumed) > 0 {
		if !q.preemptFlag.CompareAndSwap(0, 1) {
			return fmt.Errorf("queue %s is preempting, not allowed to preempt again", q.name)
		}
		startTime := time.Now()
		victims := []*framework.QueueUnitInfo{}
		ps := map[string]map[string]framework.Admission{}
		for assmed := range q.assumed {
			if assumedQi, ok := q.queueUnits[assmed]; !ok {
				delete(q.assumed, assmed)
				continue
			} else if q.lessFunc(qi, assumedQi) < 0 {
				victims = append(victims, assumedQi)
				pps, _ := utils.GetResourcesCanReclaim(assumedQi.Unit)
				ps[assumedQi.Unit.Namespace+"/"+assumedQi.Unit.Name] = pps
			}
		}
		// recheck from the first queueunit
		// 20250724: remove this reset because when a queueunit failed to preempt other job
		// the queue will endless try.
		// q.resetNextIdxFlag = true
		if len(victims) > 0 {
			q.preemptQueueUnits(victims, ps, startTime)
			victimNames := []string{}
			for _, v := range victims {
				victimNames = append(victimNames, v.Name)
			}
			q.preemptFlag.CompareAndSwap(1, 0)
			return fmt.Errorf("preemption: waiting for queueUnit preempted, victims: [%v]", strings.Join(victimNames, ","))
		} else {
			q.preemptFlag.CompareAndSwap(1, 0)
			assumed := []string{}
			for k := range q.assumed {
				assumed = append(assumed, k)
			}
			logger.V(2).Info("preemption: no more queueUnit allowed to schedule in this queue", "assumed", assumed)
			return fmt.Errorf("no more queueUnit allowed to schedule in this queue")
		}
	}
	if !q.blocked {
		q.nextIdx++
	} else {
		q.resetNextIdxFlag = true
	}
	q.assumed[qi.Unit.Namespace+"/"+qi.Unit.Name] = struct{}{}
	q.updating[qi.Unit.Namespace+"/"+qi.Unit.Name] = struct{}{}
	return nil
}

// AddUnschedulableIfNotPresent will be call when a queueUnit is unschedulable, allocatableChangedDuringScheduling
// indicates whether there is a quota change during the scheduling.
func (q *PriorityQueue) AddUnschedulableIfNotPresent(ctx context.Context, qi *framework.QueueUnitInfo, allocatableChangedDuringScheduling bool) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if !allocatableChangedDuringScheduling && !q.blocked {
		q.nextIdx++
	} else {
		q.resetNextIdxFlag = true
	}
	if klog.V(4).Enabled() {
		klog.FromContext(ctx).Info("debug: add unschedulable queue unit to queue", "queueunit", klog.KObj(qi.Unit), "nextIdx", q.nextIdx, "allocatableChangedDuringScheduling", allocatableChangedDuringScheduling,
			"block", q.blocked)
	}

	if q.blocked && !allocatableChangedDuringScheduling {
		quotas, err := q.fw.GetQueueUnitQuotaName(qi.Unit)
		if err == nil && len(quotas) > 0 {
			klog.FromContext(ctx).V(2).Info(fmt.Sprintf("unschedulable event for %v, quota %v is blocked in queue %v", klog.KObj(qi.Unit), quotas[0], q.name))
			q.blockedQuota.Store(quotas[0], 1)
		}
	}
	delete(q.assumed, qi.Unit.Namespace+"/"+qi.Unit.Name)
	delete(q.updating, qi.Unit.Namespace+"/"+qi.Unit.Name)
	return nil
}

// List returns all queueUnits in this queue.
func (q *PriorityQueue) List() []*framework.QueueUnitInfo {
	q.lock.RLock()
	defer q.lock.RUnlock()

	res := []*framework.QueueUnitInfo{}
	for _, qu := range q.queueUnits {
		res = append(res, qu)
	}
	return res
}

// Fix will be called when quota info is changed. Queue should determine whether the queueUnit should be moved to
// another queue.
func (q *PriorityQueue) Fix(g framework.QueueUnitMappingFunc) []*framework.QueueUnitInfo {
	q.lock.Lock()
	defer q.lock.Unlock()

	res := []*framework.QueueUnitInfo{}
	for _, qu := range q.queueUnits {
		if pname, _ := g(qu.Unit); pname != q.name {
			res = append(res, qu)
		}
	}
	for _, qu := range res {
		q.deleteWithoutLock(qu.Unit)
	}
	return res
}

var SupportedPolicy = []string{"Priority", "Block"}

// SupportedPolicy return the supported policy of this queue.
func (q *PriorityQueue) SupportedPolicy() []string {
	return SupportedPolicy
}

// ChangePolicy will be called when queue strategy is changed.
func (q *PriorityQueue) ChangePolicy(old, new string) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if old != new {
		klog.V(2).InfoS("Change policy for queue", "queue", q.name, "old", old, "new", new)
	}
	q.blocked = new == "Block"
	if q.wakeUpTimer != nil {
		q.wakeUpTimer.Stop()
	}
	q.cond.Broadcast()
}

// UpdateQueueCr updates the queue CR with the latest configuration.
func (q *PriorityQueue) UpdateQueueCr(new *v1alpha1.Queue) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.queueCr = new
	if q.waitPodsRunning != (new.Annotations[WaitForPodsRunningAnnotation] == "true") {
		klog.V(0).InfoS("Change waitForPodsRunning for queue", "queue", q.name,
			"old", q.waitPodsRunning, "new", new.Annotations[WaitForPodsRunningAnnotation] == "true")
		q.waitPodsRunning = new.Annotations[WaitForPodsRunningAnnotation] == "true"
	}
	if q.enablePreempt != (new.Annotations[EnableQueueUnitPreemption] == "true") {
		klog.V(0).InfoS("Change enablePreempt for queue", "queue", q.name,
			"old", q.enablePreempt, "new", new.Annotations[EnableQueueUnitPreemption] == "true")
		q.enablePreempt = new.Annotations[EnableQueueUnitPreemption] == "true"
	}
	if new.Annotations[MaxDepthAnnotation] != "" {
		maxDepth, err := strconv.ParseInt(new.Annotations[MaxDepthAnnotation], 10, 32)
		if err == nil {
			klog.V(0).InfoS("Update maxDepth for queue", "queue", q.name,
				"old", q.maxDepth, "new", maxDepth)
			q.maxDepth = int32(maxDepth)
		}
	}
}

// SortedList will be called every xxx seconds to get sorted jobs.
// key of returned map is the kind of queue, value is the list of sorted jobs.
func (q *PriorityQueue) SortedList(scheduling ...*framework.QueueUnitInfo) map[string][]v1alpha1.QueueItemDetail {
	q.lock.RLock()
	defer q.lock.RUnlock()

	res := []v1alpha1.QueueItemDetail{}
	for idx, qu := range q.queue {
		priority := int32(0)
		if qu.Unit.Spec.Priority != nil {
			priority = int32(*qu.Unit.Spec.Priority)
		}
		res = append(res, v1alpha1.QueueItemDetail{
			Name:      qu.Unit.Name,
			Namespace: qu.Unit.Namespace,
			Position:  int32(idx + 1),
			Priority:  priority,
		})
	}
	return map[string][]v1alpha1.QueueItemDetail{
		"active": res,
	}
}

// UpdateQueueLimitByParent will be called when parent limit changes.
func (q *PriorityQueue) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string) {
}

// PendingLength returns the number of pending items.
func (q *PriorityQueue) PendingLength() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.queue)
}

// Length returns the current number of items in the queue.
func (q *PriorityQueue) Length() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.queue)
}

// Run starts the queue.
func (q *PriorityQueue) Run() {
}

// Close stops the queue.
func (q *PriorityQueue) Close() {
}

// GetQueueDebugInfo returns debug information about the queue.
func (q *PriorityQueue) GetQueueDebugInfo() queuepolicies.QueueDebugInfo {
	return nil
}

// GetUserQuotaDebugInfo returns debug information about user quotas.
func (q *PriorityQueue) GetUserQuotaDebugInfo() queuepolicies.UserQuotaDebugInfo {
	return nil
}

// Complete will be called when queueUnitInfo is required by a RESTful api.
func (q *PriorityQueue) Complete(*apiv1alpha1.QueueUnit) {
}

func (q *PriorityQueue) DequeueSuccess(qu *v1alpha1.QueueUnit) {
	delete(q.updating, qu.Namespace+"/"+qu.Name)
}
