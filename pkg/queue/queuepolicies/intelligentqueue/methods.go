package intelligentqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/basequeue"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
	"k8s.io/klog/v2"
)

// AddQueueUnitInfo adds a queue unit to the intelligent queue
func (q *IntelligentQueue) AddQueueUnitInfo(qu *framework.QueueUnitInfo) (bool, error) {
	if utils.IsQueueUnitDequeued(qu.Unit) {
		return false, nil
	}

	q.Lock.Lock()
	defer q.Lock.Unlock()

	key := qu.Unit.Namespace + "/" + qu.Unit.Name
	if utils.IsQueueUnitReservedNotRun(qu.Unit) {
		q.Assumed[key] = struct{}{}
	}
	if _, ok := q.QueueUnits[key]; ok {
		return false, fmt.Errorf("queueUnit %s already exists", key)
	}

	q.QueueUnits[key] = qu

	// Distribute to appropriate sub-queue
	idx := q.distributeToQueue(qu)
	// If the high queue is not empty or the queueUnit is inserted before where we are in the low queue, reset nextIdx
	if len(q.highPriorityQueue) > 0 || idx <= int(q.lowNextIdx)+len(q.highPriorityQueue) {
		q.ResetNextIdxFlag = true
		klog.V(2).Infof("Reset nextIdx for queue %s due to new queueUnit added", q.Name)
	}

	queueType := "low-priority"
	if q.isHighPriority(qu) {
		queueType = "high-FIFO"
	}
	klog.V(1).Infof("queueUnit add event %v (priority=%v) to %s queue in %s", key, q.getPriority(qu), queueType, q.Name)

	q.SessionId++
	q.Cond.Broadcast()
	return true, nil
}

// Top returns the next queue unit to be scheduled without updating nextIdx
func (q *IntelligentQueue) Top(ctx context.Context) *framework.QueueUnitInfo {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	// First check high priority queue (FIFO)
	if len(q.highPriorityQueue) > 0 {
		return q.highPriorityQueue[0]
	}

	// Then check low priority queue (priority ordering)
	if len(q.lowPriorityQueue) > 0 {
		return q.lowPriorityQueue[0]
	}

	return nil
}

// findNextQueueUnit finds the next schedulable queue unit
// Returns the queue unit and a boolean indicating which queue it came from (true = high priority)
// For high priority tasks, we don't increment nextIdx on failure to allow retrying the same task
// For low priority tasks, we increment nextIdx on failure to move to the next task
func (q *IntelligentQueue) findNextQueueUnit(ctx context.Context) (*framework.QueueUnitInfo, int, bool) {
	logger := klog.FromContext(ctx)
	// First try high priority queue (priority + timestamp ordering)
	availableHighPrioQueueUnits := 0
	for q.highNextIdx < int32(len(q.highPriorityQueue)) && (q.MaxDepth <= 0 || q.highNextIdx < q.MaxDepth) {
		qu := q.highPriorityQueue[q.highNextIdx]
		if utils.IsQueueUnitSatisfied(qu.Unit) {
			q.highNextIdx++
			continue
		}
		availableHighPrioQueueUnits++
		quotas, err := q.Fw.GetQueueUnitQuotaName(qu.Unit)
		if err == nil && len(quotas) > 0 {
			if _, ok := q.BlockedQuota.Load(quotas[0]); ok {
				logger.V(4).Info("queueUnit is blocked by quota", "quota", quotas[0], "queueUnit", qu.Unit.Namespace+"/"+qu.Unit.Name)
				q.highNextIdx++
				continue
			}
		}
		return qu, int(q.highNextIdx), true
	}
	if availableHighPrioQueueUnits > 0 || q.MaxDepth > 0 && q.highNextIdx >= q.MaxDepth {
		return nil, -1, false
	}

	// Then try low priority queue (priority + timestamp ordering)
	for q.lowNextIdx < int32(len(q.lowPriorityQueue)) && (q.MaxDepth <= 0 || (q.highNextIdx+q.lowNextIdx) < q.MaxDepth) {
		qu := q.lowPriorityQueue[q.lowNextIdx]
		if utils.IsQueueUnitSatisfied(qu.Unit) {
			q.lowNextIdx++
			continue
		}
		return qu, int(q.highNextIdx + q.lowNextIdx), false
	}

	return nil, -2, false
}

// Next returns the next queue unit to be scheduled
func (q *IntelligentQueue) Next(ctx context.Context) (*framework.QueueUnitInfo, error) {
	var qu *framework.QueueUnitInfo
	q.Lock.Lock()
	defer q.Lock.Unlock()

	logger := klog.FromContext(ctx)

	for {
		if q.Closed {
			return nil, fmt.Errorf("queue %s is closed", q.Name)
		}
		if q.ResetNextIdxFlag {
			q.highNextIdx = 0
			q.lowNextIdx = 0
			q.ResetNextIdxFlag = false
			q.LastResetTime = time.Now()
			logger.V(4).Info("Reset next index flag")
		}
		shouldBlock := false
		shouldRejudge := false
		var index int
		qu, index, _ = q.findNextQueueUnit(ctx)
		if index >= 0 {
			logger.V(3).Info("Found next queue unit", "index", index)
		} else if index == -1 {
			logger.V(3).Info("No schedulable queue unit found in high priority queue")
		} else {
			logger.V(3).Info("No schedulable queue unit found in low priority queue")
		}
		reason := ""
		if qu == nil {
			block := []string{}
			q.BlockedQuota.Range(func(key, value any) bool {
				block = append(block, key.(string))
				return true
			})
			assumed := []string{}
			for qustr := range q.Assumed {
				assumed = append(assumed, qustr)
			}
			reason = fmt.Sprintf("queue %s is blocked because no more queueUnits to schedule, blockedQuotas=(%v), assumed=(%v)", q.Name, block, assumed)
			if time.Since(q.LastScheduledTime) > time.Minute*3 && len(block) > 0 {
				q.ResetNextIdxFlag = true
				q.BlockedQuota = basequeue.SyncInt{}
				shouldRejudge = true
				logger.V(1).Info("clear q.BlockedQuota to avoid queue hang", "lastScheduledTime", q.LastScheduledTime)
			} else {
				q.ResetNextIdxFlag = true
				shouldBlock = true
				logger.V(3).Info("Set timer to wake up the queue after 30 seconds")
				q.WakeUpTimer = time.AfterFunc(time.Second*30, func() {
					logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", q.Name))
					q.Cond.Broadcast()
				})
			}
		} else if time.Since(q.LastScheduledTime) < 5*time.Second &&
			qu.Unit.Namespace+"/"+qu.Name == q.LastScheduledQueueUnit {
			reason = fmt.Sprintf("queue %s is blocked because of throttling", q.Name)
			q.ResetNextIdxFlag = true
			shouldBlock = true
			logger.V(3).Info("Set timer to wake up the queue after 5 seconds")
			q.WakeUpTimer = time.AfterFunc(time.Second*5, func() {
				logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", q.Name))
				q.Cond.Broadcast()
			})
		}
		if shouldBlock {
			logger.V(3).Info(reason)
			q.Cond.Wait()
			if q.WakeUpTimer != nil {
				q.WakeUpTimer.Stop()
			}
		} else if shouldRejudge {
			continue
		} else {
			break
		}
	}

	q.LastScheduledQueueUnit = qu.Unit.Namespace + "/" + qu.Name
	q.LastScheduledTime = time.Now()

	queueType := "low-priority"
	if q.isHighPriority(qu) {
		queueType = "high-FIFO"
	}
	klog.Infof("Next queue unit %v from %s queue", qu.Name, queueType)

	return qu, nil
}

// Delete removes a queue unit from the queue
func (q *IntelligentQueue) Delete(qu *v1alpha1.QueueUnit) error {
	q.Lock.Lock()
	defer func() {
		q.Lock.Unlock()
	}()

	key := qu.Namespace + "/" + qu.Name
	qi, ok := q.QueueUnits[key]
	if !ok {
		qi = framework.NewQueueUnitInfo(qu)
	}

	// Remove from appropriate sub-queue
	q.removeFromQueue(qi)

	delete(q.QueueUnits, key)
	delete(q.Assumed, key)
	delete(q.Updating, key)

	quotas, err := q.Fw.GetQueueUnitQuotaName(qu)
	if err == nil && len(quotas) > 0 {
		klog.V(2).Infof("delete event for %v, quota %v no more blocked in queue %v", klog.KObj(qu), quotas[0], q.Name)
		q.BlockedQuota.Delete(quotas[0])
	}

	// When reserved queueUnit is deleted, wake up the scheduler
	if _, ok := q.Assumed[qu.Name]; ok || utils.IsQueueUnitReservedAnyResource(qu) {
		q.SessionId++
	}
	q.ResetNextIdxFlag = true

	klog.V(2).Infof("queueUnit %v delete event in intelligent queue %s", klog.KObj(qu), q.Name)
	q.Cond.Broadcast()
	return nil
}

// podSetsChanged checks if pod sets changed between old and new queue unit
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

// isAdmissionChanged checks if admission changed between old and new queue unit
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

// Update updates a queue unit in the queue
func (q *IntelligentQueue) Update(old, new *v1alpha1.QueueUnit) error {
	q.Lock.Lock()
	defer func() {
		key := new.Namespace + "/" + new.Name
		qi, ok := q.QueueUnits[key]
		if !ok {
			q.Lock.Unlock()
			return
		}
		if qi.Unit.ResourceVersion != new.ResourceVersion {
			qi.Unit = new.DeepCopy()
		}

		if utils.IsResourceReleased(old, new) {
			// reset nextIdx
			if new.Status.Phase != v1alpha1.SchedFailed {
				q.ResetNextIdxFlag = true
			}
			klog.V(2).Infof("queue %s is released because job %v release quota", q.Name, klog.KObj(new))
			quotas, err := q.Fw.GetQueueUnitQuotaName(new)
			if err == nil && len(quotas) > 0 {
				klog.V(2).Infof("update event for %v, quota %v no more blocked in queue %v", klog.KObj(new), quotas[0], q.Name)
				q.BlockedQuota.Delete(quotas[0])
			}
			q.Cond.Broadcast()
		} else if podSetsChanged(old, new) {
			klog.V(2).Infof("queue %s is released because job %v's request changed", q.Name, klog.KObj(new))
			q.Cond.Broadcast()
		}
		q.Lock.Unlock()
	}()

	if old.Status.Phase == new.Status.Phase && old.Spec.Priority == new.Spec.Priority && !podSetsChanged(old, new) && !isAdmissionChanged(old, new) {
		return nil
	}

	key := new.Namespace + "/" + new.Name
	if utils.IsQueueUnitDequeued(new) {
		if _, ok := q.Assumed[key]; ok {
			delete(q.Assumed, key)
			q.ResetNextIdxFlag = true
			klog.V(4).Infof("Reset nextIdx for queue %s due to queueUnit dequeued change", q.Name)
			if !utils.IsQueueUnitDequeued(old) && q.WaitPodsRunning {
				// when queueUnit convert from reserved to dequeued, we release all quotas
				blockSlice := make([]string, 0)
				q.BlockedQuota.Range(func(key, value any) bool {
					blockSlice = append(blockSlice, key.(string))
					return true
				})
				q.BlockedQuota = basequeue.SyncInt{}
				klog.V(2).InfoS(fmt.Sprintf("quota (%s) is released because queueUnit %v dequeued in queue %v", blockSlice, klog.KObj(new), q.Name))
			}
			defer func() {
				klog.V(2).Infof("queue %s is released because job %v become running", q.Name, klog.KObj(new))
				q.Cond.Broadcast()
			}()
		}

		// Remove from queues
		oldQi := framework.NewQueueUnitInfo(old)
		q.removeFromQueue(oldQi)
		delete(q.QueueUnits, key)
		return nil
	}

	// Check if priority changed and crosses threshold
	oldPriority := int32(0)
	newPriority := int32(0)
	if old.Spec.Priority != nil {
		oldPriority = *old.Spec.Priority
	}
	if new.Spec.Priority != nil {
		newPriority = *new.Spec.Priority
	}

	oldIsHigh := oldPriority >= q.priorityThreshold
	newIsHigh := newPriority >= q.priorityThreshold

	if _, ok := q.QueueUnits[key]; !ok || oldIsHigh != newIsHigh {
		// Priority crosses threshold, need to migrate between queues
		oqi := framework.NewQueueUnitInfo(old)
		qi := framework.NewQueueUnitInfo(new)

		q.removeFromQueue(oqi)
		q.distributeToQueue(qi)
		q.QueueUnits[key] = qi

		direction := "low->high"
		if oldIsHigh {
			direction = "high->low"
		}
		klog.V(2).Infof("queue unit %v migrated %s (old priority=%v, new priority=%v, threshold=%v)",
			key, direction, oldPriority, newPriority, q.priorityThreshold)
	} else {
		// Priority didn't cross threshold, just update in place
		oqi := framework.NewQueueUnitInfo(old)
		qi := framework.NewQueueUnitInfo(new)

		q.removeFromQueue(oqi)
		q.distributeToQueue(qi)
		q.QueueUnits[key] = qi
	}

	_, updating := q.Updating[key]

	if !utils.IsQueueUnitReservedAnyResource(new) {
		klog.V(2).InfoS("delete qu from assumed", "queue", q.Name, "qu", klog.KObj(new), "reason", "not reserved")
		delete(q.Assumed, key)
	} else if !updating && utils.IsQueueUnitAllRunning(new) {
		klog.V(2).InfoS("delete qu from assumed", "queue", q.Name, "qu", klog.KObj(new), "reason", "all running")
		q.ResetNextIdxFlag = true
		delete(q.Assumed, key)
	} else if !utils.IsQueueUnitAllRunning(new) {
		klog.V(2).InfoS("insert qu to assumed", "queue", q.Name, "qu", klog.KObj(new), "reason", "not all running")
		q.Assumed[key] = struct{}{}
	}

	return nil
}

// Reserve reserves a queue unit for scheduling
func (q *IntelligentQueue) Reserve(ctx context.Context, qi *framework.QueueUnitInfo) (error, bool) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	logger := klog.FromContext(ctx)

	// Check if we need to wait for pods running
	if q.WaitPodsRunning && len(q.Assumed) > 0 {
		if !q.PreemptFlag.CompareAndSwap(0, 1) {
			return fmt.Errorf("queue %s is preempting, not allowed to preempt again", q.Name), true
		}

		assumed := []string{}
		for k := range q.Assumed {
			assumed = append(assumed, k)
		}
		logger.V(2).Info("no more queueUnit allowed to schedule in this queue", "assumed", assumed)
		q.PreemptFlag.CompareAndSwap(1, 0)
		return fmt.Errorf("no more queueUnit allowed to schedule in this queue"), false
	}

	q.Assumed[qi.Unit.Namespace+"/"+qi.Unit.Name] = struct{}{}
	q.Updating[qi.Unit.Namespace+"/"+qi.Unit.Name] = struct{}{}
	return nil, false
}

// DequeueSuccess is called when a queue unit is successfully dequeued
func (q *IntelligentQueue) DequeueSuccess(qu *v1alpha1.QueueUnit) {
	delete(q.Updating, qu.Namespace+"/"+qu.Name)
}

// Preempt preempts lower priority queue units
func (q *IntelligentQueue) Preempt(ctx context.Context, qi *framework.QueueUnitInfo) error {
	// Not implemented for now - can inherit from PriorityQueue if needed
	return fmt.Errorf("preemption not supported in intelligent queue yet")
}

// AddUnschedulableIfNotPresent adds an unschedulable queue unit
func (q *IntelligentQueue) AddUnschedulableIfNotPresent(ctx context.Context, qi *framework.QueueUnitInfo, allocatableChangedDuringScheduling bool) error {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	if !allocatableChangedDuringScheduling {
		// For high priority tasks, we don't increment nextIdx to allow retrying the same task
		// For low priority tasks, we increment nextIdx to move to the next task
		if q.isHighPriority(qi) {
			// Do not increment highNextIdx for high priority tasks
		} else {
			q.lowNextIdx++
		}
	} else {
		q.ResetNextIdxFlag = true
		klog.V(4).Infof("Reset nextIdx for queue %s due to allocatable change", q.Name)
	}

	if klog.V(4).Enabled() {
		klog.FromContext(ctx).Info("debug: add unschedulable queue unit to queue",
			"queueunit", klog.KObj(qi.Unit),
			"highNextIdx", q.highNextIdx,
			"lowNextIdx", q.lowNextIdx,
			"allocatableChangedDuringScheduling", allocatableChangedDuringScheduling)
	}

	if !allocatableChangedDuringScheduling {
		quotas, err := q.Fw.GetQueueUnitQuotaName(qi.Unit)
		if err == nil && len(quotas) > 0 {
			klog.FromContext(ctx).V(2).Info(fmt.Sprintf("unschedulable event for %v, quota %v is blocked in queue %v", klog.KObj(qi.Unit), quotas[0], q.Name))
			q.BlockedQuota.Store(quotas[0], 1)
		}
	}

	delete(q.Assumed, qi.Unit.Namespace+"/"+qi.Unit.Name)
	delete(q.Updating, qi.Unit.Namespace+"/"+qi.Unit.Name)
	return nil
}

// List returns all queue units in the queue
func (q *IntelligentQueue) List() []*framework.QueueUnitInfo {
	q.Lock.RLock()
	defer q.Lock.RUnlock()

	res := []*framework.QueueUnitInfo{}
	for _, qu := range q.QueueUnits {
		res = append(res, qu)
	}
	return res
}

// Fix fixes queue unit assignments when quota changes
func (q *IntelligentQueue) Fix(g framework.QueueUnitMappingFunc) []*framework.QueueUnitInfo {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	res := []*framework.QueueUnitInfo{}
	for _, qu := range q.QueueUnits {
		if pname, _ := g(qu.Unit); pname != q.Name {
			res = append(res, qu)
		}
	}
	for _, qu := range res {
		q.removeFromQueue(qu)
		delete(q.QueueUnits, qu.Unit.Namespace+"/"+qu.Unit.Name)
	}
	return res
}

// SupportedPolicy returns supported policies
func (q *IntelligentQueue) SupportedPolicy() []string {
	return []string{"Intelligent"}
}

// ChangePolicy changes queue policy
func (q *IntelligentQueue) ChangePolicy(old, new string) {
	// Not supported - would require rebuilding queue structure
	klog.Warningf("Policy change not supported for intelligent queue from %s to %s", old, new)
}

// UpdateQueueCr updates queue CR configuration
func (q *IntelligentQueue) UpdateQueueCr(new *v1alpha1.Queue) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	q.QueueCr = new

	// Check if threshold changed
	newThreshold := int32(DefaultPriorityThreshold)
	if new.Annotations != nil {
		if thresholdStr, ok := new.Annotations[queuepolicies.PriorityThresholdAnnotationKey]; ok {
			if parsedThreshold, err := strconv.ParseInt(thresholdStr, 10, 32); err == nil {
				newThreshold = int32(parsedThreshold)
			}
		}
	}

	if newThreshold != q.priorityThreshold {
		klog.V(0).InfoS("Change priority threshold for queue", "queue", q.Name,
			"old", q.priorityThreshold, "new", newThreshold)

		oldThreshold := q.priorityThreshold
		q.priorityThreshold = newThreshold

		// Redistribute all queue units
		allUnits := make([]*framework.QueueUnitInfo, 0, len(q.QueueUnits))
		for _, qu := range q.QueueUnits {
			allUnits = append(allUnits, qu)
		}

		// Clear queues
		q.highPriorityQueue = make([]*framework.QueueUnitInfo, 0)
		q.lowPriorityQueue = make([]*framework.QueueUnitInfo, 0)

		// Redistribute
		for _, qu := range allUnits {
			oldIsHigh := q.getPriority(qu) >= oldThreshold
			newIsHigh := q.getPriority(qu) >= newThreshold
			if oldIsHigh != newIsHigh {
				direction := "low->high"
				if oldIsHigh {
					direction = "high->low"
				}
				klog.V(2).Infof("queue unit %v/%v migrated %s due to threshold change",
					qu.Unit.Namespace, qu.Unit.Name, direction)
			}
			q.distributeToQueue(qu)
		}

		q.ResetNextIdxFlag = true
		klog.V(4).Infof("Reset nextIdx for queue %s due to queueCR change", q.Name)
		q.Cond.Broadcast()
	}

	if q.WaitPodsRunning != (new.Annotations[basequeue.WaitForPodsRunningAnnotation] == "true") {
		klog.V(0).InfoS("Change waitForPodsRunning for queue", "queue", q.Name,
			"old", q.WaitPodsRunning, "new", new.Annotations[basequeue.WaitForPodsRunningAnnotation] == "true")
		q.WaitPodsRunning = new.Annotations[basequeue.WaitForPodsRunningAnnotation] == "true"
	}
	if q.EnablePreempt != (new.Annotations[basequeue.EnableQueueUnitPreemption] == "true") {
		klog.V(0).InfoS("Change enablePreempt for queue", "queue", q.Name,
			"old", q.EnablePreempt, "new", new.Annotations[basequeue.EnableQueueUnitPreemption] == "true")
		q.EnablePreempt = new.Annotations[basequeue.EnableQueueUnitPreemption] == "true"
	}
}

// SortedList returns sorted list of queue units for visualization
func (q *IntelligentQueue) SortedList(scheduling ...*framework.QueueUnitInfo) map[string][]v1alpha1.QueueItemDetail {
	q.Lock.RLock()
	defer q.Lock.RUnlock()

	res := []v1alpha1.QueueItemDetail{}

	// First add high priority queue items (FIFO order)
	for idx, qu := range q.highPriorityQueue {
		priority := int32(0)
		if qu.Unit.Spec.Priority != nil {
			priority = *qu.Unit.Spec.Priority
		}
		res = append(res, v1alpha1.QueueItemDetail{
			Name:      qu.Unit.Name,
			Namespace: qu.Unit.Namespace,
			Position:  int32(idx + 1),
			Priority:  priority,
		})
	}

	// Then add low priority queue items (priority order)
	offset := len(q.highPriorityQueue)
	for idx, qu := range q.lowPriorityQueue {
		priority := int32(0)
		if qu.Unit.Spec.Priority != nil {
			priority = *qu.Unit.Spec.Priority
		}
		res = append(res, v1alpha1.QueueItemDetail{
			Name:      qu.Unit.Name,
			Namespace: qu.Unit.Namespace,
			Position:  int32(offset + idx + 1),
			Priority:  priority,
		})
	}

	return map[string][]v1alpha1.QueueItemDetail{
		"active": res,
	}
}

// UpdateQueueLimitByParent updates queue limit by parent
func (q *IntelligentQueue) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string) {
	// Not implemented
}

// PendingLength returns the number of pending items
func (q *IntelligentQueue) PendingLength() int {
	q.Lock.RLock()
	defer q.Lock.RUnlock()
	return len(q.highPriorityQueue) + len(q.lowPriorityQueue)
}

// Length returns the current number of items in the queue
func (q *IntelligentQueue) Length() int {
	q.Lock.RLock()
	defer q.Lock.RUnlock()
	return len(q.highPriorityQueue) + len(q.lowPriorityQueue)
}

// Run starts the queue
func (q *IntelligentQueue) Run() {
	// Nothing to do
}

// Close stops the queue
func (q *IntelligentQueue) Close() {
	// Nothing to do
	q.BaseQueue.Close()
}

// GetQueueDebugInfo returns debug information about the queue
func (q *IntelligentQueue) GetQueueDebugInfo() queuepolicies.QueueDebugInfo {
	return nil
}

// GetUserQuotaDebugInfo returns debug information about user quotas
func (q *IntelligentQueue) GetUserQuotaDebugInfo() queuepolicies.UserQuotaDebugInfo {
	return nil
}

// Complete completes queue unit info for REST API
func (q *IntelligentQueue) Complete(*apiv1alpha1.QueueUnit) {
	// Nothing to do
}
