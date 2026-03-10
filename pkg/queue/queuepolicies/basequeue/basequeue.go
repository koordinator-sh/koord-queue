package basequeue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"k8s.io/klog/v2"
)

const (
	WaitForPodsRunningAnnotation = "kube-queue/wait-for-pods-running"
	EnableQueueUnitPreemption    = "kube-queue/enable-queueunit-preemption"
	MaxDepthAnnotation           = "kube-queue/max-depth"
)

// SyncInt is a thread-safe integer map
type SyncInt struct {
	sync.Map
}

func (s *SyncInt) Store(key string, value int) {
	s.Map.Store(key, value)
}

func (s *SyncInt) Load(key string) (int, bool) {
	v, ok := s.Map.Load(key)
	if !ok {
		return 0, false
	}
	vi, ok := v.(int)
	if !ok {
		return 0, false
	}
	return vi, ok
}

func (s *SyncInt) Delete(key string) {
	s.Map.Delete(key)
}

// BaseQueue contains common fields and methods for all queue implementations
type BaseQueue struct {
	Closed bool

	Lock        sync.RWMutex
	Cond        *sync.Cond
	Fw          framework.Handle
	WakeUpTimer *time.Timer

	Name             string
	ResetNextIdxFlag bool
	LastResetTime    time.Time
	MaxDepth         int32
	SessionId        int64

	// All queue units by key (namespace/name)
	QueueUnits map[string]*framework.QueueUnitInfo

	// Assumed queue units (reserved but not running)
	Assumed map[string]struct{}

	// Updating queue units
	Updating map[string]struct{}

	QueueCr                *v1alpha1.Queue
	BlockedQuota           SyncInt
	LastScheduledTime      time.Time
	LastScheduledQueueUnit string

	WaitPodsRunning bool
	EnablePreempt   bool
	PreemptFlag     atomic.Int32

	// Client for accessing queue units
	Client          versioned.Interface
	QueueUnitLister externalv1alpha1.QueueUnitLister
}

// NewBaseQueue creates a new base queue instance
func NewBaseQueue(name string, q *v1alpha1.Queue, fw framework.Handle, client versioned.Interface, queueUnitLister externalv1alpha1.QueueUnitLister) *BaseQueue {
	bq := &BaseQueue{
		Name:            name,
		Fw:              fw,
		QueueCr:         q,
		QueueUnits:      make(map[string]*framework.QueueUnitInfo),
		Assumed:         make(map[string]struct{}),
		Updating:        make(map[string]struct{}),
		BlockedQuota:    SyncInt{},
		LastResetTime:   time.Now(),
		MaxDepth:        -1,
		Client:          client,
		QueueUnitLister: queueUnitLister,
	}

	bq.Cond = sync.NewCond(&bq.Lock)

	if q.Annotations != nil {
		bq.WaitPodsRunning = q.Annotations[WaitForPodsRunningAnnotation] == "true"
		bq.EnablePreempt = q.Annotations[EnableQueueUnitPreemption] == "true"
	}

	return bq
}

// GetQueueMaxResetDuration returns the maximum duration before resetting queue index
func (bq *BaseQueue) GetQueueMaxResetDuration(queueLen int) time.Duration {
	l := queueLen
	if bq.MaxDepth > 0 && queueLen > int(bq.MaxDepth) {
		l = int(bq.MaxDepth)
	}
	if l > 500 {
		return time.Minute * 5
	}
	return time.Minute * 3
}

// ShouldSkipQueueUnit checks if a queue unit should be skipped during scheduling
func (bq *BaseQueue) ShouldSkipQueueUnit(qu *framework.QueueUnitInfo) bool {
	// Skip if already satisfied
	if utils.IsQueueUnitSatisfied(qu.Unit) {
		return true
	}

	// Skip if quota is blocked
	quotas, err := bq.Fw.GetQueueUnitQuotaName(qu.Unit)
	if err == nil && len(quotas) > 0 {
		if _, ok := bq.BlockedQuota.Load(quotas[0]); ok {
			return true
		}
	}

	return false
}

// // HandleBlockedQueue handles the blocking logic when queue is blocked
// func (bq *BaseQueue) HandleBlockedQueue(ctx context.Context, qu *framework.QueueUnitInfo, hasMoreUnits bool) (shouldBlock bool, shouldRejudge bool) {
// 	logger := klog.FromContext(ctx)

// 	if qu == nil {
// 		block := []string{}
// 		bq.BlockedQuota.Range(func(key, value any) bool {
// 			block = append(block, key.(string))
// 			return true
// 		})
// 		assumed := []string{}
// 		for qustr := range bq.Assumed {
// 			assumed = append(assumed, qustr)
// 		}

// 		if time.Since(bq.LastScheduledTime) > time.Minute*3 && len(block) > 0 {
// 			bq.ResetNextIdxFlag = true
// 			bq.BlockedQuota = SyncInt{}
// 			shouldRejudge = true
// 			logger.V(1).Info("clear blockedQuota to avoid queue hang", "lastScheduledTime", bq.LastScheduledTime)
// 		} else {
// 			bq.ResetNextIdxFlag = true
// 			shouldBlock = true
// 			logger.V(3).Info("queue blocked, no more queueUnits to schedule", "queue", bq.Name, "blockedQuotas", block, "assumed", assumed)
// 			bq.WakeUpTimer = time.AfterFunc(time.Second*30, func() {
// 				logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", bq.Name))
// 				bq.Cond.Broadcast()
// 			})
// 		}
// 		return shouldBlock, shouldRejudge
// 	}

// 	// Throttling check
// 	if time.Since(bq.LastScheduledTime) < 5*time.Second &&
// 		qu.Unit.Namespace+"/"+qu.Unit.Name == bq.LastScheduledQueueUnit {
// 		bq.ResetNextIdxFlag = true
// 		shouldBlock = true
// 		logger.V(3).Info("Set timer to wake up the queue after 5 seconds")
// 		bq.WakeUpTimer = time.AfterFunc(time.Second*5, func() {
// 			logger.V(3).Info(fmt.Sprintf("queue %s is released by wake up timer", bq.Name))
// 			bq.Cond.Broadcast()
// 		})
// 		return shouldBlock, false
// 	}

// 	return false, false
// }

// // HandleNonBlockedQueue handles the non-blocking logic when queue is not blocked
// func (bq *BaseQueue) HandleNonBlockedQueue(ctx context.Context, qu *framework.QueueUnitInfo) {
// 	logger := klog.FromContext(ctx)

// 	if qu == nil {
// 		block := []string{}
// 		bq.BlockedQuota.Range(func(key, value any) bool {
// 			block = append(block, key.(string))
// 			return true
// 		})
// 		assumed := []string{}
// 		for qustr := range bq.Assumed {
// 			assumed = append(assumed, qustr)
// 		}
// 		logger.V(3).Info(fmt.Sprintf("queue %s is blocked because no more queueUnits to schedule, blockedQuotas=(%v), assumed=(%v)", bq.Name, block, assumed))

// 		if len(block) > 0 {
// 			bq.ResetNextIdxFlag = true
// 			bq.BlockedQuota = SyncInt{}
// 		} else {
// 			bq.ResetNextIdxFlag = true
// 			bq.WakeUpTimer = time.AfterFunc(time.Minute, func() {
// 				logger.V(3).Info(fmt.Sprintf("queue %s is released because of throttling", bq.Name))
// 				bq.Cond.Broadcast()
// 			})
// 			bq.Cond.Wait()
// 			if bq.WakeUpTimer != nil {
// 				bq.WakeUpTimer.Stop()
// 			}
// 		}
// 	}
// }

// UpdateScheduledInfo updates the last scheduled queue unit info
func (bq *BaseQueue) UpdateScheduledInfo(qu *framework.QueueUnitInfo) {
	bq.LastScheduledQueueUnit = qu.Unit.Namespace + "/" + qu.Unit.Name
	bq.LastScheduledTime = time.Now()
}

// CleanupDeletedUnit performs cleanup when a queue unit is deleted
// func (bq *BaseQueue) CleanupDeletedUnit(qu *v1alpha1.QueueUnit) {
// 	key := qu.Namespace + "/" + qu.Name

// 	delete(bq.QueueUnits, key)
// 	delete(bq.Assumed, key)
// 	delete(bq.Updating, key)

// 	quotas, err := bq.Fw.GetQueueUnitQuotaName(qu)
// 	if err == nil && len(quotas) > 0 {
// 		klog.V(2).Infof("delete event for %v, quota %v no more blocked in queue %v", klog.KObj(qu), quotas[0], bq.Name)
// 		bq.BlockedQuota.Delete(quotas[0])
// 	}

// 	// When reserved queueUnit is deleted, wake up the scheduler
// 	if _, ok := bq.Assumed[qu.Name]; ok || utils.IsQueueUnitReservedAnyResource(qu) {
// 		bq.SessionId++
// 	}
// 	bq.ResetNextIdxFlag = true
// }

// HandleResourceRelease handles resource release events
// func (bq *BaseQueue) HandleResourceRelease(old, new *v1alpha1.QueueUnit) {
// 	if utils.IsResourceReleased(old, new) {
// 		// reset nextIdx
// 		if new.Status.Phase != v1alpha1.SchedFailed {
// 			bq.ResetNextIdxFlag = true
// 		}
// 		klog.V(2).Infof("queue %s is released because job %v release quota", bq.Name, klog.KObj(new))
// 		quotas, err := bq.Fw.GetQueueUnitQuotaName(new)
// 		if err == nil && len(quotas) > 0 {
// 			klog.V(2).Infof("update event for %v, quota %v no more blocked in queue %v", klog.KObj(new), quotas[0], bq.Name)
// 			bq.BlockedQuota.Delete(quotas[0])
// 		}
// 		bq.Cond.Broadcast()
// 	}
// }

// HandleDequeueTransition handles the transition from reserved to dequeued
// func (bq *BaseQueue) HandleDequeueTransition(old, new *v1alpha1.QueueUnit) bool {
// 	key := new.Namespace + "/" + new.Name

// 	if utils.IsQueueUnitDequeued(new) {
// 		if _, ok := bq.Assumed[key]; ok {
// 			delete(bq.Assumed, key)
// 			bq.ResetNextIdxFlag = true

// 			if !utils.IsQueueUnitDequeued(old) && bq.WaitPodsRunning {
// 				// when queueUnit convert from reserved to dequeued, we release all quotas
// 				blockSlice := make([]string, 0)
// 				bq.BlockedQuota.Range(func(key, value any) bool {
// 					blockSlice = append(blockSlice, key.(string))
// 					return true
// 				})
// 				bq.BlockedQuota = SyncInt{}
// 				klog.V(2).InfoS(fmt.Sprintf("quota (%s) is released because queueUnit %v dequeued in queue %v", blockSlice, klog.KObj(new), bq.Name))
// 			}

// 			klog.V(2).Infof("queue %s is released because job %v become running", bq.Name, klog.KObj(new))
// 			bq.Cond.Broadcast()
// 			return true
// 		}
// 	}
// 	return false
// }

// UpdateAssumedState updates the assumed state of a queue unit
// func (bq *BaseQueue) UpdateAssumedState(key string, qu *v1alpha1.QueueUnit, updating bool) {
// 	if !utils.IsQueueUnitReservedAnyResource(qu) {
// 		klog.V(2).InfoS("delete qu from assumed", "queue", bq.Name, "qu", klog.KObj(qu), "reason", "not reserved")
// 		delete(bq.Assumed, key)
// 	} else if !updating && utils.IsQueueUnitAllRunning(qu) {
// 		klog.V(2).InfoS("delete qu from assumed", "queue", bq.Name, "qu", klog.KObj(qu), "reason", "all running")
// 		bq.ResetNextIdxFlag = true
// 		delete(bq.Assumed, key)
// 	} else if !utils.IsQueueUnitAllRunning(qu) {
// 		klog.V(2).InfoS("insert qu to assumed", "queue", bq.Name, "qu", klog.KObj(qu), "reason", "not all running")
// 		bq.Assumed[key] = struct{}{}
// 	}
// }

// AddUnschedulableUnit adds an unschedulable queue unit
func (bq *BaseQueue) AddUnschedulableUnit(ctx context.Context, qi *framework.QueueUnitInfo, allocatableChangedDuringScheduling bool) {
	if !allocatableChangedDuringScheduling {
		quotas, err := bq.Fw.GetQueueUnitQuotaName(qi.Unit)
		if err == nil && len(quotas) > 0 {
			klog.FromContext(ctx).V(2).Info(fmt.Sprintf("unschedulable event for %v, quota %v is blocked in queue %v", klog.KObj(qi.Unit), quotas[0], bq.Name))
			bq.BlockedQuota.Store(quotas[0], 1)
		}
	}

	delete(bq.Assumed, qi.Unit.Namespace+"/"+qi.Unit.Name)
	delete(bq.Updating, qi.Unit.Namespace+"/"+qi.Unit.Name)
}

// PodSetsChanged checks if pod sets changed between old and new queue unit
func PodSetsChanged(old, new *v1alpha1.QueueUnit) bool {
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

// IsAdmissionChanged checks if admission changed between old and new queue unit
func IsAdmissionChanged(old, new *v1alpha1.QueueUnit) bool {
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

// List returns all queue units
func (bq *BaseQueue) List() []*framework.QueueUnitInfo {
	res := []*framework.QueueUnitInfo{}
	for _, qu := range bq.QueueUnits {
		res = append(res, qu)
	}
	return res
}

// PendingLength returns the number of pending items (should be overridden by specific implementation)
func (bq *BaseQueue) PendingLength() int {
	return len(bq.QueueUnits)
}

// Length returns the current number of items (should be overridden by specific implementation)
func (bq *BaseQueue) Length() int {
	return len(bq.QueueUnits)
}

// Run starts the queue
func (bq *BaseQueue) Run() {
	// Nothing to do by default
}

// Close stops the queue
func (bq *BaseQueue) Close() {
	// Nothing to do by default
	bq.Closed = true
	bq.Cond.Broadcast()
}

// UpdateQueueLimitByParent updates queue limit by parent
func (bq *BaseQueue) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string) {
	// Not implemented by default
}
