package intelligentqueue

import (
	"slices"
	"strconv"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/util"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/basequeue"
	"k8s.io/klog/v2"
)

const (
	DefaultPriorityThreshold = 4
)

// IntelligentQueue implements intelligent queue strategy with dual-queue mechanism
// High priority tasks (priority >= threshold) are scheduled by priority then timestamp
// High priority tasks will retry the same task on failure
// Low priority tasks (priority < threshold) are scheduled by priority and timestamp
// Low priority tasks will move to the next task on failure
type IntelligentQueue struct {
	*basequeue.BaseQueue

	// Priority threshold for queue separation
	priorityThreshold int32

	// High priority queue (priority + timestamp ordering)
	highPriorityQueue []*framework.QueueUnitInfo

	// Low priority queue (priority + timestamp ordering)
	lowPriorityQueue []*framework.QueueUnitInfo

	// Priority comparison function for both high and low priority tasks
	priorityLessFunc func(a, b *framework.QueueUnitInfo) int

	// Next index for high priority queue
	highNextIdx int32

	// Next index for low priority queue
	lowNextIdx int32
}

// priorityLess compares queue units by priority and timestamp
func priorityLess(a, b *framework.QueueUnitInfo) int {
	prioA := int32(0)
	prioB := int32(0)
	if a.Unit.Spec.Priority != nil {
		prioA = *a.Unit.Spec.Priority
	}
	if b.Unit.Spec.Priority != nil {
		prioB = *b.Unit.Spec.Priority
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

// NewIntelligentQueue creates a new intelligent queue instance
func NewIntelligentQueue(name string,
	q *v1alpha1.Queue,
	fw framework.Handle,
	client versioned.Interface,
	queueUnitLister externalv1alpha1.QueueUnitLister,
	args map[string]string,
	items ...*framework.QueueUnitInfo) queuepolicies.SchedulingQueue {

	// Parse priority threshold from annotation
	threshold := int32(DefaultPriorityThreshold)
	if q.Annotations != nil {
		if thresholdStr, ok := q.Annotations[queuepolicies.PriorityThresholdAnnotationKey]; ok {
			if parsedThreshold, err := strconv.ParseInt(thresholdStr, 10, 32); err == nil {
				threshold = int32(parsedThreshold)
			}
		}
	}

	// Parse maxDepth
	if q.Annotations != nil {
		if maxDepthStr, ok := q.Annotations[basequeue.MaxDepthAnnotation]; ok {
			if parsedDepth, err := strconv.ParseInt(maxDepthStr, 10, 32); err == nil {
				baseQueue := basequeue.NewBaseQueue(name, q, fw, client, queueUnitLister)
				baseQueue.MaxDepth = int32(parsedDepth)
			}
		}
	}

	// Create base queue
	baseQueue := basequeue.NewBaseQueue(name, q, fw, client, queueUnitLister)

	iq := &IntelligentQueue{
		BaseQueue:         baseQueue,
		priorityThreshold: threshold,
		highPriorityQueue: make([]*framework.QueueUnitInfo, 0),
		lowPriorityQueue:  make([]*framework.QueueUnitInfo, 0),
		priorityLessFunc:  priorityLess,
		highNextIdx:       0,
		lowNextIdx:        0,
	}

	klog.V(1).Infof("create new intelligent queue %v, threshold %v (priority >= %v: priority+time, priority < %v: priority+time), maxDepth=%v",
		name, threshold, threshold, threshold, baseQueue.MaxDepth)

	// Distribute existing items to dual queues
	for _, item := range items {
		key := item.Unit.Namespace + "/" + item.Unit.Name
		iq.QueueUnits[key] = item
		iq.distributeToQueue(item)
	}

	return iq
}

// getPriority returns the priority of a queue unit, treating nil as 0
func (q *IntelligentQueue) getPriority(qu *framework.QueueUnitInfo) int32 {
	if qu.Unit.Spec.Priority == nil {
		return 0
	}
	return *qu.Unit.Spec.Priority
}

// isHighPriority checks if a queue unit should be in high priority queue
func (q *IntelligentQueue) isHighPriority(qu *framework.QueueUnitInfo) bool {
	return q.getPriority(qu) >= q.priorityThreshold
}

// distributeToQueue adds queue unit to appropriate sub-queue
func (q *IntelligentQueue) distributeToQueue(qu *framework.QueueUnitInfo) int {
	if q.isHighPriority(qu) {
		// Insert into high priority queue with priority+time ordering
		index, found := slices.BinarySearchFunc(q.highPriorityQueue, qu, q.priorityLessFunc)
		if !found {
			q.highPriorityQueue = slices.Insert(q.highPriorityQueue, index, qu)
		}
		return index
	} else {
		// Insert into low priority queue with priority+time ordering
		index, found := slices.BinarySearchFunc(q.lowPriorityQueue, qu, q.priorityLessFunc)
		if !found {
			q.lowPriorityQueue = slices.Insert(q.lowPriorityQueue, index, qu)
		}
		return index + len(q.highPriorityQueue)
	}
}

// removeFromQueue removes queue unit from appropriate sub-queue
func (q *IntelligentQueue) removeFromQueue(qu *framework.QueueUnitInfo) {
	if q.isHighPriority(qu) {
		// Remove from high priority queue
		index, found := slices.BinarySearchFunc(q.highPriorityQueue, qu, q.priorityLessFunc)
		if found {
			q.highPriorityQueue = slices.Delete(q.highPriorityQueue, index, index+1)
		}
	} else {
		// Remove from low priority queue
		index, found := slices.BinarySearchFunc(q.lowPriorityQueue, qu, q.priorityLessFunc)
		if found {
			q.lowPriorityQueue = slices.Delete(q.lowPriorityQueue, index, index+1)
		}
	}
}

var _ queuepolicies.SchedulingQueue = &IntelligentQueue{}
