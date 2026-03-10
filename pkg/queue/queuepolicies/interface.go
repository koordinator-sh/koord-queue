package queuepolicies

import (
	"context"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
)

type QueueDebugInfo interface {
	MarshalJSON() ([]byte, error)
}

type UserQuotaDebugInfo interface {
	MarshalJSON() ([]byte, error)
}

// SchedulingQueue is interface of Single Scheduling Queue.
type SchedulingQueue interface {
	// return the next queueUnit in the queue but do not update nextId
	Top(context.Context) *framework.QueueUnitInfo
	// Next will be called to get the next queueUnit to be scheduled.
	// Next should blocked if no more queueUnit to be scheduled.
	Next(context.Context) (*framework.QueueUnitInfo, error)
	// AddQueueUnitInfo will be called when new queueUnit is added to the queue.
	AddQueueUnitInfo(*framework.QueueUnitInfo) (bool, error)
	// Delete will be called when queueUnits are delete, or the queueUnits change to another queue.
	Delete(*v1alpha1.QueueUnit) error
	// Update will be called when queueUnits are updated.
	Update(*v1alpha1.QueueUnit, *v1alpha1.QueueUnit) error
	// Reserve will be called when a queueUnit is scheduled
	Reserve(ctx context.Context, qi *framework.QueueUnitInfo) (err error, preempted bool)
	DequeueSuccess(qu *v1alpha1.QueueUnit)
	Preempt(ctx context.Context, qi *framework.QueueUnitInfo) error
	// AddUnschedulableIfNotPresent will be call when a queueUnit is unschedulable, allocatableChangedDuringScheduling
	// indicates whether there is a quota change during the scheduling.
	AddUnschedulableIfNotPresent(ctx context.Context, qi *framework.QueueUnitInfo, allocatableChangedDuringScheduling bool) error
	// List() should return all queueUnits in this queue.
	List() []*framework.QueueUnitInfo
	// Fix will be called when quota info is changed. Queue should determine whether the queueUnit should be moved to
	// another queue.
	Fix(framework.QueueUnitMappingFunc) []*framework.QueueUnitInfo
	// SupportedPolicy return the supported policy of this queue.
	SupportedPolicy() []string
	ChangePolicy(old, new string)
	UpdateQueueCr(new *v1alpha1.Queue)
	// SortedList will be called every xxx seconds.
	// key of returned map is the kind of queue
	// value of returned map is the list of sorted jobs
	SortedList(scheduling ...*framework.QueueUnitInfo) map[string][]v1alpha1.QueueItemDetail

	// Paiqueue specific
	UpdateQueueLimitByParent(limit bool, limitParentQuotas []string)
	PendingLength() int
	Length() int
	// Start the queue
	Run()
	Close()
	// Debug endpoint.
	GetQueueDebugInfo() QueueDebugInfo
	GetUserQuotaDebugInfo() UserQuotaDebugInfo
	// Complete() will be called when queueUnitInfo is required by a RESTful api.
	Complete(*apiv1alpha1.QueueUnit)
}
