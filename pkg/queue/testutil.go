package queue

import (
	"context"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
)

func NewQueueForTesting(name string, admissionChecks []string) *Queue {
	ads := map[string]labels.Selector{}
	for _, ad := range admissionChecks {
		ads[ad] = labels.Everything()
	}
	return &Queue{
		name:            name,
		admissionChecks: ads,
		queueImpl:       &fakeQueueImpl{},
	}
}

type fakeQueueImpl struct {
}

func (f *fakeQueueImpl) Top(context.Context) *framework.QueueUnitInfo { return nil }

// Next will be called to get the next queueUnit to be scheduled.
// Next should blocked if no more queueUnit to be scheduled.
func (f *fakeQueueImpl) Next(context.Context) (*framework.QueueUnitInfo, error) { return nil, nil }

// AddQueueUnitInfo will be called when new queueUnit is added to the queue.
func (f *fakeQueueImpl) AddQueueUnitInfo(*framework.QueueUnitInfo) (bool, error) { return true, nil }

// Delete will be called when queueUnits are delete, or the queueUnits change to another queue.
func (f *fakeQueueImpl) Delete(*v1alpha1.QueueUnit) error { return nil }

// Update will be called when queueUnits are updated.
func (f *fakeQueueImpl) Update(*v1alpha1.QueueUnit, *v1alpha1.QueueUnit) error { return nil }

// Reserve will be called when a queueUnit is scheduled
func (f *fakeQueueImpl) Reserve(ctx context.Context, qi *framework.QueueUnitInfo) error { return nil }
func (f *fakeQueueImpl) DequeueSuccess(qu *v1alpha1.QueueUnit)                          {}
func (f *fakeQueueImpl) Preempt(ctx context.Context, qi *framework.QueueUnitInfo) error { return nil }

// AddUnschedulableIfNotPresent will be call when a queueUnit is unschedulable, allocatableChangedDuringScheduling
// indicates whether there is a quota change during the scheduling.
func (f *fakeQueueImpl) AddUnschedulableIfNotPresent(ctx context.Context, qi *framework.QueueUnitInfo, allocatableChangedDuringScheduling bool) error {
	return nil
}

// List() should return all queueUnits in this queue.
func (f *fakeQueueImpl) List() []*framework.QueueUnitInfo { return nil }

// Fix will be called when quota info is changed. Queue should determine whether the queueUnit should be moved to
// another queue.
func (f *fakeQueueImpl) Fix(framework.QueueUnitMappingFunc) []*framework.QueueUnitInfo { return nil }

// SupportedPolicy return the supported policy of this queue.
func (f *fakeQueueImpl) SupportedPolicy() []string         { return nil }
func (f *fakeQueueImpl) ChangePolicy(old, new string)      {}
func (f *fakeQueueImpl) UpdateQueueCr(new *v1alpha1.Queue) {}

// SortedList will be called every xxx seconds.
// key of returned map is the kind of queue
// value of returned map is the list of sorted jobs
func (f *fakeQueueImpl) SortedList(scheduling ...*framework.QueueUnitInfo) map[string][]v1alpha1.QueueItemDetail {
	return nil
}

// Paiqueue specific
func (f *fakeQueueImpl) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string) {}
func (f *fakeQueueImpl) PendingLength() int                                              { return 0 }
func (f *fakeQueueImpl) Length() int                                                     { return 0 }

// Start the queue
func (f *fakeQueueImpl) Run()   {}
func (f *fakeQueueImpl) Close() {}

// Debug endpoint.
func (f *fakeQueueImpl) GetQueueDebugInfo() queuepolicies.QueueDebugInfo         { return nil }
func (f *fakeQueueImpl) GetUserQuotaDebugInfo() queuepolicies.UserQuotaDebugInfo { return nil }

// Complete() will be called when queueUnitInfo is required by a RESTful api.
func (f *fakeQueueImpl) Complete(*apiv1alpha1.QueueUnit) {}
