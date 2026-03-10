package queue

import (
	"errors"

	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/intelligentqueue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
)

var ErrUnsupportedStrategy = errors.New("unsupported strategy")

var factory map[string]func(name string,
	q *v1alpha1.Queue,
	fw framework.Handle,
	client versioned.Interface,
	queueUnitLister externalv1alpha1.QueueUnitLister,
	args map[string]string,
	items ...*framework.QueueUnitInfo) queuepolicies.SchedulingQueue

func init() {
	factory = map[string]func(name string, q *v1alpha1.Queue, fw framework.Handle, client versioned.Interface, queueUnitLister externalv1alpha1.QueueUnitLister, args map[string]string, items ...*framework.QueueUnitInfo) queuepolicies.SchedulingQueue{}

	for _, p := range schedulingqueuev2.SupportedPolicy {
		factory[p] = schedulingqueuev2.NewPriorityQueue
	}

	// Register intelligent queue strategy
	factory[queuepolicies.Intelligent] = intelligentqueue.NewIntelligentQueue
}

func CreateSchedulingQueue(name, strategy string, q *v1alpha1.Queue, fw framework.Handle, queueUnitLister externalv1alpha1.QueueUnitLister, args map[string]string, items ...*framework.QueueUnitInfo) (queuepolicies.SchedulingQueue, error) {
	if f, ok := factory[strategy]; !ok {
		return nil, ErrUnsupportedStrategy
	} else {
		return f(name, q, fw, fw.QueueUnitClient(), queueUnitLister, args, items...), nil
	}
}
