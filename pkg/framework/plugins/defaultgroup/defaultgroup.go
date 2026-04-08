package defaultgroup

import (
	"context"
	"fmt"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	clientv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "defaultgroup"

var _ framework.QueueUnitMappingPlugin = &DefaultGroup{}

type DefaultGroup struct {
}

// Name returns name of the plugin.
func (d *DefaultGroup) Name() string {
	return Name
}

func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	return &DefaultGroup{}, nil
}

func (d *DefaultGroup) Mapping(q *v1alpha1.QueueUnit) (string, error) {
	return "default", nil
}

func (d *DefaultGroup) AddEventHandler(queueInformer clientv1alpha1.QueueInformer, handle framework.QueueManageHandle) {
	_, _ = queueInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			var q *v1alpha1.Queue
			switch t := obj.(type) {
			case *v1alpha1.Queue:
				q = t
			case cache.DeletedFinalStateUnknown:
				var ok bool
				q, ok = t.Obj.(*v1alpha1.Queue)
				if !ok {
					return false
				}
			default:
				return false
			}
			return q.Name == "default" && q.Namespace == "koord-queue"
		},
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) {
				if err := handle.CreateQueue("default", "", queuepolicies.Priority, nil, "", nil, nil, nil); err != nil {
					panic(fmt.Errorf("failed to create default queue, err: %v", err))
				}
			},
		},
	})
}

func (d *DefaultGroup) Start(ctx context.Context, handle framework.QueueManageHandle) {
	err := handle.CreateQueue("default", "", queuepolicies.Priority, nil, "", nil, nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to create default queue, err: %v", err))
	}
}
