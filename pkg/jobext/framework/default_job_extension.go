package framework

import (
	"context"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type GenericJobExtensionImpl struct {
	GenericJob
	managedByQueue func(ctx context.Context, obj client.Object) bool
}

var _ RequeueJobExtension = &GenericJobExtensionImpl{}

// func NewGenericJobExtension() GenericJobExtension {
// 	return &GenericJobExtensionImpl{
// 		GenericJob: &AnnotationSuspendJob{},
// 	}
// }

func NewGenericJobExtensionWithJob(adaptor GenericJob, managedByQueue func(ctx context.Context, obj client.Object) bool) GenericJobExtension {
	return &GenericJobExtensionImpl{
		GenericJob:     adaptor,
		managedByQueue: managedByQueue,
	}
}

func (d *GenericJobExtensionImpl) SupportReservation() (GenericReservationJobExtension, bool) {
	rext, ok := d.GenericJob.(GenericReservationJobExtension)
	return rext, ok
}

func (d *GenericJobExtensionImpl) SupportNetworkAware() (NetworkAwareJobExtension, bool) {
	rext, ok := d.GenericJob.(NetworkAwareJobExtension)
	return rext, ok
}

func (d *GenericJobExtensionImpl) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	return d.managedByQueue(ctx, obj)
}

// func (d *GenericJobExtensionImpl) OnQueueUnitDequeued(ctx context.Context, obj client.Object, queueUnit *v1alpha1.QueueUnit, cli client.Client) error {
// 	return d.Resume(ctx, obj, cli)
// }

func (d *GenericJobExtensionImpl) OnQueueUnitRunningTimeout(ctx context.Context, obj client.Object, queueUnit *v1alpha1.QueueUnit, cli client.Client) error {
	err := d.Suspend(ctx, obj, cli)
	if err != nil {
		return err
	}

	return nil
}

func (d *GenericJobExtensionImpl) OnQueueUnitBackoffTimeout(ctx context.Context, obj client.Object, queueUnit *v1alpha1.QueueUnit, cli client.Client) error {
	return nil
}

func (d *GenericJobExtensionImpl) GetRelatedQueueUnit(ctx context.Context, obj client.Object, client client.Client) (*v1alpha1.QueueUnit, error) {
	var qu = &v1alpha1.QueueUnit{}
	suffix := d.GenericJob.QueueUnitSuffix()
	if suffix != "" {
		suffix = "-" + suffix
	}
	err := client.Get(context.Background(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: (obj.GetName() + suffix)}, qu)
	return qu, err
}

func (d *GenericJobExtensionImpl) GetRelatedJob(ctx context.Context, qu *v1alpha1.QueueUnit, client client.Client) client.Object {
	object := d.Object()
	client.Get(context.Background(), types.NamespacedName{Namespace: qu.GetNamespace(), Name: qu.GetName()}, object)
	return object
}
