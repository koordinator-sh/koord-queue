package framework

import (
	"context"
	"time"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	networkv1alpha1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/networkaware/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type JobStatus string

// These status should be able to get from job only
var Created JobStatus = "Created"
var Queuing JobStatus = "Queuing"
var Pending JobStatus = "Pending"
var Running JobStatus = "Running"

// var Suspend JobStatus = "Suspend"
var Failed JobStatus = "Failed"
var Succeeded JobStatus = "Succeeded"

type GenericJob interface {
	Object() client.Object
	DeepCopy(client.Object) client.Object
	GVK() schema.GroupVersionKind
	Resources(ctx context.Context, obj client.Object) v1.ResourceList
	PodSet(ctx context.Context, obj client.Object) []kueue.PodSet
	Priority(ctx context.Context, obj client.Object) (string, *int32)
	QueueUnitSuffix() string
	// podset should be obtained from pod and owner name
	GetPodSetName(ownerName string, p *v1.Pod) string
	GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error)

	GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (JobStatus, time.Time)
	Enqueue(ctx context.Context, obj client.Object, cli client.Client) error
	Suspend(ctx context.Context, obj client.Object, cli client.Client) error
	Resume(ctx context.Context, obj client.Object, cli client.Client) error
}

type GenericJobExtension interface {
	GenericJob
	ManagedByQueue(ctx context.Context, obj client.Object) bool
	GetRelatedQueueUnit(ctx context.Context, obj client.Object, client client.Client) (*v1alpha1.QueueUnit, error)
	GetRelatedJob(ctx context.Context, qu *v1alpha1.QueueUnit, client client.Client) client.Object
	SupportReservation() (GenericReservationJobExtension, bool)
	SupportNetworkAware() (NetworkAwareJobExtension, bool)
}

const ReservationStatus_Succeed = "Succeed"
const ReservationStatus_Failed = "Failed"
const ReservationStatus_Pending = "Pending"

type GenericReservationJobExtension interface {
	Reservation(ctx context.Context, obj client.Object) ([]koordinatorschedulerv1alpha1.Reservation, error)
	ReservationStatus(ctx context.Context, obj client.Object, qu *v1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string)
}

type NetworkAwareJobExtension interface {
	GetNetworkTopologyNamespaceName(ctx context.Context, job client.Object) (string, string)
	GetJobNetworkTopologyCR(ctx context.Context, job client.Object, client client.Client) (jnt *networkv1alpha1.JobNetworkTopology, err error)
}

// type QueueunitsHandler interface {
// 	ConstructQueueUnits(ctx context.Context, obj client.Object, client client.Client) []*v1alpha1.QueueUnit
// 	SyncQueuingJobQueueUnits(ctx context.Context, obj client.Object, queueUnits []*v1alpha1.QueueUnit, cli client.Client) (toCreate, toModify []*v1alpha1.QueueUnit, toDelete []string)
// 	SyncRunningJobQueueUnits(ctx context.Context, obj client.Object, queueUnits []*v1alpha1.QueueUnit, cli client.Client) (toCreate, toModify []*v1alpha1.QueueUnit, toDelete []string)
// 	SyncRunningJobQueueUnits(ctx context.Context, obj client.Object, queueUnits []*v1alpha1.QueueUnit, cli client.Client) (toCreate, toModify []*v1alpha1.QueueUnit, toDelete []string)
// }

type RequeueJobExtension interface {
	OnQueueUnitRunningTimeout(ctx context.Context, obj client.Object, queueUnit *v1alpha1.QueueUnit, client client.Client) error
	OnQueueUnitBackoffTimeout(ctx context.Context, obj client.Object, queueUnit *v1alpha1.QueueUnit, client client.Client) error
}

type JobManager interface {
	FindWorkloadFromPod(ctx context.Context, pod *v1.Pod) *ownerInfo

	GetRelatedPods(ctx context.Context, qu *v1alpha1.QueueUnit) ([]*v1.Pod, error)

	GetPodSetName(ctx context.Context, qu *v1alpha1.QueueUnit, p *v1.Pod) string

	GetRelatedQueueUnit(ctx context.Context, pod *v1.Pod) types.NamespacedName
}
