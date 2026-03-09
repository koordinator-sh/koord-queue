package restapi

import (
	"context"
	"encoding/json"

	"github.com/kube-queue/kube-queue/pkg/controller"
	"github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
)

type QueueUnitsByQueueREST struct {
	ctrl *controller.Controller
}

var _ rest.Storage = &QueueUnitsByQueueREST{}
var _ rest.Scoper = &QueueUnitsByQueueREST{}
var _ rest.GetterWithOptions = &QueueUnitsByQueueREST{}

func NewQueueUnitsByQueueREST(ctrl *controller.Controller) *QueueUnitsByQueueREST {
	return &QueueUnitsByQueueREST{ctrl: ctrl}
}

// New implements rest.Storage interface
func (m *QueueUnitsByQueueREST) New() runtime.Object {
	return &v1alpha1.QueueUnitsSummary{}
}

// Destroy implements rest.Storage interface
func (m *QueueUnitsByQueueREST) Destroy() {}

// NamespaceScoped implements rest.Scoper interface
func (m *QueueUnitsByQueueREST) NamespaceScoped() bool {
	return false
}

func (m *QueueUnitsByQueueREST) Get(_ context.Context, name string, obj runtime.Object) (runtime.Object, error) {
	queueUnits, err := m.ctrl.GetQueueUnitsByQueue(name)

	if klog.V(4).Enabled() {
		debug, _ := json.Marshal(queueUnits)
		klog.Infof("debug: %s, %v", string(debug), err)
	}
	return &v1alpha1.QueueUnitsSummary{Items: queueUnits}, err
}

func (m *QueueUnitsByQueueREST) NewGetOptions() (runtime.Object, bool, string) {
	return &v1alpha1.QueueUnitOptions{
		Limit: DefaultQueueUnitsLimit,
	}, false, ""
}
