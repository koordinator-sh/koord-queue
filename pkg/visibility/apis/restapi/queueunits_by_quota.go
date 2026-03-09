package restapi

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/kube-queue/kube-queue/pkg/controller"
	"github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
)

type QueueUnitByQuotaREST struct {
	ctrl *controller.Controller
}

var _ rest.Storage = &QueueUnitByQuotaREST{}
var _ rest.Scoper = &QueueUnitByQuotaREST{}
var _ rest.GetterWithOptions = &QueueUnitByQuotaREST{}

func NewQueueUnitsByQuotaREST(ctrl *controller.Controller) *QueueUnitByQuotaREST {
	return &QueueUnitByQuotaREST{ctrl: ctrl}
}

// New implements rest.Storage interface
func (m *QueueUnitByQuotaREST) New() runtime.Object {
	return &v1alpha1.QueueUnitsSummary{}
}

// Destroy implements rest.Storage interface
func (m *QueueUnitByQuotaREST) Destroy() {}

// NamespaceScoped implements rest.Scoper interface
func (m *QueueUnitByQuotaREST) NamespaceScoped() bool {
	return false
}

func (m *QueueUnitByQuotaREST) Get(_ context.Context, name string, obj runtime.Object) (runtime.Object, error) {
	opt, ok := obj.(*v1alpha1.QueueUnitOptions)
	if !ok {
		return nil, errors.New("failed to convert runtime.Object to *v1alpha1.QueueUnitOptions")
	}
	queueUnits := m.ctrl.GetQueueUnitsByQuota(name, opt)

	if klog.V(4).Enabled() {
		debug, _ := json.Marshal(queueUnits)
		klog.Infof("debug: %s", string(debug))
	}
	return &v1alpha1.QueueUnitsSummary{Items: queueUnits}, nil
}

func (m *QueueUnitByQuotaREST) NewGetOptions() (runtime.Object, bool, string) {
	return &v1alpha1.QueueUnitOptions{
		Limit: DefaultQueueUnitsLimit,
	}, false, ""
}
