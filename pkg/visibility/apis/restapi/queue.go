package restapi

import (
	"github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

type QueueREST struct {
}

var _ rest.Storage = &QueueREST{}
var _ rest.Scoper = &QueueREST{}
var _ rest.SingularNameProvider = &QueueREST{}

func NewQueueREST() *QueueREST {
	return &QueueREST{}
}

// New implements rest.Storage interface
func (m *QueueREST) New() runtime.Object {
	return &v1alpha1.Queue{}
}

// Destroy implements rest.Storage interface
func (m *QueueREST) Destroy() {}

// NamespaceScoped implements rest.Scoper interface
func (m *QueueREST) NamespaceScoped() bool {
	return false
}

// GetSingularName implements rest.SingularNameProvider interface
func (m *QueueREST) GetSingularName() string {
	return "queue"
}
