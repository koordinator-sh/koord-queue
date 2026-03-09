package restapi

import (
	"github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

type ElasticQuotaREST struct {
}

var _ rest.Storage = &ElasticQuotaREST{}
var _ rest.Scoper = &ElasticQuotaREST{}
var _ rest.SingularNameProvider = &ElasticQuotaREST{}

func NewElasticQuotaREST() *ElasticQuotaREST {
	return &ElasticQuotaREST{}
}

// New implements rest.Storage interface
func (m *ElasticQuotaREST) New() runtime.Object {
	return &v1alpha1.ElasticQuota{}
}

// Destroy implements rest.Storage interface
func (m *ElasticQuotaREST) Destroy() {}

// NamespaceScoped implements rest.Scoper interface
func (m *ElasticQuotaREST) NamespaceScoped() bool {
	return false
}

// GetSingularName implements rest.SingularNameProvider interface
func (m *ElasticQuotaREST) GetSingularName() string {
	return "elasticquota"
}
