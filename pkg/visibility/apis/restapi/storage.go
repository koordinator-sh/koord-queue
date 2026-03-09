package restapi

import (
	"github.com/kube-queue/kube-queue/pkg/controller"
	"k8s.io/apiserver/pkg/registry/rest"
)

func NewStorage(ctrl *controller.Controller) map[string]rest.Storage {
	return map[string]rest.Storage{
		"elasticquotas":            NewElasticQuotaREST(),
		"elasticquotas/queueunits": NewQueueUnitsByQuotaREST(ctrl),
		"queues":                   NewQueueREST(),
		"queues/queueunits":        NewQueueUnitsByQueueREST(ctrl),
	}
}
