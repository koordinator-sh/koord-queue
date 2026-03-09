package elasticquotav1alpha1

import (
	quv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"

	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
)

const (
	ParentQuotaNameLabelKey = "quota.scheduling.koordinator.sh/parent"
	KoordRootQuota          = "koordinator-root-quota"
)

func key(q *v1alpha1.ElasticQuota) string {
	return q.Name
}

func getQuotaName(queueUnit *quv1alpha1.QueueUnit) string {
	quotaName := queueUnit.Labels[QuotaNameLabelKey]
	if quotaName == "" {
		quotaName = queueUnit.Labels[AsiQuotaNameLabelKey]
	}
	return quotaName
}

func ResourceUsageRecord(rl v1.ResourceList, recorder *prometheus.GaugeVec, namespace string, op int) {
	if op == 1 {
		for name, value := range rl {
			recorder.WithLabelValues(namespace, string(name)).Add(float64(value.Value()))
		}
	} else {
		for name, value := range rl {
			recorder.WithLabelValues(namespace, string(name)).Sub(float64(value.Value()))
		}
	}
}
