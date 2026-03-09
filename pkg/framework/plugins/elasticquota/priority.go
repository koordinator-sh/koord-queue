package elasticquotatree

import (
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

var _ framework.MultiQueueSortPlugin = &ElasticQuota{}

func (p *ElasticQuota) MultiQueueLess(q1 *v1alpha1.Queue, q2 *v1alpha1.Queue) bool {
	var p1, p2 int32 = 0, 0
	if q1.Spec.Priority != nil {
		p1 = *(q1.Spec.Priority)
	}
	if q2.Spec.Priority != nil {
		p2 = *(q2.Spec.Priority)
	}

	if p1 != p2 {
		return p1 > p2
	}

	quotaName1 := getQuotaFullName(q1)
	quotaName2 := getQuotaFullName(q2)
	if quotaName1 != quotaName2 {
		return quotaName1 != ""
	}

	if quotaName1 == "" {
		return q1.CreationTimestamp.Before(&q2.CreationTimestamp)
	}

	p.Lock()
	useage1 := p.GetQuotaUsagePercentageByFullName(quotaName1)
	useage2 := p.GetQuotaUsagePercentageByFullName(quotaName2)
	p.Unlock()
	if useage1 != useage2 {
		return useage1 < useage2
	}
	return q1.CreationTimestamp.Before(&q2.CreationTimestamp)
}

func getQuotaFullName(q *v1alpha1.Queue) string {
	if q.Annotations == nil {
		return ""
	}

	if _, ok := q.Annotations[utils.AnnotationQuotaFullName]; ok {
		return q.Annotations[utils.AnnotationQuotaFullName]
	}
	return ""
}
