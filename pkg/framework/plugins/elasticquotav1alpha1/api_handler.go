package elasticquotav1alpha1

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ElasticQuotaUnitDebugInfo struct {
	Name               string
	Resource           corev1.ResourceList
	Priority           int32
	OversoldType       string
	ActualOversoldType string
	CreationTimestamp  v1.Time
	InReserveCache     bool
}

type ElasticQuotaDebugInfo struct {
	Count int64
	Max   map[corev1.ResourceName]int64
	Min   map[corev1.ResourceName]int64

	Used         map[corev1.ResourceName]int64
	SelfUsed     map[corev1.ResourceName]int64
	ChildrenUsed map[corev1.ResourceName]int64

	GuaranteedUsed         map[corev1.ResourceName]int64
	SelfGuaranteedUsed     map[corev1.ResourceName]int64
	ChildrenGuaranteedUsed map[corev1.ResourceName]int64

	OverSoldUsed         map[corev1.ResourceName]int64
	SelfOverSoldUsed     map[corev1.ResourceName]int64
	ChildrenOverSoldUsed map[corev1.ResourceName]int64

	Items map[string]*ElasticQuotaUnitDebugInfo
}

func (eq *ElasticQuota) RegisterApiHandler(engine *gin.Engine) {
	routes := engine.Group("/apis/v1")
	{
		routes.GET("/elasticquota", eq.getDebugInfo())
	}
}

func (eq *ElasticQuota) getDebugInfo() gin.HandlerFunc {
	return func(c *gin.Context) {
		result := eq.GetDebugInfoInternal(true)
		resultData, _ := json.MarshalIndent(result, "", "\t")
		c.Data(http.StatusOK, "%s", resultData)
	}
}

func (eq *ElasticQuota) GetDebugInfoInternal(verbose bool) map[string]*ElasticQuotaDebugInfo {
	result := make(map[string]*ElasticQuotaDebugInfo)

	cache := eq.cache.(*cacheImpl)
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	reserved := eq.cache.GetReserved()

	for queueName, queueInfo := range cache.quotas {
		result[queueName] = &ElasticQuotaDebugInfo{
			Count:                  int64(len(queueInfo.Reserved)),
			Max:                    queueInfo.Max,
			Min:                    queueInfo.Min,
			Used:                   queueInfo.Used,
			SelfUsed:               queueInfo.SelfUsed,
			ChildrenUsed:           queueInfo.ChildrenUsed,
			GuaranteedUsed:         queueInfo.GuaranteedUsed,
			SelfGuaranteedUsed:     queueInfo.SelfGuaranteedUsed,
			ChildrenGuaranteedUsed: queueInfo.ChildrenGuaranteedUsed,
			OverSoldUsed:           queueInfo.OverSoldUsed,
			SelfOverSoldUsed:       queueInfo.SelfOverSoldUsed,
			ChildrenOverSoldUsed:   queueInfo.ChildrenOverSoldUsed,
			Items:                  make(map[string]*ElasticQuotaUnitDebugInfo),
		}

		if !verbose {
			continue
		}

		for _, quInfo := range queueInfo.Reserved {
			oversoldType, actualOversoldType := utils.GetQueueUnitOversoldAnnotations(quInfo.Unit.Annotations)
			itemDebugInfo := &ElasticQuotaUnitDebugInfo{
				Name:               quInfo.Unit.Name,
				Resource:           quInfo.Unit.Spec.Resource.DeepCopy(),
				OversoldType:       oversoldType,
				ActualOversoldType: actualOversoldType,
				CreationTimestamp:  quInfo.Unit.CreationTimestamp,
			}
			if quInfo.Unit.Spec.Priority != nil {
				itemDebugInfo.Priority = *quInfo.Unit.Spec.Priority
			}
			if _, exist := reserved[quInfo.Unit.UID]; exist {
				itemDebugInfo.InReserveCache = true
			} else {
				itemDebugInfo.InReserveCache = false
			}
			result[queueName].Items[quInfo.Unit.Namespace+"/"+quInfo.Unit.Name] = itemDebugInfo
		}
	}

	return result
}
