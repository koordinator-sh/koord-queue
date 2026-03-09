package elasticquotatree

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/kube-queue/kube-queue/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
)

type ElasticQuotaUnitDebugInfo struct {
	Name              string
	Resource          corev1.ResourceList
	Priority          int32
	CreationTimestamp v1.Time
}

type ElasticQuotaDebugInfo struct {
	Count int64
	Max   *utils.Resource
	Min   *utils.Resource
	Used  *utils.Resource
	Items map[string]*ElasticQuotaUnitDebugInfo
}

func (eq *ElasticQuota) RegisterApiHandler(engine *gin.Engine) {
	routes := engine.Group("/apis/v1")
	{
		routes.GET("/elasticquotatree", eq.getDebugInfo())
	}
}

func (eq *ElasticQuota) getDebugInfo() gin.HandlerFunc {
	return func(c *gin.Context) {
		result := make(map[string]*ElasticQuotaDebugInfo)

		eq.RLock()
		defer eq.RUnlock()

		for _, queueInfo := range eq.elasticQuotaTree.ElasticQuotaInfos {
			result[queueInfo.Name] = &ElasticQuotaDebugInfo{
				Count: queueInfo.Count,
				Max:   queueInfo.Max.Clone(),
				Min:   queueInfo.Min.Clone(),
				Used:  queueInfo.Used.Clone(),
				Items: make(map[string]*ElasticQuotaUnitDebugInfo),
			}
		}

		qus, err := eq.queueUnitLister.List(labels.Everything())
		if err != nil {
			klog.ErrorS(err, "failed list queueunit on elasticQuotaTree getDebugInfo")
		}
		for _, qu := range qus {
			if reservedQuota, exist := eq.allQueueUnitsMapping[qu.UID]; exist {
				info := eq.elasticQuotaTree.GetElasticQuotaInfoByName(reservedQuota)

				elasticQuotaDebugInfo := result[info.Name]
				if elasticQuotaDebugInfo != nil {
					itemDebugInfo := &ElasticQuotaUnitDebugInfo{
						Name:              qu.Name,
						Resource:          qu.Spec.Resource.DeepCopy(),
						CreationTimestamp: qu.CreationTimestamp,
					}
					if qu.Spec.Priority != nil {
						itemDebugInfo.Priority = *qu.Spec.Priority
					}
					elasticQuotaDebugInfo.Items[qu.Namespace+"/"+qu.Name] = itemDebugInfo
				}
			}
		}

		resultData, _ := json.MarshalIndent(result, "", "\t")
		c.Data(http.StatusOK, "%s", resultData)
	}
}
