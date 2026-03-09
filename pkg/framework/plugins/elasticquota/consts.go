package elasticquotatree

import (
	"strings"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	NamespaceAvailableQuotaAnnotation = "kube-queue/available-quota-in-namespace"
	NamespaceAvailableQueueAnnotation = "kube-queue/available-queue-in-namespace"
	QueueAvailableQuotaAnnotation     = "kube-queue/available-quota-in-queue"
	AvailableQuota                    = "kube-queue/available-quota"
	AvailableQUeue                    = "kube-queue/available-queue"

	QuotaFullNameInQueue = "kube-queue/quota-fullname"
	QueueNameInQueueUnit = "kube-queue/queue-name"

	QueueUnitBlockForTestAnno = "kube-queue/block-for-test"
)

func RemoveQuotaStrInNs(ns *v1.Namespace) {
	delete(ns.Annotations, NamespaceAvailableQuotaAnnotation)
	delete(ns.Annotations, AvailableQuota)
}

func RemoveQuotaStrInQueue(queue *v1alpha1.Queue) {
	delete(queue.Annotations, QueueAvailableQuotaAnnotation)
	delete(queue.Annotations, AvailableQuota)
}

func RemoveQueueStr(mp map[string]string) {
	delete(mp, NamespaceAvailableQueueAnnotation)
	delete(mp, AvailableQUeue)
}

func AddQuotaStr(mp map[string]string, quota string) {
	mp[AvailableQuota] = quota
	if _, exist := mp[NamespaceAvailableQuotaAnnotation]; exist {
		mp[NamespaceAvailableQuotaAnnotation] = quota
	}
	if _, exist := mp[QueueAvailableQuotaAnnotation]; exist {
		mp[QueueAvailableQuotaAnnotation] = quota
	}
}

func AddQueueStr(mp map[string]string, quota string) {
	mp[AvailableQUeue] = quota
	if _, exist := mp[NamespaceAvailableQueueAnnotation]; exist {
		mp[NamespaceAvailableQueueAnnotation] = quota
	}
}

func GetStrFromAnnotationForComplience(mp map[string]string, key1, key2 string) string {
	if s, ok := mp[key1]; ok {
		return s
	} else if s, ok := mp[key2]; ok {
		return s
	}
	return ""
}

func GetAvailableQuotaString(annotations map[string]string) string {
	return GetStrFromAnnotationForComplience(annotations, NamespaceAvailableQuotaAnnotation, AvailableQuota)
}

func GetAvailableQuotaStringInQueue(annotations map[string]string) string {
	return GetStrFromAnnotationForComplience(annotations, QueueAvailableQuotaAnnotation, AvailableQuota)
}

func GetAvailableQueueString(annotations map[string]string) string {
	return GetStrFromAnnotationForComplience(annotations, NamespaceAvailableQueueAnnotation, AvailableQUeue)
}

func IsQuotaAvailableInNamespace(quota, ns string, nsToQuotas map[string]sets.Set[string]) bool {
	if qts, ok := nsToQuotas[ns]; ok {
		return qts.Has(quota)
	}
	return false
}

func IsQueueCreatedForQuota(quota string, queueObj *v1alpha1.Queue) bool {
	return queueObj.Annotations[QuotaFullNameInQueue] == quota
}

func IsQuotaAvailableInQueue(quota string, queueToQuotas map[string]sets.Set[string], queueObj *v1alpha1.Queue) bool {
	if qts, ok := queueToQuotas[queueObj.Name]; !ok {
		return IsQueueCreatedForQuota(quota, queueObj)
	} else {
		return qts.Has(quota) || qts.Has("*") || IsQueueCreatedForQuota(quota, queueObj)
	}
}

func GetAvailableQuotasInNamespace(namespace *v1.Namespace) []string {
	availableQuotaStr := GetAvailableQuotaString(namespace.Annotations)
	if availableQuotaStr == "" {
		return nil
	}
	quotas := strings.Split(availableQuotaStr, ",")
	for _, quota := range quotas {
		if quota == "*" {
			return []string{"*"}
		}
	}
	if len(quotas) == 1 && quotas[0] == "" {
		return []string{}
	}
	return quotas
}

// will return * if all quotas are available
func GetAvailableQuotasInQueue(queue *v1alpha1.Queue) []string {
	quotaStr := GetAvailableQuotaStringInQueue(queue.Annotations)
	if quotaStr == "" {
		return []string{}
	}
	quotas := strings.Split(quotaStr, ",")
	for _, quota := range quotas {
		if quota == "*" {
			return []string{"*"}
		}
	}
	if len(quotas) == 1 && quotas[0] == "" {
		return []string{}
	}
	return quotas
}

func BuildAvailableQueueInNamespace(availableQuotas []string, quotasAvaiInQueue map[string][]string) map[string][]string {
	res := map[string][]string{}
	for _, quota := range availableQuotas {
		if avai, ok := quotasAvaiInQueue[quota]; ok {
			res[quota] = avai
		}
	}
	return res
}
