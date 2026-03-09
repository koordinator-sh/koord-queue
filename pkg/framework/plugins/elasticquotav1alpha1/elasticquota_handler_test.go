package elasticquotav1alpha1

import (
	"testing"

	queuev1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"github.com/stretchr/testify/assert"
)

//please see elasticquotav1alpha1_test.go

func TestMakeNewestQueueCr(t *testing.T) {
	//create
	{
		{
			elasticQuota := v1alpha1.ElasticQuota{}
			elasticQuota.Name = "quota1"
			elasticQuota.Labels = map[string]string{
				queuepolicies.QueuePolicyLabelKey: "11",
				utils.ParentQuotaNameLabelKey:     "Q1",
			}
			elasticQuota.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			newQueueCr, needUpdate := makeNewestQueueCr(nil, &elasticQuota)
			assert.Equal(t, needUpdate, true)
			assert.Equal(t, newQueueCr.Name, "quota1")
			assert.Equal(t, newQueueCr.Namespace, KoordQueueNamespace)
			assert.Equal(t, newQueueCr.Annotations[utils.QuotaKoordQueueEnable], "aa")
			assert.Equal(t, newQueueCr.Labels[utils.ParentQuotaNameLabelKey], "Q1")
			assert.Equal(t, newQueueCr.Spec.QueuePolicy, queuev1alpha1.QueuePolicy("Robin"))
			assert.Equal(t, *newQueueCr.Spec.Priority, int32(1000))
		}
		{
			elasticQuota := v1alpha1.ElasticQuota{}
			elasticQuota.Name = "quota1"
			elasticQuota.Labels = map[string]string{
				queuepolicies.QueuePolicyLabelKey: "Priority",
				utils.ParentQuotaNameLabelKey:     "Q1",
			}
			elasticQuota.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			newQueueCr, needUpdate := makeNewestQueueCr(nil, &elasticQuota)
			assert.Equal(t, needUpdate, true)
			assert.Equal(t, newQueueCr.Name, "quota1")
			assert.Equal(t, newQueueCr.Namespace, KoordQueueNamespace)
			assert.Equal(t, newQueueCr.Labels[utils.ParentQuotaNameLabelKey], "Q1")
			assert.Equal(t, newQueueCr.Annotations[utils.QuotaKoordQueueEnable], "aa")
			assert.Equal(t, newQueueCr.Spec.QueuePolicy, queuev1alpha1.QueuePolicy("Priority"))
			assert.Equal(t, *newQueueCr.Spec.Priority, int32(1000))
		}
	}
	//update
	{
		{
			elasticQuota := v1alpha1.ElasticQuota{}
			elasticQuota.Name = "quota1"
			elasticQuota.Labels = map[string]string{
				queuepolicies.QueuePolicyLabelKey: "PaiStrategyIntelligent",
			}
			elasticQuota.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			priority := int32(1001)
			existQueue := &queuev1alpha1.Queue{}
			existQueue.Namespace = "test"
			existQueue.Name = "quota2"
			existQueue.Spec = queuev1alpha1.QueueSpec{}
			existQueue.Spec.QueuePolicy = "PaiStrategyIntelligent"
			existQueue.Spec.Priority = &priority
			existQueue.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			newQueueCr, needUpdate := makeNewestQueueCr(existQueue, &elasticQuota)
			assert.Equal(t, needUpdate, false)
			assert.Equal(t, newQueueCr.Name, "quota2")
			assert.Equal(t, newQueueCr.Namespace, "test")
			assert.Equal(t, newQueueCr.Annotations[utils.QuotaKoordQueueEnable], "aa")
			assert.Equal(t, newQueueCr.Spec.QueuePolicy, queuev1alpha1.QueuePolicy("PaiStrategyIntelligent"))
			assert.Equal(t, *newQueueCr.Spec.Priority, int32(1001))
		}
		{
			elasticQuota := v1alpha1.ElasticQuota{}
			elasticQuota.Name = "quota1"
			elasticQuota.Labels = map[string]string{
				queuepolicies.QueuePolicyLabelKey: "PaiStrategyIntelligent1",
			}
			elasticQuota.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			priority := int32(1001)
			existQueue := &queuev1alpha1.Queue{}
			existQueue.Namespace = "test"
			existQueue.Name = "quota2"
			existQueue.Spec = queuev1alpha1.QueueSpec{}
			existQueue.Spec.QueuePolicy = "PaiStrategyIntelligent"
			existQueue.Spec.Priority = &priority
			existQueue.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			newQueueCr, needUpdate := makeNewestQueueCr(existQueue, &elasticQuota)
			assert.Equal(t, needUpdate, true)
			assert.Equal(t, newQueueCr.Name, "quota2")
			assert.Equal(t, newQueueCr.Namespace, "test")
			assert.Equal(t, newQueueCr.Annotations[utils.QuotaKoordQueueEnable], "aa")
			assert.Equal(t, newQueueCr.Spec.QueuePolicy, queuev1alpha1.QueuePolicy("PaiStrategyIntelligent"))
			assert.Equal(t, *newQueueCr.Spec.Priority, int32(1001))
		}
		{
			elasticQuota := v1alpha1.ElasticQuota{}
			elasticQuota.Name = "quota1"
			elasticQuota.Labels = map[string]string{
				queuepolicies.QueuePolicyLabelKey: "Priority",
			}
			elasticQuota.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			priority := int32(1001)
			existQueue := &queuev1alpha1.Queue{}
			existQueue.Namespace = "test"
			existQueue.Name = "quota2"
			existQueue.Spec = queuev1alpha1.QueueSpec{}
			existQueue.Spec.QueuePolicy = "Block"
			existQueue.Spec.Priority = &priority
			existQueue.Annotations = map[string]string{
				utils.QuotaKoordQueueEnable: "aa",
			}

			newQueueCr, needUpdate := makeNewestQueueCr(existQueue, &elasticQuota)
			assert.Equal(t, needUpdate, true)
			assert.Equal(t, newQueueCr.Name, "quota2")
			assert.Equal(t, newQueueCr.Namespace, "test")
			assert.Equal(t, newQueueCr.Annotations[utils.QuotaKoordQueueEnable], "aa")
			assert.Equal(t, newQueueCr.Spec.QueuePolicy, queuev1alpha1.QueuePolicy("Priority"))
			assert.Equal(t, *newQueueCr.Spec.Priority, int32(1001))
		}
	}
}
