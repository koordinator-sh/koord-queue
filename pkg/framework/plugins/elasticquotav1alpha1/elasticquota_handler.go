package elasticquotav1alpha1

import (
	"context"

	queuev1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

const (
	KoordQueueNamespace = "koord-queue"
)

func (eq *ElasticQuota) Add(obj interface{}) {
	eq.WaitForFailOverDone()

	elasticQuota := toQuota(obj)
	if elasticQuota == nil {
		return
	}

	eq.tryCreateOrUpdateQueueCr(elasticQuota)
	eq.cache.AddOrUpdateQuota(elasticQuota)

	qus, err := eq.queueUnitLister.List(labels.Everything())
	if err != nil {
		panic(err)
	}
	for _, qu := range qus {
		if len(qu.Status.Admissions) == 0 {
			continue
		}
		eq.Reserve(context.Background(), framework.NewQueueUnitInfo(qu), nil)
	}
}

func (eq *ElasticQuota) Update(oldObj, newObj interface{}) {
	eq.WaitForFailOverDone()

	elasticQuota := toQuota(newObj)
	if elasticQuota == nil {
		return
	}

	eq.tryCreateOrUpdateQueueCr(elasticQuota)
	eq.cache.AddOrUpdateQuota(elasticQuota)
}

func (eq *ElasticQuota) Delete(obj interface{}) {
	eq.WaitForFailOverDone()

	elasticQuota := toQuota(obj)
	if elasticQuota == nil {
		return
	}

	eq.cache.DeleteQuota(elasticQuota)

	ctx := context.Background()
	retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := eq.handle.QueueUnitClient().SchedulingV1alpha1().Queues(KoordQueueNamespace).
			Delete(ctx, elasticQuota.Name, metav1.DeleteOptions{})
		if err != nil {
			klog.Infof("failed delete queue Cr, queueName:%v, err:%v", elasticQuota.Name, err.Error())
			return err
		}

		klog.Infof("success delete queue Cr, queueName:%v", elasticQuota.Name)
		return nil
	})
}

func toQuota(obj interface{}) *v1alpha1.ElasticQuota {
	var elasticQuota *v1alpha1.ElasticQuota
	switch t := obj.(type) {
	case *v1alpha1.ElasticQuota:
		elasticQuota = t
	case cache.DeletedFinalStateUnknown:
		elasticQuota, _ = t.Obj.(*v1alpha1.ElasticQuota)
	}
	return elasticQuota
}

func (eq *ElasticQuota) tryCreateOrUpdateQueueCr(elasticQuota *v1alpha1.ElasticQuota) {
	ctx := context.Background()
	retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existQueueCr, errGet := eq.handle.QueueUnitClient().SchedulingV1alpha1().
			Queues(KoordQueueNamespace).Get(ctx, elasticQuota.Name, metav1.GetOptions{})

		if errGet == nil {
			newQueueCr, needUpdate := makeNewestQueueCr(existQueueCr, elasticQuota)
			if !needUpdate {
				klog.Infof("nothing to update queue Cr, queueName:%v", elasticQuota.Name)
				return nil
			}

			_, errUpdate := eq.handle.QueueUnitClient().SchedulingV1alpha1().Queues(KoordQueueNamespace).Update(
				ctx, newQueueCr, metav1.UpdateOptions{})
			if errUpdate != nil {
				klog.Infof("failed update queue Cr, queueName:%v, err:%v", newQueueCr.Name, errUpdate.Error())
				return errUpdate
			} else {
				klog.Infof("success update queue Cr, queueName:%v", newQueueCr.Name)
				return nil
			}
		} else {
			if errors.IsNotFound(errGet) {
				newQueueCr, _ := makeNewestQueueCr(nil, elasticQuota)
				_, errCreate := eq.handle.QueueUnitClient().SchedulingV1alpha1().Queues(KoordQueueNamespace).Create(
					ctx, newQueueCr, metav1.CreateOptions{})
				if errCreate != nil {
					klog.Infof("failed create queue Cr, queueName:%v, err:%v", newQueueCr.Name, errCreate.Error())
					return errCreate
				} else {
					klog.Infof("success create queue Cr, queueName:%v", newQueueCr.Name)
					return nil
				}
			} else {
				klog.Infof("failed get queue Cr, queueName:%v, err:%v", elasticQuota.Name, errGet.Error())
				return errGet
			}
		}
	})
}

func makeNewestQueueCr(existQueue *queuev1alpha1.Queue, elasticQuota *v1alpha1.ElasticQuota) (*queuev1alpha1.Queue, bool) {
	if existQueue == nil {
		newQueue := &queuev1alpha1.Queue{}
		newQueue.Namespace = KoordQueueNamespace
		newQueue.Name = elasticQuota.Name
		newQueue.Spec = queuev1alpha1.QueueSpec{}

		newQueue.Annotations = make(map[string]string)
		if elasticQuota.Annotations != nil {
			newQueue.Annotations[utils.QuotaKoordQueueEnable] = elasticQuota.Annotations[utils.QuotaKoordQueueEnable]
			// newQueue.Annotations[strategy.MaxFIFOWaitTimeInSecond] = elasticQuota.Annotations[strategy.MaxFIFOWaitTimeInSecond]
			// newQueue.Annotations[strategy.IntelligentPriorityLevels] = elasticQuota.Annotations[strategy.IntelligentPriorityLevels]
			// newQueue.Annotations[strategy.IntelligentAdjustWeightMaxPriority] = elasticQuota.Annotations[strategy.IntelligentAdjustWeightMaxPriority]
			// newQueue.Annotations[strategy.IntelligentAdjustWeightStepToNextPriority] = elasticQuota.Annotations[strategy.IntelligentAdjustWeightStepToNextPriority]
			// newQueue.Annotations[api.UserQuotaAnnotationConfig] = elasticQuota.Annotations[api.UserQuotaAnnotationConfig]
		}

		newQueue.Labels = make(map[string]string)
		if elasticQuota.Labels != nil {
			newQueue.Labels[utils.ParentQuotaNameLabelKey] = elasticQuota.Labels[utils.ParentQuotaNameLabelKey]
		}

		queueDefaultPriority := int32(1000)
		newQueue.Spec.Priority = &queueDefaultPriority

		newPolicy := findMatchedSupportPolicy(elasticQuota)
		if newPolicy == "" {
			newPolicy = "Priority"

			klog.Infof("failed to parse supported queue policy, init as default Priority, "+
				"queueName:%v, label:%v, default:%v", elasticQuota.Name, elasticQuota.Labels[queuepolicies.QueuePolicyLabelKey],
				"Priority")
		}
		newQueue.Spec.QueuePolicy = queuev1alpha1.QueuePolicy(newPolicy)

		// klog.Infof("success to make and create new queue in local, queueName:%v, priority:%v, policy:%v, "+
		// 	"queueEnable:%v, maxFIFOWaitTimeInSecond:%v, intelligentPriorityLevels:%v, "+
		// 	"intelligentAdjustWeightMaxPriority:%v, intelligentAdjustWeightStepToNextPriority:%v,"+
		// 	"UserQuotaAnnotationConfig:%v", newQueue.Name, *newQueue.Spec.Priority, newQueue.Spec.QueuePolicy,
		// 	newQueue.Annotations[utils.QuotaKoordQueueEnable])
		// newQueue.Annotations[strategy.MaxFIFOWaitTimeInSecond],
		// newQueue.Annotations[strategy.IntelligentPriorityLevels], newQueue.Annotations[strategy.IntelligentAdjustWeightMaxPriority],
		// newQueue.Annotations[strategy.IntelligentAdjustWeightStepToNextPriority], newQueue.Annotations[api.UserQuotaAnnotationConfig])

		return newQueue, true
	} else {
		needUpdate := false
		if existQueue.Spec.QueuePolicy != queuev1alpha1.QueuePolicy(elasticQuota.Labels[queuepolicies.QueuePolicyLabelKey]) {
			needUpdate = true
		}

		if existQueue.Annotations[utils.QuotaKoordQueueEnable] != elasticQuota.Annotations[utils.QuotaKoordQueueEnable] ||
			existQueue.Labels[utils.ParentQuotaNameLabelKey] != elasticQuota.Labels[utils.ParentQuotaNameLabelKey] {
			needUpdate = true
		}

		if needUpdate {
			newQueue := existQueue.DeepCopy()

			newPolicy := findMatchedSupportPolicy(elasticQuota)
			if newPolicy == "" {
				newPolicy = "Priority"
			}
			newQueue.Spec.QueuePolicy = queuev1alpha1.QueuePolicy(newPolicy)

			if newQueue.Annotations == nil {
				newQueue.Annotations = make(map[string]string)
			}
			newQueue.Annotations[utils.QuotaKoordQueueEnable] = elasticQuota.Annotations[utils.QuotaKoordQueueEnable]
			// newQueue.Annotations[strategy.MaxFIFOWaitTimeInSecond] = elasticQuota.Annotations[strategy.MaxFIFOWaitTimeInSecond]
			// newQueue.Annotations[strategy.IntelligentPriorityLevels] = elasticQuota.Annotations[strategy.IntelligentPriorityLevels]
			// newQueue.Annotations[strategy.IntelligentAdjustWeightMaxPriority] = elasticQuota.Annotations[strategy.IntelligentAdjustWeightMaxPriority]
			// newQueue.Annotations[strategy.IntelligentAdjustWeightStepToNextPriority] = elasticQuota.Annotations[strategy.IntelligentAdjustWeightStepToNextPriority]
			// newQueue.Annotations[api.UserQuotaAnnotationConfig] = elasticQuota.Annotations[api.UserQuotaAnnotationConfig]

			if newQueue.Labels == nil {
				newQueue.Labels = make(map[string]string)
			}
			if elasticQuota.Labels != nil {
				newQueue.Labels[utils.ParentQuotaNameLabelKey] = elasticQuota.Labels[utils.ParentQuotaNameLabelKey]
			}

			// klog.Infof("success to make and update exist queue in local, queueName:%v, oldPolicy:%v, newPolicy:%v, "+
			// 	"queueEnable:%v, maxFIFOWaitTimeInSecond:%v, intelligentPriorityLevels:%v, "+
			// 	"intelligentAdjustWeightMaxPriority:%v, intelligentAdjustWeightStepToNextPriority:%v,"+
			// 	"UserQuotaAnnotationConfig:%v", newQueue.Name, existQueue.Spec.QueuePolicy, newQueue.Spec.QueuePolicy,
			// 	newQueue.Annotations[utils.QuotaKoordQueueEnable])
			return newQueue, true
		}

		return existQueue, false
	}
}

func findMatchedSupportPolicy(elasticQuota *v1alpha1.ElasticQuota) string {
	newPolicy := ""
	if elasticQuota.Labels != nil && elasticQuota.Labels[queuepolicies.QueuePolicyLabelKey] != "" {
		if newPolicy == "" {
			for _, supportPolicy := range schedulingqueuev2.SupportedPolicy {
				if supportPolicy == elasticQuota.Labels[queuepolicies.QueuePolicyLabelKey] {
					newPolicy = supportPolicy
					break
				}
			}
		}
	}

	return newPolicy
}
