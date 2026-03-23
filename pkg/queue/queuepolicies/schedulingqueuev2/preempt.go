package schedulingqueuev2

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// deprecated
// tryPreempt find if assumed queue units are the most prioritized
// queue units in queue, if not, queue will try to preempt them.
// At the beginning of the prempt, queue will set queue unit status
// to Preempted, job-extensions will delete the pods and other resources
// in cluster for Preempted queue units. After the deletion of resources,
// job-extensions will set queue unit status to Enqueued.
//
// Algorithm: When we find a high priority queue units in queue need to
// preempt, we preempt from the assumed queue units from low priority to
// high priority until the released resource is enough for the high priority
// queue units to run.
//
// q.lock should be obtained outside
func (q *PriorityQueue) tryPreempt() {
	if len(q.assumed) == 0 {
		return
	}
	if !q.preemptFlag.CompareAndSwap(0, 1) {
		klog.V(1).Infof("queue %s is preempting, not allowed to preempt again", q.name)
		return
	}

	start := time.Now()
	assumed := make([]*framework.QueueUnitInfo, 0, len(q.assumed))
	for k := range q.assumed {
		if _, ok := q.queueUnits[k]; !ok {
			continue
		}
		assumed = append(assumed, q.queueUnits[k])
	}
	preemptedQueueUnitKeys, preempted := q.findQueueUnitsToPreempt(assumed)
	if len(preemptedQueueUnitKeys) == 0 {
		q.preemptFlag.CompareAndSwap(1, 0)
		klog.V(1).Infof("no victims found in queue %s, start at %v", q.name, start.Format(time.TimeOnly))
		return
	}
	klog.V(1).Infof("preempt queue units in queue %s: %v, start at %v", q.name, strings.Join(preemptedQueueUnitKeys, ","), start.Format(time.TimeOnly))
	q.preemptQueueUnits(preempted, nil, start)
	q.preemptFlag.CompareAndSwap(1, 0)
	klog.V(1).Infof("preempt in queue %s completed(%v), start at %v, last %vs", q.name, strings.Join(preemptedQueueUnitKeys, ","), start.Format(time.TimeOnly), time.Since(start).Seconds())
}

func (q *PriorityQueue) preemptQueueUnits(preempted []*framework.QueueUnitInfo, ps map[string]map[string]framework.Admission, start time.Time) {
	if q.fw.QueueUnitClient() == nil {
		return
	}
	var wg sync.WaitGroup
	for _, p := range preempted {
		wg.Add(1)
		go func(victim *framework.QueueUnitInfo) {
			defer wg.Done()
			err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				newQu, err := q.fw.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister().QueueUnits(victim.Unit.Namespace).Get(victim.Unit.Name)
				if err != nil {
					return err
				}
				// can not preempt a queueunit if queueunits do not reserve any resource
				if utils.IsQueueUnitDequeued(newQu) || !utils.IsQueueUnitReservedAnyResource(newQu) {
					klog.V(1).Infof("queueUnit %s is dequeued or not reserved, skip to preempt it", klog.KObj(newQu))
					return nil
				}
				newQuCopy := newQu.DeepCopy()
				pps := ps[newQu.Namespace+"/"+newQu.Name]
				for i, ad := range newQu.Status.Admissions {
					if replica := pps[ad.Name].Replicas; replica > 0 {
						if newQuCopy.Status.Admissions[i].ReclaimState != nil {
							return fmt.Errorf("")
						}
						newQuCopy.Status.Admissions[i].ReclaimState = &v1alpha1.ReclaimState{
							Replicas: replica,
						}
					}
				}
				// newQuCopy.Status.Phase = "Preempted"
				newQuCopy.Status.LastUpdateTime = ptr.To(metav1.Now())
				newQuCopy.Status.Message = "Waiting job extension to reclaim resources."
				_, err = q.fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits(victim.Unit.Namespace).UpdateStatus(context.Background(), newQuCopy, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				klog.Errorf("Failed to update status for queue unit %s/%s: %v", victim.Unit.Namespace, victim.Unit.Name, err)
			}
		}(p)
	}
	wg.Wait()
}

// deprecated
func (q *PriorityQueue) findQueueUnitsToPreempt(assumed []*framework.QueueUnitInfo) ([]string, []*framework.QueueUnitInfo) {
	releasedResource := v1.ResourceList{}
	preemptorResource := v1.ResourceList{}
	preemtped := []*framework.QueueUnitInfo{}
	preemptedQueueUnitKeys := []string{}
	slices.SortFunc(assumed, q.lessFunc)
	for _, quInfo := range q.queue {
		key := quInfo.Unit.Namespace + "/" + quInfo.Unit.Name
		if _, ok := q.assumed[key]; ok {
			continue
		}
		if q.blocked {
			quotas, err := q.fw.GetQueueUnitQuotaName(quInfo.Unit)
			if err == nil && len(quotas) > 0 {
				if _, ok := q.blockedQuota.Load(quotas[0]); ok {
					continue
				}
			}
		}
		if len(assumed) == 0 {
			break
		}
		if assumed[len(assumed)-1] == nil {
			klog.InfoS("assumed is empty")
		} else if assumed[len(assumed)-1].Unit == nil {
			klog.InfoS("assumed unit is empty")
		}
		if q.lessFunc(quInfo, assumed[len(assumed)-1]) < 0 {
			Add(preemptorResource, quInfo.Unit.Spec.Resource)
			// find a low priority assumed pod
			if !IsLargeThan(releasedResource, preemptorResource) {
				// released resource not enough
				for len(assumed) > 0 && q.lessFunc(quInfo, assumed[len(assumed)-1]) < 0 && !IsLargeThan(releasedResource, preemptorResource) {
					last := assumed[len(assumed)-1]
					preemtped = append(preemtped, last)
					preemptedQueueUnitKeys = append(preemptedQueueUnitKeys, last.Unit.Namespace+"/"+last.Unit.Name)
					assumed = assumed[:len(assumed)-1]
					Add(releasedResource, last.Unit.Spec.Resource)
				}
			}
		} else {
			// find a low priority queue unit, stop
			break
		}
	}
	return preemptedQueueUnitKeys, preemtped
}

func (q *PriorityQueue) Preempt(ctx context.Context, qi *framework.QueueUnitInfo) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if !q.waitPodsRunning {
		return fmt.Errorf("preemption is disabled because wait-for-pods-running is disabled")
	}
	if !q.enablePreempt {
		return fmt.Errorf("preemption is disabled in queue")
	}

	if !q.preemptFlag.CompareAndSwap(0, 1) {
		return fmt.Errorf("queue %s is preempting, not allowed to preempt again", q.name)
	}
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	qis := []*framework.QueueUnitInfo{}
	for assumed := range q.assumed {
		if qu, ok := q.queueUnits[assumed]; ok {
			qis = append(qis, qu)
		}
	}
	victimKeys, victims, preemptedPodSets := q.dryRunPreemption(qi, qis)
	if len(victimKeys) == 0 {
		q.preemptFlag.CompareAndSwap(1, 0)
		logger.V(1).Info(fmt.Sprintf("no victims found in queue %s, start at %v", q.name, startTime.Format(time.TimeOnly)))
		return fmt.Errorf("no victims found")
	}

	q.preemptQueueUnits(victims, preemptedPodSets, startTime)
	q.preemptFlag.CompareAndSwap(1, 0)
	logger.V(1).Info(fmt.Sprintf("preempt in queue %s completed(%v), start at %v, last %vs", q.name, strings.Join(victimKeys, ","), startTime.Format(time.TimeOnly), time.Since(startTime).Seconds()))
	return nil
}

func (q *PriorityQueue) getQueueUnitBaseQuotaName(qu *framework.QueueUnitInfo) string {
	quotas, err := q.fw.GetQueueUnitQuotaName(qu.Unit)
	if err != nil {
		return ""
	}
	if len(quotas) > 0 {
		return quotas[0]
	}
	return ""
}

// when quota check failed, we call dryRunPreemption to release quota
// so we just need to consider queueunits in same quota.
func (q *PriorityQueue) dryRunPreemption(preemptor *framework.QueueUnitInfo, assumed []*framework.QueueUnitInfo) ([]string, []*framework.QueueUnitInfo, map[string]map[string]framework.Admission) {
	releasedResource := utils.NewResource(nil)
	preemtped := []*framework.QueueUnitInfo{}
	preemptedReplicas := map[string]map[string]framework.Admission{}
	preemptedQueueUnitKeys := []string{}

	preemptorQuota := q.getQueueUnitBaseQuotaName(preemptor)
	if preemptorQuota == "" {
		return preemptedQueueUnitKeys, preemtped, preemptedReplicas
	}
	// 0807: allow to preempt other quota's queueunits so we remove the code
	// slices.DeleteFunc(assumed, func(qu *framework.QueueUnitInfo) bool {
	// 	return q.getQueueUnitBaseQuotaName(qu) != preemptorQuota
	// })
	slices.SortFunc(assumed, q.lessFunc)

	if len(assumed) == 0 {
		return preemptedQueueUnitKeys, preemtped, preemptedReplicas
	}
	if assumed[len(assumed)-1] == nil {
		klog.InfoS("assumed is empty", "preemptor", klog.KObj(preemptor.Unit))
		return preemptedQueueUnitKeys, preemtped, preemptedReplicas
	} else if assumed[len(assumed)-1].Unit == nil {
		klog.InfoS("assumed unit is empty", "preemptor", klog.KObj(preemptor.Unit))
		return preemptedQueueUnitKeys, preemtped, preemptedReplicas
	}
	if q.lessFunc(preemptor, assumed[len(assumed)-1]) < 0 {
		ads := utils.GetQueueUnitResourceRequirementAds(preemptor.Unit)
		preemptorResource := utils.ConvertFromAdmissionToResource(preemptor.Unit, ads)
		// find a low priority assumed pod
		if !IsResLargeThan(releasedResource, preemptorResource) {
			// released resource not enough
			for len(assumed) > 0 && q.lessFunc(preemptor, assumed[len(assumed)-1]) < 0 && !IsResLargeThan(releasedResource, preemptorResource) {
				last := assumed[len(assumed)-1]
				assumed = assumed[:len(assumed)-1]
				// currently we do not consider resource that reserved in memory
				ps, res := utils.GetResourcesCanReclaim(last.Unit, q.reclaimProtectTime)
				if len(ps) == 0 {
					continue
				}
				preemtped = append(preemtped, last)
				preemptedQueueUnitKeys = append(preemptedQueueUnitKeys, last.Unit.Namespace+"/"+last.Unit.Name)
				preemptedReplicas[last.Unit.Namespace+"/"+last.Unit.Name] = ps
				releasedResource.AddResource(res)
			}
		}
	}
	return preemptedQueueUnitKeys, preemtped, preemptedReplicas
}

// a += b
func Add(a, b v1.ResourceList) {
	for rName, rQuant := range b {
		if _, ok := a[rName]; !ok {
			a[rName] = rQuant.DeepCopy()
			continue
		}

		t := a[rName]
		t.Add(rQuant)
		a[rName] = t
	}
}

// return true if all a > b for all resource types
func IsLargeThan(a, b v1.ResourceList) bool {
	for rName, rQuant := range b {
		if rQuant.Cmp(a[rName]) > 0 {
			return false
		}
	}
	return true
}

// return true if all a > b for all resource types
func IsResLargeThan(a, b *utils.Resource) bool {
	for rName, rQuant := range b.Resources {
		if rQuant > a.Resources[rName] {
			return false
		}
	}
	return true
}
