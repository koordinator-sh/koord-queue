package elasticquotatree

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	clientv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions/scheduling/v1alpha1"
	queuev1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned"
	schedinformer "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/informers/externalversions"
	externalv1beta1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/listers/scheduling/v1beta1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/elasticquotatree"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/util"
	"github.com/koordinator-sh/koord-queue/pkg/metrics"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "ElasticQuota"

// ResourceQuota is a plugin that implements ResourceQuota filter.
type ElasticQuota struct {
	// whenever you update queueUnits、queueToQuotas and namespaceToQuotas, lock this lock
	sync.RWMutex
	checkHungryQuota           bool
	maxAvailablePreemptibleJob *int64
	frameworkHandle            framework.Handle
	queueLister                queuev1alpha1.QueueLister
	queueInformer              cache.SharedIndexInformer
	queueunitIndexer           cache.Indexer
	queueUnitLister            queuev1alpha1.QueueUnitLister
	elasticQuotaTreeLister     externalv1beta1.ElasticQuotaTreeLister
	elasticQuotaTree           *elasticquotatree.ElasticQuotaTree
	eqClient                   versioned.Interface
	// we should ensure data in queueUnits and queueUnitsSnapshot are always consistent
	// everytime we update queueUnits, we should update queueUnitsSnapshot
	// be sure to obtain lock before update the queueUnits
	// only reserved queueunits will be recorded in queueUnits, queueUnitsSnapshot will
	// contains all queueunits
	queueUnits map[types.UID]*utils.Resource
	// when called reserve, we set queueUnitAssignmentSnapshot
	// when event comming, we set queueUnits
	// after dequeue complete(no matter succeed or failed), we remove assignment from queueUnitAssignmentSnapshot
	// used will be calculated according to queueUnitAssignmentSnapshot and queueUnits
	// and queueUnitAssignmentSnapshot is prior to queueUnits
	queueUnitAssignmentSnapshot map[types.UID]*utils.Resource
	// key is quota name
	reservedQueueUnitsByQuota map[string]sets.Set[types.UID]

	// snapshotUpdateLock should be locked when update queueUnitsSnapshot
	snapshotUpdateLock sync.RWMutex
	// you should always set a new object in snapshot instead of update it
	// allQueueUnitsMapping maps QueueUnit UID to its quota name, used to maintain queueUnitsSnapshot
	allQueueUnitsMapping map[types.UID]string
	// queueUnitsSnapshot tracks all QueueUnits (including pending) per quota, used for hungry quota check
	queueUnitsSnapshot map[string]sets.Set[types.NamespacedName]

	queueToQuotas     map[string]sets.Set[string]
	namespaceToQuotas map[string]sets.Set[string]

	wq chan int
}

func (eq *ElasticQuota) Lock() {
	// klog.Info("elasticquota lock")
	eq.RWMutex.Lock()
}

func (eq *ElasticQuota) Unlock() {
	// klog.Info("elasticquota unlock")
	eq.RWMutex.Unlock()
}

func (eq *ElasticQuota) RLock() {
	// klog.Info("elasticquota rlock")
	eq.RWMutex.RLock()
}

func (eq *ElasticQuota) RUnlock() {
	// klog.Info("elasticquota runlock")
	eq.RWMutex.RUnlock()
}

var _ framework.FilterPlugin = &ElasticQuota{}
var _ framework.ReservePlugin = &ElasticQuota{}
var _ framework.QueueUnitMappingPlugin = &ElasticQuota{}
var _ framework.ApiHandlerPlugin = &ElasticQuota{}
var _ framework.QueueUnitInfoProvider = &ElasticQuota{}

func (eq *ElasticQuota) GetQueueUnitQuotaName(unit *v1alpha1.QueueUnit) ([]string, error) {
	info, err := eq.GetElasticQuotaInfo(unit)
	return []string{info.Name}, err
}

// Name returns name of the plugin.
func (eq *ElasticQuota) Name() string {
	return Name
}

func (eq *ElasticQuota) GetClient() versioned.Interface {
	return eq.eqClient
}

func getQuotaByLabels(q *v1alpha1.QueueUnit) (string, bool) {
	if quota, ok := q.Labels["quota.scheduling.alibabacloud.com/name"]; ok {
		return quota, true
	} else if quota, ok := q.Labels["quota.scheduling.koordinator.sh/name"]; ok {
		return quota, true
	}
	return "", false
}

func (eq *ElasticQuota) getElasticQuotaInfoWithoutLock(q *v1alpha1.QueueUnit) (*elasticquotatree.ElasticQuotaInfo, error) {
	var quota *elasticquotatree.ElasticQuotaInfo
	if quotaname, ok := getQuotaByLabels(q); features.Enabled(features.ElasticQuota) && ok {
		if features.Enabled(features.ElasticQuotaTreeCheckAvailableQuota) &&
			!IsQuotaAvailableInNamespace(quotaname, q.Namespace, eq.namespaceToQuotas) {
			return nil, errors.New("quota is not available in namespace")
		}
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByName(quotaname)
	} else {
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByNamespace(q.Namespace)
	}
	if quota == nil {
		return nil, errors.New("the job doesn't belong to any elastic quota")
	}
	return quota, nil
}

func (eq *ElasticQuota) GetElasticQuotaInfo(q *v1alpha1.QueueUnit) (*elasticquotatree.ElasticQuotaInfo, error) {
	var quota *elasticquotatree.ElasticQuotaInfo
	if quotaname, ok := getQuotaByLabels(q); features.Enabled(features.ElasticQuota) && ok {
		eq.RLock()
		if features.Enabled(features.ElasticQuotaTreeCheckAvailableQuota) &&
			!IsQuotaAvailableInNamespace(quotaname, q.Namespace, eq.namespaceToQuotas) {
			eq.RUnlock()
			return nil, errors.New("quota is not available in namespace")
		}
		eq.RUnlock()
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByName(quotaname)
	} else {
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByNamespace(q.Namespace)
	}
	if quota == nil {
		return nil, errors.New("the job doesn't belong to any elastic quota")
	}
	return quota, nil
}

func (eq *ElasticQuota) GetElasticQuotaInfoWithoutLock(q *v1alpha1.QueueUnit) (*elasticquotatree.ElasticQuotaInfo, error) {
	var quota *elasticquotatree.ElasticQuotaInfo
	if quotaname, ok := getQuotaByLabels(q); features.Enabled(features.ElasticQuota) && ok {
		if features.Enabled(features.ElasticQuotaTreeCheckAvailableQuota) &&
			!IsQuotaAvailableInNamespace(quotaname, q.Namespace, eq.namespaceToQuotas) {
			return nil, errors.New("quota is not available in namespace")
		}
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByName(quotaname)
	} else {
		quota = eq.elasticQuotaTree.GetElasticQuotaInfoByNamespace(q.Namespace)
	}
	if quota == nil {
		return nil, errors.New("the job doesn't belong to any elastic quota")
	}
	return quota, nil
}

func (eq *ElasticQuota) Mapping(q *v1alpha1.QueueUnit) (string, error) {
	if eq == nil || q == nil || eq.elasticQuotaTree == nil {
		return "", errors.New("no available quotas in cluster")
	}
	quota, err := eq.GetElasticQuotaInfo(q)
	if err != nil {
		return "", err
	}
	if quota == nil {
		return "", errors.New("the job doesn't belong to any elastic quota")
	}
	// 这个判断仍然需要吗？
	// if len(quota.Namespaces) == 0 {
	// 	return "", errors.New("there is no namespace in the elastic quota")
	// }
	attemptingQueue := q.Annotations[QueueNameInQueueUnit]
	if attemptingQueue == "" {
		if features.Enabled(features.ElasticQuotaTreeDecoupledQueue) {
			indexer := eq.queueInformer.GetIndexer()
			if indexer == nil {
				klog.Fatal("indexer is nil")
			}
			items, err := indexer.ByIndex(utils.AnnotationQuotaFullName, quota.FullName)
			if err != nil || len(items) == 0 {
				klog.ErrorS(err, "failed to get queue by index", "quota", quota.FullName)
				return "", errors.New("failed to get queue by index, this is a internal error")
			}

			item, ok := items[0].(*v1alpha1.Queue)
			if !ok {
				klog.Errorf("failed to convert item %T to queue.", items[0])
				return "", errors.New("failed to convert item to v1alpha1.Queue, this is a internal error")
			}
			return item.Name, nil
		}
		return "", errors.New("queue name must be set in queue unit")
	}
	queue, err := eq.queueLister.Queues("koord-queue").Get(attemptingQueue)
	if err != nil {
		klog.ErrorS(err, "failed to find queue object in cluster", "queueUnit", q.Name, "namespace", q.Namespace, "attempting", attemptingQueue)
		return "", errors.New("failed to find queue object in cluster")
	}
	if !IsQuotaAvailableInQueue(quota.Name, eq.queueToQuotas, queue) {
		return "", fmt.Errorf("quota %s is not available in queue %s", quota.Name, queue.Name)
	}
	return queue.Name, nil
}

func convertMaxToReadable(rname v1.ResourceName, originMax *resource.Quantity, oversellrate float64) *resource.Quantity {
	var originMaxInt64 int64
	if rname == v1.ResourceCPU {
		originMaxInt64 = originMax.MilliValue()
	} else {
		originMaxInt64 = originMax.Value()
	}
	scaledmaxF64 := float64(originMaxInt64) * oversellrate
	var scaledmax *resource.Quantity
	if rname == v1.ResourceCPU {
		scaledmax = resource.NewMilliQuantity(int64(scaledmaxF64), resource.DecimalSI)
	} else if rname == v1.ResourceMemory {
		scaledmax = resource.NewQuantity(int64(scaledmaxF64), resource.BinarySI)
	} else {
		scaledmax = resource.NewQuantity(int64(scaledmaxF64), resource.DecimalSI)
	}
	return scaledmax
}

func (eq *ElasticQuota) Filter(ctx context.Context, queueUnit *framework.QueueUnitInfo, request map[string]framework.Admission) *framework.Status {
	if queueUnit.Unit.Annotations[QueueUnitBlockForTestAnno] == "true" {
		// this should only used for test
		return framework.NewStatus(framework.Unschedulable, "block for test", nil)
	}

	eq.Lock()
	defer eq.Unlock()

	if eq.elasticQuotaTree == nil || eq.elasticQuotaTree.NamespaceToQuota == nil {
		return framework.NewStatus(framework.Error, "elasticQuotaTree is nil", nil)
	}

	unit := queueUnit.Unit
	info, err := eq.GetElasticQuotaInfoWithoutLock(unit)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error(), nil)
	}
	if info == nil {
		klog.Warningf("can't find elasticquota by the namespace %v", unit.Namespace)
		return framework.NewStatus(framework.Error, fmt.Sprintf("can't find elasticquota by the namespace %v", unit.Namespace), nil)
	}

	resourceRequest := utils.ConvertFromAdmissionToResource(queueUnit.Unit, request)
	mask := resourceRequest.ResourceNames()
	usedLowerThanMin, _ := info.UsedLowerThanMinWithOverSell(eq.frameworkHandle.OversellRate(), mask)
	if eq.checkHungryQuota &&
		!usedLowerThanMin {
		for _, otherInfo := range eq.elasticQuotaTree.ElasticQuotaInfos {
			if !otherInfo.IsLeaf() {
				continue
			}
			if otherInfo.Name != info.Name {
				usedLowerThanMin, _ := otherInfo.UsedLowerThanMinWithOverSell(eq.frameworkHandle.OversellRate(), mask)
				if usedLowerThanMin && len(eq.queueUnitsSnapshot[otherInfo.Name]) > len(eq.reservedQueueUnitsByQuota[otherInfo.Name]) {
					return framework.NewStatus(framework.Unschedulable,
						fmt.Sprintf("%v(used:%v, min:%v) is blocked by quota %v(used:%v,min:%v) because of Min.",
							info.FullName, info.Used, info.Min, otherInfo.FullName, otherInfo.Used, otherInfo.Min), nil)
				}
			}
		}
	}

	if util.IsJobPreemptible(queueUnit) {
		if valid, count := info.ValidatePreemptibleJobCount(eq.maxAvailablePreemptibleJob); !valid {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Too many(%v) preemptible job in quota %v ", count, info.FullName), nil)
		}

		if over, res := info.UsedOverMaxWithOverSell(resourceRequest, eq.frameworkHandle.OversellRate()); over {
			// klog.Warningf("%v elastic quota filter failed request %v max %v used %v with oversellreate %v", QueueUnit.Name, *resourceRequest, info.Max, info.Used, eq.frameworkHandle.OversellRate())
			rname := v1.ResourceName(res)
			if res == "jobs" {
				used := info.Count
				originMax := info.Max.ResourceList()["koord-queue/max-jobs"]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, max %v, used %v, oversellreate %v. Wait for running jobs to complete", res, info.FullName, 1, scaledmax, used, eq.frameworkHandle.OversellRate()), nil)
			} else {
				request := resourceRequest.ResourceList()[rname]
				used := info.Used.ResourceList()[rname]
				originMax := info.Max.ResourceList()[rname]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, max %v, used %v, oversellreate %v. Wait for running jobs to complete", res, info.FullName, request.String(), scaledmax.String(), used.String(), eq.frameworkHandle.OversellRate()), nil)
			}
		}

		if over, pname, res, nodeStatus := info.ParentUsedOverMaxWithOverSell(klog.NewKlogr().WithValues("quota", info.Name, "queueunit", queueUnit.Name, "request", resourceRequest),
			resourceRequest, eq.frameworkHandle.OversellRate()); over {
			// klog.Warningf("%v elastic quota filter failed request %v max %v used %v with oversellreate %v", QueueUnit.Name, *resourceRequest, info.Max, info.Used, eq.frameworkHandle.OversellRate())
			rname := v1.ResourceName(res)
			request := resourceRequest.ResourceList()[rname]
			pinfo := eq.elasticQuotaTree.ElasticQuotaInfos[pname]
			if res == "jobs" {
				used := pinfo.Count
				originMax := pinfo.Max.ResourceList()["koord-queue/max-jobs"]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf(
					"Insufficient quota(%v) in parent quota %v: request %v, max %v, used %v, "+
						"oversellreate %v. Wait for running jobs to complete",
					res, pname, 1, scaledmax, used, eq.frameworkHandle.OversellRate()), nil)
			} else {
				used := pinfo.Used.ResourceList()[rname]
				usedWithReserved := nodeStatus.Resources[rname]
				originMax := pinfo.Max.ResourceList()[rname]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf(
					"Insufficient quota(%v) in parent quota %v: request %v, max %v, used %v, "+
						"used+reserved %v, oversellreate %v. Wait for running jobs to complete",
					res, pname, request.String(), scaledmax.String(), used.String(), usedWithReserved, eq.frameworkHandle.OversellRate()), nil)
			}
		}
	} else {
		totalPreemptCount := 0
		totalPreempt := utils.NewResource(nil)
		if info.EnablePreemptLowerPriority() {
			priority := queueUnit.Unit.Spec.Priority
			reservedQueueUnits := eq.reservedQueueUnitsByQuota[info.Name]
			for unit := range reservedQueueUnits {
				qus, err := eq.queueunitIndexer.ByIndex("queueunits.metadata.uid", string(unit))
				if err != nil {
					klog.ErrorS(err, "failed to get queueunit when try to get resource from queue", "queueunitUid", unit)
					continue
				}
				if len(qus) != 1 {
					klog.ErrorS(nil, "invalid queueunit when try to get resource from queue", "returned queueunits", qus)
					continue
				}
				qu := qus[0].(*v1alpha1.QueueUnit)
				reservedPriority := qu.Spec.Priority
				if utils.GetPriority(reservedPriority) < utils.GetPriority(priority) {
					klog.Infof("queueUnit %v priority %v is higher than reserved queueUnit %v priority %v, preempting",
						qu.Name, utils.GetPriority(reservedPriority), queueUnit.Name, utils.GetPriority(priority))
					totalPreempt.AddResource(utils.GetReservedResource(qu))
					totalPreemptCount++
				}
			}
		}

		// we do not check Max for a non-preemptible job, we should dequeue the non-preemptible job and let it to preempt preemptible jobs.
		if over, res := info.UsedOverMinWithOverSell(resourceRequest, totalPreempt, eq.frameworkHandle.OversellRate()); over {
			// klog.Warningf("%v elastic quota filter failed request %v max %v used %v with oversellreate %v", QueueUnit.Name, *resourceRequest, info.Max, info.Used, eq.frameworkHandle.OversellRate())
			rname := v1.ResourceName(res)
			if res == "jobs" {
				used := info.Count
				originMax := info.Min.ResourceList()["koord-queue/max-jobs"]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, min %v, used %v, preempt %v, oversellreate %v. Wait for running jobs to complete",
					res, info.FullName, 1, scaledmax, used, totalPreemptCount, eq.frameworkHandle.OversellRate()), nil)
			} else {
				request := resourceRequest.ResourceList()[rname]
				used := info.NonPreemptibleUsed.ResourceList()[rname]
				originMax := info.Min.ResourceList()[rname]
				scaledmax := convertMaxToReadable(rname, &originMax, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, min %v, used %v, preempt %v, oversellreate %v. Wait for running jobs to complete",
					res, info.FullName, request.String(), scaledmax.String(), used.String(), totalPreempt.Resources[rname], eq.frameworkHandle.OversellRate()), nil)
			}
		}

		if over, pname, res := info.ParentUsedOverMinWithOverSell(resourceRequest, totalPreempt, eq.frameworkHandle.OversellRate()); over {
			// klog.Warningf("%v elastic quota filter failed request %v max %v used %v with oversellreate %v", QueueUnit.Name, *resourceRequest, info.Max, info.Used, eq.frameworkHandle.OversellRate())
			rname := v1.ResourceName(res)
			request := resourceRequest.ResourceList()[rname]
			pinfo := eq.elasticQuotaTree.ElasticQuotaInfos[pname]
			if res == "jobs" {
				used := pinfo.Count
				originMin := pinfo.Min.ResourceList()["koord-queue/max-jobs"]
				scaledMin := convertMaxToReadable(rname, &originMin, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in parent quota %v: request %v, min %v, used %v, oversellreate %v. Wait for running jobs to complete", res, pname, 1, scaledMin, used, eq.frameworkHandle.OversellRate()), nil)
			} else {
				used := pinfo.NonPreemptibleUsed.ResourceList()[rname]
				originMin := pinfo.Min.ResourceList()[rname]
				scaledMin := convertMaxToReadable(rname, &originMin, eq.frameworkHandle.OversellRate())
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quota(%v) in parent quota %v: request %v, min %v, used %v, oversellreate %v. Wait for running jobs to complete", res, pname, request.String(), scaledMin.String(), used.String(), eq.frameworkHandle.OversellRate()), nil)
			}
		}
	}

	// TODO: when we support partial admission, this need update
	return framework.NewStatus(0, "", request)
}

// obtain the lock outside
func (eq *ElasticQuota) getLatestResAssignment(ctx context.Context, queueUnit *framework.QueueUnitInfo) (*utils.Resource, bool) {
	if res, ok := eq.queueUnitAssignmentSnapshot[queueUnit.Unit.UID]; ok {
		return res, true
	} else {
		return eq.queueUnits[queueUnit.Unit.UID], false
	}
}

func (eq *ElasticQuota) Reserve(ctx context.Context, queueUnit *framework.QueueUnitInfo, ads map[string]framework.Admission) *framework.Status {
	eq.Lock()
	defer eq.Unlock()

	curRes, hasSnap := eq.getLatestResAssignment(ctx, queueUnit)
	if hasSnap {
		return framework.NewStatus(framework.Error, "queue unit already reserved", nil)
	}
	newRes := utils.ConvertFromAdmissionToResource(queueUnit.Unit, ads)
	newClone := newRes.Clone()
	newClone.AddResource(curRes)
	eq.queueUnitAssignmentSnapshot[queueUnit.Unit.UID] = newClone
	eq.updateWithoutLock(ctx, queueUnit, curRes.Zero() && !newClone.Zero(), !curRes.Zero() && newClone.Zero(), newRes)
	return nil
}

func (eq *ElasticQuota) DequeueComplete(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	eq.Lock()
	defer eq.Unlock()

	curRes, _ := eq.getLatestResAssignment(ctx, queueUnit)
	newRes := utils.ConvertFromStatusAdmissionToResource(queueUnit.Unit, queueUnit.Unit.Status.Admissions)
	delete(eq.queueUnitAssignmentSnapshot, queueUnit.Unit.UID)
	eq.queueUnits[queueUnit.Unit.UID] = newRes
	if !curRes.Equal(newRes) {
		// Update
		newClone := newRes.Clone()
		newClone.Sub(curRes)
		eq.updateWithoutLock(ctx, queueUnit, curRes.Zero() && !newRes.Zero(), !curRes.Zero() && newRes.Zero(), newClone)
	}
}

func (eq *ElasticQuota) addAssignedJob(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	curRes, hasSnap := eq.getLatestResAssignment(ctx, queueUnit)
	newRes := utils.ConvertFromStatusAdmissionToResource(queueUnit.Unit, queueUnit.Unit.Status.Admissions)
	eq.queueUnits[queueUnit.Unit.UID] = newRes
	if !hasSnap {
		// UPDATE
		newClone := newRes.Clone()
		newClone.Sub(curRes)
		eq.updateWithoutLock(ctx, queueUnit, curRes.Zero(), false, newClone)
	}
}

func (eq *ElasticQuota) AddAssignedJob(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	eq.Lock()
	defer eq.Unlock()

	eq.addAssignedJob(ctx, queueUnit)
}

func (eq *ElasticQuota) Remove(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	eq.Lock()
	defer eq.Unlock()

	eq.snapshotUpdateLock.Lock()
	defer eq.snapshotUpdateLock.Unlock()
	quota, quotaOk := eq.allQueueUnitsMapping[queueUnit.Unit.UID]
	if quotaOk {
		if snapshot, exist := eq.queueUnitsSnapshot[quota]; exist && snapshot.Has(types.NamespacedName{Namespace: queueUnit.Unit.Namespace, Name: queueUnit.Unit.Name}) {
			newSp := snapshot.Clone()
			newSp.Delete(types.NamespacedName{Namespace: queueUnit.Unit.Namespace, Name: queueUnit.Unit.Name})
			eq.queueUnitsSnapshot[quota] = newSp
		}
		if quIds, exist := eq.reservedQueueUnitsByQuota[quota]; exist {
			quIds.Delete(queueUnit.Unit.UID)
		}
	}

	res, ok := eq.queueUnits[queueUnit.Unit.UID]
	if !ok {
		return
	}
	delete(eq.queueUnits, queueUnit.Unit.UID)

	if !quotaOk {
		return
	}
	info := eq.elasticQuotaTree.GetElasticQuotaInfoByName(quota)
	for k, v := range res.Resources {
		res.Resources[k] = -v
	}
	info.ReserveResource(res, false, true, util.IsJobPreemptible(queueUnit))
	info.Iterate(func(i *elasticquotatree.ElasticQuotaInfo) {
		klog.InfoS("removeJob", "quota", i.FullName, "used", i.Used, "qu", queueUnit.Name, "updated", res)
	})
	ResourceUsageRecord(res, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace)
}

func (eq *ElasticQuota) Resize(ctx context.Context, old, new *framework.QueueUnitInfo) {
	eq.Lock()
	defer eq.Unlock()

	oldQuota, _ := eq.getElasticQuotaInfoWithoutLock(old.Unit)
	newQuota, _ := eq.getElasticQuotaInfoWithoutLock(new.Unit)
	if oldQuota != nil && newQuota != nil && oldQuota.FullName != newQuota.FullName {
		curRes, hasSnap := eq.getLatestResAssignment(ctx, new)
		// remove from old quota
		newResClone := curRes.Clone()
		for k, v := range newResClone.Resources {
			newResClone.Resources[k] = -v
		}
		eq.updateWithoutLock(ctx, old, false, true, newResClone)
		if hasSnap {
			delete(eq.queueUnitAssignmentSnapshot, old.Unit.UID)
		}
		// add to new quota
		newRes := utils.ConvertFromStatusAdmissionToResource(new.Unit, new.Unit.Status.Admissions)
		if newRes != nil && !newRes.Zero() {
			eq.queueUnits[new.Unit.UID] = newRes
		}
		eq.updateWithoutLock(ctx, new, true, false, newRes)
		return
	}

	curRes, hasSnap := eq.getLatestResAssignment(ctx, new)
	if hasSnap {
		newRes := utils.ConvertFromStatusAdmissionToResource(new.Unit, new.Unit.Status.Admissions)
		if newRes != nil && !newRes.Zero() {
			eq.queueUnits[new.Unit.UID] = newRes
		} else {
			delete(eq.queueUnits, new.Unit.UID)
		}
		return
	}

	newRes := utils.ConvertFromStatusAdmissionToResource(new.Unit, new.Unit.Status.Admissions)
	if newRes != nil && !newRes.Zero() {
		eq.queueUnits[new.Unit.UID] = newRes
	} else {
		delete(eq.queueUnits, new.Unit.UID)
	}

	// UPDATE
	newClone := newRes.Clone()
	newClone.Sub(curRes)
	eq.updateWithoutLock(ctx, new, curRes.Zero() && !newRes.Zero(), !curRes.Zero() && newRes.Zero(), newClone)
}

func ResourceUsageRecord(rl *utils.Resource, recorder *prometheus.GaugeVec, namespace string) {
	for name, value := range rl.Resource() {
		recorder.WithLabelValues(namespace, string(name)).Add(float64(value))
	}
}

func (eq *ElasticQuota) updateWithoutLock(ctx context.Context, queueUnit *framework.QueueUnitInfo, add, remove bool, resourceChange *utils.Resource) {
	unit := queueUnit.Unit
	quota, err := eq.getElasticQuotaInfoWithoutLock(queueUnit.Unit)
	if err != nil {
		klog.Errorf("GetElasticQuotaInfo failed, err: %v", err)
		delete(eq.queueUnits, queueUnit.Unit.UID)
		return
	}
	reservedQuotaName := quota.Name
	info := eq.elasticQuotaTree.GetElasticQuotaInfoByName(reservedQuotaName)
	if info == nil {
		if eq.reservedQueueUnitsByQuota[reservedQuotaName] != nil {
			eq.reservedQueueUnitsByQuota[reservedQuotaName].Delete(queueUnit.Unit.UID)
		}
		return
	}
	if remove && eq.reservedQueueUnitsByQuota[reservedQuotaName] != nil {
		eq.reservedQueueUnitsByQuota[reservedQuotaName].Delete(queueUnit.Unit.UID)
	}
	if add {
		if _, ok := eq.reservedQueueUnitsByQuota[reservedQuotaName]; !ok {
			eq.reservedQueueUnitsByQuota[reservedQuotaName] = sets.New[types.UID]()
		}
		eq.reservedQueueUnitsByQuota[reservedQuotaName].Insert(queueUnit.Unit.UID)
	}
	info.ReserveResource(resourceChange, add, remove, util.IsJobPreemptible(queueUnit))
	if !resourceChange.Zero() {
		info.Iterate(func(i *elasticquotatree.ElasticQuotaInfo) {
			klog.InfoS("update", "quota", i.FullName, "used", i.Used, "qu", queueUnit.Name, "updated", resourceChange)
		})
	}
	ResourceUsageRecord(resourceChange, metrics.QuotaUsageByNamespace, unit.Namespace)
}

func (eq *ElasticQuota) Unreserve(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	eq.Lock()
	defer eq.Unlock()

	curRes, hasSnap := eq.getLatestResAssignment(ctx, queueUnit)
	if !hasSnap {
		return
	}
	delete(eq.queueUnitAssignmentSnapshot, queueUnit.Unit.UID)
	newRes, _ := eq.getLatestResAssignment(ctx, queueUnit)

	newClone := newRes.Clone()
	newClone.Sub(curRes)
	eq.updateWithoutLock(ctx, queueUnit, false, newRes.Zero(), newClone)
}

func (eq *ElasticQuota) AddElasticQuotaTree(obj interface{}) {
	eq.Lock()
	defer eq.Unlock()
	klog.Info("add quota")
	t := obj.(*v1beta1.ElasticQuotaTree)
	eq.elasticQuotaTree = elasticquotatree.NewElasticQuotaTree(t)
	eq.queueUnits = make(map[types.UID]*utils.Resource)
	eq.reservedQueueUnitsByQuota = make(map[string]sets.Set[types.UID])

	if eq.elasticQuotaTree.Root != nil {
		qus, err := eq.queueUnitLister.List(labels.Everything())
		if err != nil {
			panic(err)
		}
		for _, qu := range qus {
			if len(qu.Status.Admissions) == 0 {
				continue
			}
			eq.addAssignedJob(context.Background(), framework.NewQueueUnitInfo(qu))
		}
	}
	klog.Infof("%v", eq.elasticQuotaTree.NamespaceToQuota)
	eq.elasticQuotaTree.PrintElasticQuotaTree()

	sendSyncQueueEvent(eq.wq)
}

func (eq *ElasticQuota) GetQuotaUsagePercentageByFullName(fn string) float32 {
	quota := eq.elasticQuotaTree.ElasticQuotaInfos[fn]
	used := quota.Used
	min := quota.Min
	var dominantResourceUsage float32 = 0.0
	for rname, useage := range used.Resources {
		p := float32(useage) / float32(min.Resources[v1.ResourceName(rname)])
		if p > dominantResourceUsage {
			dominantResourceUsage = p
		}
	}
	return dominantResourceUsage
}

func (eq *ElasticQuota) GetExpectedQuotas() (ret sets.Set[string], parent map[string]string) {
	ret = sets.New[string]()
	parent = make(map[string]string)
	for quota, quotaInfo := range eq.elasticQuotaTree.GetChildQuotas() {
		ret.Insert(quota)
		if p := quotaInfo.GetParent(); p != nil {
			parent[quota] = p.Name
		}
	}
	return
}

func (eq *ElasticQuota) syncQueues(handle framework.QueueManageHandle) bool {
	if !features.Enabled(features.ElasticQuotaTreeBuildQueueForQuota) {
		return true
	}
	expectedQuotas, parent := eq.GetExpectedQuotas()
	currentQueues, err := handle.ListQueus()
	var succeed = true
	if err != nil {
		klog.Errorf("list queues err:%v", err)
		klog.Infof("try to create queues:%v", expectedQuotas)
		succeed = succeed && createQueues(expectedQuotas, parent, handle)
	} else {
		currentQuotaSet, mapping := convertQueuesToStringSet(currentQueues)
		needToCreate := expectedQuotas.Difference(currentQuotaSet)
		needToDelete := currentQuotaSet.Difference(expectedQuotas)
		needToSync := currentQuotaSet.Intersection(expectedQuotas)
		klog.Infof("try to create queues:%v", needToCreate)
		klog.Infof("try to delete queues:%v", needToDelete)
		succeed = succeed && createQueues(needToCreate, parent, handle)
		succeed = succeed && deleteQueues(needToDelete, mapping, handle)
		succeed = succeed && syncQueues(needToSync, parent, mapping, handle)
	}
	return succeed
}

func convertQuotaToQueueName(fullname string, ns string) string {
	newFullName := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(fullname, "/", "-"), "_", "-")) + "-"
	// newFullName = newFullName + "-" + ns
	if len(newFullName) > 58 {
		offset := int(math.Abs(float64(58) - float64(len(newFullName))))
		newFullName = newFullName[offset:]
	}
	return newFullName
}

func createQueues(quotas sets.Set[string], parent map[string]string, handle framework.QueueManageHandle) bool {
	hasError := false
	for q := range quotas {
		policy := queuepolicies.Priority
		if os.Getenv("StrictPriority") == "true" {
			policy = queuepolicies.Block
		}
		if os.Getenv("StrictConsistency") == "true" {
			policy = queuepolicies.Priority
		}

		var err error
		queueName := convertQuotaToQueueName(q, "")
		if os.Getenv("TestENV") == "true" {
			err = handle.CreateQueue(queueName, queueName, policy, nil, "", nil,
				map[string]string{utils.AnnotationQuotaFullName: q, utils.AnnotationParentQuotaName: parent[q]}, nil)
		} else {
			err = handle.CreateQueue("", queueName, policy, nil, "", nil,
				map[string]string{utils.AnnotationQuotaFullName: q, utils.AnnotationParentQuotaName: parent[q]}, nil)
		}
		if err != nil {
			klog.ErrorS(err, "failed to create queue")
			hasError = true
		}
	}
	return !hasError
}

func syncQueues(quotas sets.Set[string], parent map[string]string, mapping map[string]*v1alpha1.Queue, handle framework.QueueManageHandle) bool {
	hasError := false
	for q := range quotas {
		queue := mapping[q]
		if queue == nil || queue.Labels["create-by-koordqueue"] != "true" {
			continue
		}
		var policy string
		policy = string(queue.Spec.QueuePolicy)
		if policy == "" {
			policy = queuepolicies.Priority
		}
		if os.Getenv("StrictPriority") == "true" {
			policy = queuepolicies.Block
		}
		if os.Getenv("StrictConsistency") == "true" {
			policy = queuepolicies.Priority
		}
		if policy == "Round" {
			policy = queuepolicies.Priority
		}
		var err error
		if policy != string(queue.Spec.QueuePolicy) ||
			queue.Annotations[utils.AnnotationParentQuotaName] != parent[q] ||
			queue.Annotations[utils.AnnotationQuotaFullName] != q {
			err = handle.UpdateQueue(queue.Name, &policy, nil, "", map[string]string{},
				map[string]string{utils.AnnotationQuotaFullName: q, utils.AnnotationParentQuotaName: parent[q]})
		}
		if err != nil {
			hasError = true
		}
	}
	return !hasError

}

func deleteQueues(quotas sets.Set[string], mapping map[string]*v1alpha1.Queue, handle framework.QueueManageHandle) bool {
	hasError := false
	for q := range quotas {
		queue := mapping[q]
		if queue == nil || queue.Labels["create-by-koordqueue"] != "true" {
			continue
		}
		err := handle.DeleteQueue(queue.Name)
		if err != nil {
			hasError = true
		}
	}
	return !hasError
}

func convertQueuesToStringSet(qs []*v1alpha1.Queue) (sets.Set[string], map[string]*v1alpha1.Queue) {
	res := sets.New[string]()
	mapping := map[string]*v1alpha1.Queue{}
	for _, q := range qs {
		if qn := q.Annotations[utils.AnnotationQuotaFullName]; qn != "" {
			res.Insert(qn)
			mapping[qn] = q
		}
	}
	return res, mapping
}

func (eq *ElasticQuota) DeleteElasticQuotaTree(obj interface{}) {
	eq.Lock()
	defer eq.Unlock()
	eq.elasticQuotaTree = elasticquotatree.NewElasticQuotaTree(nil)
	eq.queueUnits = make(map[types.UID]*utils.Resource)
	eq.reservedQueueUnitsByQuota = make(map[string]sets.Set[types.UID])

	sendSyncQueueEvent(eq.wq)
}

func (eq *ElasticQuota) SetMaxPreemptibleJob(labels map[string]string) {
	if maxPreJob := labels["koord-queue/max-preempible-jobs"]; maxPreJob != "" {
		c, err := strconv.Atoi(maxPreJob)
		if err != nil {
			eq.maxAvailablePreemptibleJob = nil
			return
		}
		eq.maxAvailablePreemptibleJob = ptr.To(int64(c))
		return
	}
	eq.maxAvailablePreemptibleJob = nil
}

func (eq *ElasticQuota) UpdateElasticQuotaTree(oldObj, newObj interface{}) {
	eq.Lock()
	defer eq.Unlock()

	ot := oldObj.(*v1beta1.ElasticQuotaTree)
	t := newObj.(*v1beta1.ElasticQuotaTree)
	eq.SetMaxPreemptibleJob(t.Labels)
	if reflect.DeepEqual(ot.Spec, t.Spec) {
		return
	}

	klog.Info("update elastic quota tree before")
	eq.elasticQuotaTree.PrintElasticQuotaTree()
	eq.elasticQuotaTree = elasticquotatree.NewElasticQuotaTree(t)
	eq.queueUnits = make(map[types.UID]*utils.Resource)
	eq.queueUnitAssignmentSnapshot = make(map[types.UID]*utils.Resource)
	eq.reservedQueueUnitsByQuota = make(map[string]sets.Set[types.UID])
	if eq.elasticQuotaTree.Root != nil {
		qus, err := eq.queueUnitLister.List(labels.Everything())
		if err != nil {
			panic(err)
		}
		for _, qu := range qus {
			if len(qu.Status.Admissions) == 0 {
				continue
			}
			eq.addAssignedJob(context.Background(), framework.NewQueueUnitInfo(qu))
		}
	}
	klog.Info("update elastic quota tree after")
	eq.elasticQuotaTree.PrintElasticQuotaTree()

	sendSyncQueueEvent(eq.wq)
}

func (eq *ElasticQuota) AddEventHandler(queueInformer clientv1alpha1.QueueInformer, handle framework.QueueManageHandle) {
}

func (eq *ElasticQuota) Start(ctx context.Context, handle framework.QueueManageHandle) {
	go func() {
		wq := eq.wq
		for {
			select {
			case <-ctx.Done():
				return
			case <-wq:
				succeed := eq.syncQueues(handle)
				if !succeed {
					go func() { time.Sleep(time.Second); sendSyncQueueEvent(wq) }()
				}
			}
		}
	}()
}

func sendSyncQueueEvent(ch chan int) {
	select {
	case ch <- 1:
	default:
	}
}

func (eq *ElasticQuota) buildEventHandlers(elasticTreeQuotaInformer cache.SharedIndexInformer,
	namespaceInformer cache.SharedIndexInformer,
	queueInformer, queueunitInformer cache.SharedIndexInformer) {
	elasticTreeQuotaInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1beta1.ElasticQuotaTree:
					return t.Namespace == "kube-system"
				case cache.DeletedFinalStateUnknown:
					if eqtree, ok := t.Obj.(*v1beta1.ElasticQuotaTree); ok && eqtree.Namespace == "kube-system" {
						return true
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    eq.AddElasticQuotaTree,
				UpdateFunc: eq.UpdateElasticQuotaTree,
				DeleteFunc: eq.DeleteElasticQuotaTree,
			},
		})

	namespaceInformer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			ns, ok := obj.(*v1.Namespace)
			if !ok {
				return
			}
			eq.namespaceToQuotas[ns.Name] = sets.New(GetAvailableQuotasInNamespace(ns)...)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newNs, ok := newObj.(*v1.Namespace)
			if !ok {
				return
			}
			oldNs, ok := oldObj.(*v1.Namespace)
			if !ok {
				return
			}
			if GetAvailableQuotaString(newNs.Annotations) == GetAvailableQuotaString(oldNs.Annotations) {
				return
			}
			eq.Lock()
			eq.namespaceToQuotas[newNs.Name] = sets.New(GetAvailableQuotasInNamespace(newNs)...)
			eq.Unlock()
		},
		DeleteFunc: func(obj interface{}) {
			switch t := obj.(type) {
			case *v1.Namespace:
				eq.Lock()
				delete(eq.namespaceToQuotas, t.Name)
				eq.Unlock()
			case cache.DeletedFinalStateUnknown:
				ns, ok := t.Obj.(*v1.Namespace)
				if !ok {
					return
				}
				eq.Lock()
				delete(eq.namespaceToQuotas, ns.Name)
				eq.Unlock()
			}
		},
	})

	queueInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			queue, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return
			}
			st := sets.New(GetAvailableQuotasInQueue(queue)...)
			eq.Lock()
			eq.queueToQuotas[queue.Name] = st
			eq.Unlock()
			klog.V(1).InfoS("update available quotas for queue", "queue", queue.Name, "quotas", st.UnsortedList())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newQ, ok := newObj.(*v1alpha1.Queue)
			if !ok {
				return
			}
			oldQ, ok := oldObj.(*v1alpha1.Queue)
			if !ok {
				return
			}
			if GetAvailableQuotaStringInQueue(newQ.Annotations) == GetAvailableQuotaStringInQueue(oldQ.Annotations) {
				return
			}
			st := sets.New(GetAvailableQuotasInQueue(newQ)...)
			eq.Lock()
			eq.queueToQuotas[newQ.Name] = st
			eq.Unlock()
			klog.V(1).InfoS("update available quotas for queue", "queue", newQ.Name, "quotas", st.UnsortedList())
		},
		DeleteFunc: func(obj interface{}) {
			switch t := obj.(type) {
			case *v1alpha1.Queue:
				eq.Lock()
				delete(eq.queueToQuotas, t.Name)
				eq.Unlock()
				klog.V(1).InfoS("clear available quotas for queue", "queue", t.Name)
			case cache.DeletedFinalStateUnknown:
				ns, ok := t.Obj.(*v1alpha1.Queue)
				if !ok {
					return
				}
				eq.Lock()
				delete(eq.queueToQuotas, ns.Name)
				eq.Unlock()
				klog.V(1).InfoS("clear available quotas for queue", "queue", ns.Name)
			}
		},
	})

	addOrUpdate := func(unit *v1alpha1.QueueUnit) {
		quota, err := eq.GetElasticQuotaInfo(unit)
		if err != nil {
			return
		}
		eq.snapshotUpdateLock.Lock()
		defer eq.snapshotUpdateLock.Unlock()
		if eq.allQueueUnitsMapping[unit.UID] == quota.Name {
			return
		}
		if len(eq.queueUnitsSnapshot[quota.Name]) == 0 {
			eq.queueUnitsSnapshot[quota.Name] = sets.New[types.NamespacedName]()
		}
		eq.allQueueUnitsMapping[unit.UID] = quota.Name
		eq.queueUnitsSnapshot[quota.Name].Insert(types.NamespacedName{Namespace: unit.Namespace, Name: unit.Name})
	}
	queueunitInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			unit, ok := obj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			addOrUpdate(unit)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			unit, ok := newObj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			addOrUpdate(unit)
		},
		DeleteFunc: func(obj interface{}) {
			var unit *v1alpha1.QueueUnit
			switch t := obj.(type) {
			case *v1alpha1.QueueUnit:
				unit = t
			case cache.DeletedFinalStateUnknown:
				var ok bool
				unit, ok = t.Obj.(*v1alpha1.QueueUnit)
				if !ok {
					return
				}
			}
			eq.Remove(context.Background(), framework.NewQueueUnitInfo(unit))
		},
	})
}

// New initializes a new plugin and returns it.
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.Infof("start elastic quota")
	args := obj.(*config.ElasticQuotaArgs)
	queueLister := handle.QueueInformerFactory().Scheduling().V1alpha1().Queues().Lister()
	queueInformer := handle.QueueInformerFactory().Scheduling().V1alpha1().Queues().Informer()
	queueUnitLister := handle.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister()
	eq := &ElasticQuota{
		checkHungryQuota:            args.CheckQuotaOversold,
		frameworkHandle:             handle,
		elasticQuotaTree:            elasticquotatree.NewElasticQuotaTree(nil),
		queueUnits:                  map[types.UID]*utils.Resource{},
		reservedQueueUnitsByQuota:   map[string]sets.Set[types.UID]{},
		queueLister:                 queueLister,
		queueInformer:               queueInformer,
		queueunitIndexer:            handle.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Informer().GetIndexer(),
		queueUnitLister:             queueUnitLister,
		queueToQuotas:               make(map[string]sets.Set[string]),
		namespaceToQuotas:           make(map[string]sets.Set[string]),
		wq:                          make(chan int, 1),
		allQueueUnitsMapping:        make(map[types.UID]string),
		queueUnitsSnapshot:          make(map[string]sets.Set[types.NamespacedName]),
		queueUnitAssignmentSnapshot: make(map[types.UID]*utils.Resource),
	}

	kubeConfigPath := handle.KubeConfigPath()
	var (
		restConfig *restclient.Config
		err        error
	)
	if kubeConfigPath == "" {
		restConfig, err = restclient.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		if err != nil {
			return nil, err
		}
	}
	client, err := versioned.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	nsInformer := handle.SharedInformerFactory().Core().V1().Namespaces().Informer()
	schedSharedInformerFactory := schedinformer.NewSharedInformerFactory(client, 0)
	elasticTreeQuotaInformer := schedSharedInformerFactory.Scheduling().V1beta1().ElasticQuotaTrees().Informer()
	eq.buildEventHandlers(elasticTreeQuotaInformer, nsInformer, queueInformer,
		handle.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Informer())

	eq.eqClient = client
	eq.elasticQuotaTreeLister = schedSharedInformerFactory.Scheduling().V1beta1().ElasticQuotaTrees().Lister()

	schedSharedInformerFactory.Start(nil)
	if !cache.WaitForCacheSync(nil, elasticTreeQuotaInformer.HasSynced) {
		return nil, fmt.Errorf("timed out waiting for caches to sync %v", Name)
	}
	go wait.Until(eq.updateUsageMetrics, time.Second*5, wait.NeverStop)
	return eq, nil
}

func (eq *ElasticQuota) updateUsageMetrics() {
	for _, quota := range eq.elasticQuotaTree.ElasticQuotaInfos {
		for k, v := range quota.Used.Resources {
			metrics.QuotaUsageByQuota.WithLabelValues(quota.FullName, string(k)).Set(float64(v))
		}
	}
}
