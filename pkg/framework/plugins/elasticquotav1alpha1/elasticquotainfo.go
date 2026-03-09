package elasticquotav1alpha1

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

type ElasticQuotaInfo struct {
	Quota *v1alpha1.ElasticQuota
	Min   map[v1.ResourceName]int64
	Max   map[v1.ResourceName]int64

	Used         map[v1.ResourceName]int64
	SelfUsed     map[v1.ResourceName]int64
	ChildrenUsed map[v1.ResourceName]int64

	GuaranteedUsed         map[v1.ResourceName]int64
	SelfGuaranteedUsed     map[v1.ResourceName]int64
	ChildrenGuaranteedUsed map[v1.ResourceName]int64

	OverSoldUsed         map[v1.ResourceName]int64
	SelfOverSoldUsed     map[v1.ResourceName]int64
	ChildrenOverSoldUsed map[v1.ResourceName]int64

	Reserved map[types.UID]*framework.QueueUnitInfo
}

func NewElasticQuotaInfo(q *v1alpha1.ElasticQuota) *ElasticQuotaInfo {
	info := &ElasticQuotaInfo{
		Max:                    make(map[v1.ResourceName]int64, len(q.Spec.Max)),
		Min:                    make(map[v1.ResourceName]int64, len(q.Spec.Min)),
		Used:                   make(map[v1.ResourceName]int64),
		SelfUsed:               make(map[v1.ResourceName]int64),
		ChildrenUsed:           make(map[v1.ResourceName]int64),
		GuaranteedUsed:         make(map[v1.ResourceName]int64),
		SelfGuaranteedUsed:     make(map[v1.ResourceName]int64),
		ChildrenGuaranteedUsed: make(map[v1.ResourceName]int64),
		OverSoldUsed:           make(map[v1.ResourceName]int64),
		SelfOverSoldUsed:       make(map[v1.ResourceName]int64),
		ChildrenOverSoldUsed:   make(map[v1.ResourceName]int64),

		Quota:    q,
		Reserved: make(map[types.UID]*framework.QueueUnitInfo),
	}

	for k, v := range q.Spec.Max {
		info.Max[k] = v.Value()
	}
	for k, v := range q.Spec.Min {
		info.Min[k] = v.Value()
	}

	return info
}

func (info *ElasticQuotaInfo) AddQueueUnit(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	if _, exist := info.Reserved[queueUnit.Unit.UID]; exist {
		return
	}

	info.Reserved[queueUnit.Unit.UID] = queueUnit
	res := utils.GetReservedResource(queueUnit.Unit).Resources

	queueUnitQuota := getQuotaName(queueUnit.Unit)
	sameQuota := queueUnitQuota == currentQuota

	utils.UpdateUsage(info.Used, res, 1)
	if sameQuota {
		utils.UpdateUsage(info.SelfUsed, res, 1)
	} else {
		utils.UpdateUsage(info.ChildrenUsed, res, 1)
	}

	isOversold := utils.IsQueueUnitOversold(queueUnit)
	if isOversold {
		utils.UpdateUsage(info.OverSoldUsed, res, 1)
		if sameQuota {
			utils.UpdateUsage(info.SelfOverSoldUsed, res, 1)
		} else {
			utils.UpdateUsage(info.ChildrenOverSoldUsed, res, 1)
		}
	} else {
		utils.UpdateUsage(info.GuaranteedUsed, res, 1)
		if sameQuota {
			utils.UpdateUsage(info.SelfGuaranteedUsed, res, 1)
		} else {
			utils.UpdateUsage(info.ChildrenGuaranteedUsed, res, 1)
		}
	}

	klog.Infof("success AddQueueUnit, currentQuotaName:%v, item QueueName:%v, itemName:%v, "+
		"isOversold: %v, itemRes:%v, max:%v, min:%v, used:%v, selfUsed:%v, childrenUsed:%v, "+
		"guaranteedUsed:%v, selfGuaranteedUsed:%v, childrenGuaranteedUsed:%v, "+
		"overSoldUsed:%v, selfOverSoldUsed:%v, childrenOverSoldUsed:%v", currentQuota, queueUnitQuota, queueUnit.Name,
		isOversold, res, info.Max, info.Min, info.Used, info.SelfUsed, info.ChildrenUsed, info.GuaranteedUsed,
		info.SelfGuaranteedUsed, info.ChildrenGuaranteedUsed, info.OverSoldUsed,
		info.SelfOverSoldUsed, info.ChildrenOverSoldUsed)
}

func (info *ElasticQuotaInfo) DeleteQueueUnit(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	if _, exist := info.Reserved[queueUnit.Unit.UID]; !exist {
		return
	}

	delete(info.Reserved, queueUnit.Unit.UID)
	res := utils.TransResourceList(queueUnit.Unit.Spec.Resource)

	queueUnitQuota := getQuotaName(queueUnit.Unit)
	sameQuota := queueUnitQuota == currentQuota

	utils.UpdateUsage(info.Used, res, -1)
	if sameQuota {
		utils.UpdateUsage(info.SelfUsed, res, -1)
	} else {
		utils.UpdateUsage(info.ChildrenUsed, res, -1)
	}

	isOversold := utils.IsQueueUnitOversold(queueUnit)
	if isOversold {
		utils.UpdateUsage(info.OverSoldUsed, res, -1)
		if sameQuota {
			utils.UpdateUsage(info.SelfOverSoldUsed, res, -1)
		} else {
			utils.UpdateUsage(info.ChildrenOverSoldUsed, res, -1)
		}
	} else {
		utils.UpdateUsage(info.GuaranteedUsed, res, -1)
		if sameQuota {
			utils.UpdateUsage(info.SelfGuaranteedUsed, res, -1)
		} else {
			utils.UpdateUsage(info.ChildrenGuaranteedUsed, res, -1)
		}
	}

	klog.Infof("success DeleteQueueUnit, currentQuotaName:%v, item QueueName:%v, itemName:%v, "+
		"isOversold: %v, itemRes:%v, max:%v, min:%v, used:%v, selfUsed:%v, childrenUsed:%v, "+
		"guaranteedUsed:%v, selfGuaranteedUsed:%v, childrenGuaranteedUsed:%v, "+
		"overSoldUsed:%v, selfOverSoldUsed:%v, childrenOverSoldUsed:%v", currentQuota, queueUnitQuota, queueUnit.Name,
		isOversold, res, info.Max, info.Min, info.Used, info.SelfUsed, info.ChildrenUsed, info.GuaranteedUsed,
		info.SelfGuaranteedUsed, info.ChildrenGuaranteedUsed, info.OverSoldUsed,
		info.SelfOverSoldUsed, info.ChildrenOverSoldUsed)
}

func (info *ElasticQuotaInfo) CheckUsage(currentQuota string,
	queueUnit *framework.QueueUnitInfo, oversellRate float64) error {
	queueUnitRes := utils.TransResourceList(queueUnit.Unit.Spec.Resource)
	queueUnitQuota := getQuotaName(queueUnit.Unit)

	var limit map[v1.ResourceName]int64 = info.Max
	var used map[v1.ResourceName]int64 = info.Used

	if len(limit) == 0 && len(queueUnitRes) != 0 {
		klog.Infof("limit is empty, itemName:%v, queueUnitQuota:%v, currentQuota:%v, "+
			"queueUnitRes:%v, limit:%v", queueUnit.Name, queueUnitQuota,
			currentQuota, queueUnitRes, limit)

		return fmt.Errorf("limited quotaName:%v, quota spec is empty but queue unit res is not empty", currentQuota)
	}

	valid, resKey := checkResource(limit, used, queueUnitRes, oversellRate)
	if !valid {
		klog.Infof("res not enough, itemName:%v, queueUnitQuota:%v, currentQuota:%v, "+
			"oversellRate:%v,  resKey:%v, queueUnitRes:%v, %v, %v", queueUnit.Name,
			queueUnitQuota, currentQuota, oversellRate, resKey, queueUnitRes, limit, used)

		errMsg := fmt.Sprintf("limited quotaName:%v, limited resKey:%v,", currentQuota, resKey)
		err := errors.New(errMsg)
		return err
	}

	return nil
}

func checkResource(limit map[v1.ResourceName]int64, used map[v1.ResourceName]int64,
	req map[v1.ResourceName]int64, oversellRate float64) (bool, v1.ResourceName) {
	for resKey, resValue := range req {
		if _, exist := limit[resKey]; !exist {
			continue
		}

		scaledLimit := float64(limit[resKey]) * oversellRate
		if scaledLimit-float64(used[resKey]) < float64(resValue) {
			return false, resKey
		}
	}

	return true, ""
}
