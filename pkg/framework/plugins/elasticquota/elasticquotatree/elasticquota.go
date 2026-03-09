/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package elasticquotatree

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sync"

	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/client/clientset/versioned"
	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	"github.com/kube-queue/kube-queue/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// all read & write of these maps should be lock outside
type ElasticQuotaTree struct {
	Root              *ElasticQuotaInfo
	ElasticQuotaInfos ElasticQuotaInfos
	NamespaceToQuota  map[string]string
	Name2Quota        ElasticQuotaInfos
}

func NewElasticQuotaTree(tree *v1beta1.ElasticQuotaTree) *ElasticQuotaTree {
	return newElasticQuotaTree(tree)
}

func newElasticQuotaTree(tree *v1beta1.ElasticQuotaTree) *ElasticQuotaTree {
	namespaceToQuota := make(map[string]string)
	elasticQuotaInfos := make(ElasticQuotaInfos)
	name2Quota := make(ElasticQuotaInfos)

	var root *ElasticQuotaInfo
	var eqTree *ElasticQuotaTree
	if tree == nil {
		root = nil
	} else {
		tree.Spec.Root.Name = "root"
		root = buildElasticQuotaInfo(tree.Spec.Root, elasticQuotaInfos, name2Quota, namespaceToQuota, nil, "")
	}
	eqTree = &ElasticQuotaTree{
		Root:              root,
		ElasticQuotaInfos: elasticQuotaInfos,
		NamespaceToQuota:  namespaceToQuota,
		Name2Quota:        name2Quota,
	}

	return eqTree
}

func (tree *ElasticQuotaTree) GetChildQuotas() ElasticQuotaInfos {
	return tree.ElasticQuotaInfos
}

func (tree *ElasticQuotaTree) UpdateElaticQuotaTree(newTree *v1beta1.ElasticQuotaTree) *ElasticQuotaTree {
	return tree.updateElaticQuotaTree(newTree)
}

func (tree *ElasticQuotaTree) updateElaticQuotaTree(newTree *v1beta1.ElasticQuotaTree) *ElasticQuotaTree {
	newElasticQuotaTree := NewElasticQuotaTree(newTree)
	syncStatus(newElasticQuotaTree.Root, tree.ElasticQuotaInfos)
	return newElasticQuotaTree
}

func syncStatus(newInfo *ElasticQuotaInfo, elasticQuotaInfos ElasticQuotaInfos) {
	if newInfo == nil {
		return
	}

	fullName := newInfo.FullName
	klog.Infof("FullName %v", fullName)
	oldInfo := elasticQuotaInfos[fullName]
	if oldInfo == nil {
		return
	}

	newInfo.Used = oldInfo.Used
	newInfo.NonPreemptibleUsed = oldInfo.NonPreemptibleUsed

	for _, child := range newInfo.children {
		syncStatus(child, elasticQuotaInfos)
	}
}

func buildElasticQuotaInfo(elasticQuota v1beta1.ElasticQuotaSpec, elasticQuotaInfos, name2Quota ElasticQuotaInfos, namespaceToQuota map[string]string, parent *ElasticQuotaInfo, prefix string) *ElasticQuotaInfo {
	info := newElasticQuotaInfo(elasticQuota.Name, elasticQuota.Namespaces, elasticQuota.Min, elasticQuota.Max, nil, elasticQuota.Attributes["preempt-policy"] == "PreemptLowerPriority")
	info.parent = parent
	info.FullName = path.Join(prefix, info.Name)
	for _, namespace := range elasticQuota.Namespaces {
		namespaceToQuota[namespace] = info.FullName
	}
	if lendlimit, ok := elasticQuota.Attributes["alibabacloud.com/lendlimit"]; ok {
		rl := v1.ResourceList{}
		err := json.Unmarshal([]byte(lendlimit), &rl)
		if err == nil {
			info.MinSubLendLimit = info.Min.Clone()
			info.MinSubLendLimit.Sub(utils.NewResource(rl))
		}
	}

	if len(elasticQuota.Children) > 0 {
		for _, v := range elasticQuota.Children {
			child := buildElasticQuotaInfo(v, elasticQuotaInfos, name2Quota, namespaceToQuota, info, info.FullName)
			info.children = append(info.children, child)
		}
	}
	info.updateUsedWithLendLimit(nil)
	name2Quota[info.Name] = info
	elasticQuotaInfos[info.FullName] = info

	return info
}

func (tree *ElasticQuotaTree) PrintElasticQuotaTree() {
	prefix := ""
	if tree.Root != nil {
		tree.Root.PrintElasticQuotaInfo(prefix)
	} else {
		return
	}
	if len(tree.Root.children) != 0 {
		printElasticQuotaTree(prefix, tree.Root)
	}
}

func (tree *ElasticQuotaTree) GetElasticQuotaInfoByFullName(name string) *ElasticQuotaInfo {
	return tree.ElasticQuotaInfos[name]
}

func (tree *ElasticQuotaTree) GetElasticQuotaInfoByName(name string) *ElasticQuotaInfo {
	return tree.Name2Quota[name]
}

func (tree *ElasticQuotaTree) GetElasticQuotaInfoByNamespace(namespace string) *ElasticQuotaInfo {
	elasticQuotaName, ok := tree.NamespaceToQuota[namespace]
	if !ok {
		return nil
	}
	info := tree.ElasticQuotaInfos[elasticQuotaName]
	return info
}

func printElasticQuotaTree(prefix string, elasticQuotaInfo *ElasticQuotaInfo) {
	prefix = prefix + "--"
	for _, child := range elasticQuotaInfo.children {
		child.PrintElasticQuotaInfo(prefix)
		if len(child.children) != 0 {
			printElasticQuotaTree(prefix, child)
		}
	}
}

// ElasticQuotaInfo is a wrapper to a ElasticQuota with information.
// Each namespace can only have one ElasticQuota.
type ElasticQuotaInfo struct {
	Name      string
	FullName  string
	Namespace string
	// There is only one default. Support multiple namespace in the future
	Namespaces []string

	parent   *ElasticQuotaInfo
	children []*ElasticQuotaInfo
	// when a job is submitted with "alibabacloud.com/allow-force-preemption" == "true" and
	// the preempt-policy is PreemptLowerPriority in ElasticQuota's Attributes,
	// then this job can preempt all lower priority
	enablePreemptLowerPriority bool

	// the minimum resources that are guaranteed to ensure the basic functionality/performance of the consumers
	Min             *utils.Resource
	MinSubLendLimit *utils.Resource
	// the upper bound of the resource consumption of the consumers.
	Max *utils.Resource
	// Used = Running + Reserved
	Used              *utils.Resource
	UsedWithLendLimit *utils.Resource

	NonPreemptibleUsed *utils.Resource
	// Count = Running + Reserved
	Count            int64
	PreemptibleCount int64

	sync.RWMutex
}

func (eqi *ElasticQuotaInfo) GetParent() *ElasticQuotaInfo {
	return eqi.parent
}

func (eqi *ElasticQuotaInfo) Iterate(f func(i *ElasticQuotaInfo)) {
	current := eqi
	for current != nil {
		f(current)
		current = current.parent
	}
}

func (e *ElasticQuotaInfo) getUsedWithLendLimit() *utils.Resource {
	if e.MinSubLendLimit == nil {
		if e.UsedWithLendLimit != nil {
			return e.UsedWithLendLimit.Clone()
		}
		return e.Used.Clone()
	}
	result := e.UsedWithLendLimit.Clone()
	for k, v := range e.MinSubLendLimit.Resources {
		if uv, exist := result.Resources[k]; exist && v > uv {
			result.Resources[k] = v
		} else if !exist {
			result.Resources[k] = v
		}
	}
	return result
}

// should lock outside
func (e *ElasticQuotaInfo) updateUsedWithLendLimit(childChange *utils.Resource) {
	if len(e.children) > 0 {
		if e.UsedWithLendLimit == nil {
			e.UsedWithLendLimit = utils.NewResource(nil)
		}
		if childChange != nil {
			e.UsedWithLendLimit.AddResource(childChange)
		} else {
			// for parent node
			// usedWithLendLimit = max(sum(children.usedWithLendLimit), min - lendlimit)
			e.UsedWithLendLimit = utils.NewResource(nil)
			for _, child := range e.children {
				e.UsedWithLendLimit.AddResource(child.getUsedWithLendLimit())
			}
		}
	}
	if len(e.children) == 0 {
		// for leaf node
		// for every kind of resource
		// usedWithLendLimit = max(used, max(0, min - lendlimit))
		if e.UsedWithLendLimit == nil {
			e.UsedWithLendLimit = e.Used.Clone()
		} else {
			for k, v := range e.Used.Resources {
				e.UsedWithLendLimit.Resources[k] = v
			}
		}
	}
}

func newElasticQuotaInfo(name string, namespaces []string, min, max, used v1.ResourceList, enablePreemptLowerPriority bool) *ElasticQuotaInfo {
	elasticQuotaInfo := &ElasticQuotaInfo{
		Name:                       name,
		Namespaces:                 namespaces,
		enablePreemptLowerPriority: enablePreemptLowerPriority,
		children:                   []*ElasticQuotaInfo{},
		Min:                        utils.NewResource(min),
		Max:                        utils.NewResource(max),
		Used:                       utils.NewResource(used),
		NonPreemptibleUsed:         utils.NewResource(v1.ResourceList{}),
	}
	return elasticQuotaInfo
}

func (e *ElasticQuotaInfo) EnablePreemptLowerPriority() bool {
	return e.enablePreemptLowerPriority
}

func (e *ElasticQuotaInfo) IsLeaf() bool {
	return len(e.children) == 0
}

func (e *ElasticQuotaInfo) reserveResource(request, usedChange *utils.Resource, addJob, removeJob, preemptible bool) {
	if addJob {
		e.Count++
		if preemptible {
			e.PreemptibleCount++
		}
	}
	if removeJob {
		e.Count--
		if preemptible {
			e.PreemptibleCount--
		}
	}
	oldUsed := e.getUsedWithLendLimit()
	if !preemptible {
		// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, "memory").Add(float64(request.Memory))
		// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, "cpu").Add(float64(request.MilliCPU))
		for name, value := range request.Resources {
			// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, string(name)).Add(float64(value))
			e.NonPreemptibleUsed.SetScalar(name, e.NonPreemptibleUsed.Resources[name]+value)
		}
	}
	if len(e.children) == 0 {
		e.updateUsedWithLendLimit(nil)
	} else {
		e.updateUsedWithLendLimit(usedChange)
	}
	usedChangeForParent := e.getUsedWithLendLimit()
	usedChangeForParent.Sub(oldUsed)
	// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, "memory").Add(float64(request.Memory))
	// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, "cpu").Add(float64(request.MilliCPU))
	for name, value := range request.Resources {
		// metrics.QuotaUsageByQuota.WithLabelValues(e.Name, string(name)).Add(float64(value))
		e.Used.SetScalar(name, e.Used.Resources[name]+value)
	}
	if e.parent != nil {
		e.parent.reserveResource(request, usedChangeForParent, addJob, removeJob, preemptible)
	}
}

func (e *ElasticQuotaInfo) ReserveResource(request *utils.Resource, addJob, removeJob, preemptible bool) {
	e.reserveResource(request, nil, addJob, removeJob, preemptible)
}

func (e *ElasticQuotaInfo) UsedOverMax(podRequest *utils.Resource) bool {
	for rName, rQuant := range e.Max.Resources {
		if podRequest.Resources[rName]+e.Used.Resources[rName] > rQuant {
			return true
		}
	}

	return false
}

// This is only be used for check if the quota is oversold
// Oversold quota cannot be get more resources when there are other quota not oversold.
func (e *ElasticQuotaInfo) UsedLowerThanMinWithOverSell(oversellRate float64, mask sets.Set[string]) (bool, string) {
	if v, ok := e.Min.Resources["kube-queue/max-jobs"]; ok {
		if v < math.MaxInt32 &&
			float64(v)*oversellRate <= float64(e.Count) {
			return false, "jobs"
		}
	}

	for rName, rQuant := range e.Min.Resources {
		if mask.Has(string(rName)) {
			continue
		}
		if rQuant < math.MaxInt32 &&
			float64(e.Used.Resources[rName]) >= float64(rQuant)*oversellRate {
			return false, rName.String()
		}
	}

	return true, ""
}

func (e *ElasticQuotaInfo) UsedOverMinWithOverSell(podRequest, preempt *utils.Resource, oversellRate float64) (bool, string) {
	if v, ok := e.Min.Resources["kube-queue/max-jobs"]; ok {
		if !preempt.Zero() || (v < math.MaxInt32 && float64(v)*oversellRate < float64(e.Count)+1) {
			return true, "jobs"
		}
	}

	for rName, rQuant := range e.Min.Resources {
		if podRequest.Resources[rName] == 0 {
			continue
		}
		if rQuant < math.MaxInt32 && float64(podRequest.Resources[rName]+e.NonPreemptibleUsed.Resources[rName]-preempt.Resources[rName]) > float64(rQuant)*oversellRate {
			return true, rName.String()
		}
	}

	return false, ""
}

func (e *ElasticQuotaInfo) ValidatePreemptibleJobCount(max *int64) (bool, int64) {
	if e.GetParent() == nil {
		return max == nil || e.PreemptibleCount < *max, e.PreemptibleCount
	}

	return e.GetParent().ValidatePreemptibleJobCount(max)
}

func (e *ElasticQuotaInfo) UsedOverMaxWithOverSell(podRequest *utils.Resource, oversellRate float64) (bool, string) {
	if v, ok := e.Max.Resources["kube-queue/max-jobs"]; ok {
		if v < math.MaxInt32 && float64(v)*oversellRate < float64(e.Count)+1 {
			return true, "jobs"
		}
	}

	for rName, rQuant := range e.Max.Resources {
		if podRequest.Resources[rName] == 0 {
			continue
		}
		if rQuant < math.MaxInt32 && float64(podRequest.Resources[rName]+e.Used.Resources[rName]) > float64(rQuant)*oversellRate {
			return true, rName.String()
		}
	}

	return false, ""
}

func (e *ElasticQuotaInfo) ParentUsedOverMinWithOverSell(podRequest, preempt *utils.Resource, oversellRate float64) (bool, string, string) {
	if e.parent != nil {
		over, res := e.parent.UsedOverMinWithOverSell(podRequest, preempt, oversellRate)
		if !over {
			return e.parent.ParentUsedOverMinWithOverSell(podRequest, preempt, oversellRate)
		}
		return true, e.parent.FullName, res
	}
	return false, "", ""
}

func (e *ElasticQuotaInfo) ParentUsedOverMaxWithOverSell(logger klog.Logger, podRequest *utils.Resource, oversellRate float64) (bool, string, string, *utils.Resource) {
	parent := e.parent
	if parent != nil {
		if v, ok := parent.Max.Resources["kube-queue/max-jobs"]; ok {
			if v < math.MaxInt32 && float64(v)*oversellRate < float64(parent.Count)+1 {
				return true, parent.FullName, "jobs", nil
			}
		}

		usedWithLendLimit := e.getUsedWithLendLimit()
		used := e.Used.Clone()
		parentUsed := parent.UsedWithLendLimit.Clone()
		parentUsed.Sub(usedWithLendLimit)
		parentUsed.AddResource(used)
		for rName, rQuant := range parent.Max.Resources {
			if podRequest.Resources[rName] == 0 {
				continue
			}
			if rQuant < math.MaxInt32 && float64(podRequest.Resources[rName]+parentUsed.Resources[rName]) > float64(rQuant)*oversellRate {
				return true, parent.FullName, rName.String(), parentUsed
			}
		}
		logger.V(4).Info(fmt.Sprintf("ParentUsedOverMaxWithOverSell %v %v", parent.FullName, parentUsed))

		return parent.ParentUsedOverMaxWithOverSell(logger, podRequest, oversellRate)
	}
	return false, "", "", nil
}

func (e *ElasticQuotaInfo) UsedOverMin(podRequest *utils.Resource) bool {
	for rName, rQuant := range podRequest.Resources {
		if rQuant+e.Used.Resources[rName] > e.Min.Resources[rName] {
			return true
		}
	}
	return false
}

// PrintElasticQuotaInfo is used for DEBUG.
func (e *ElasticQuotaInfo) PrintElasticQuotaInfo(prefix string) {
	klog.Infof("%vName: %v Namespaces: %v Min: %v Max: %v Used: %v NonPreemptibleUsed: %v \n", prefix, e.Name, e.Namespaces, e.Min, e.Max, e.Used, e.NonPreemptibleUsed)
}

// Speed up search, key is ElasticQuota name
type ElasticQuotaInfos map[string]*ElasticQuotaInfo

func DeepCopy(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]interface{}); ok {
		newMap := make(map[string]interface{})
		for k, v := range valueMap {
			newMap[k] = DeepCopy(v)
		}

		return newMap
	} else if valueSlice, ok := value.([]interface{}); ok {
		newSlice := make([]interface{}, len(valueSlice))
		for k, v := range valueSlice {
			newSlice[k] = DeepCopy(v)
		}

		return newSlice
	}

	return value
}

func BuildClient(kubeConfigPath string) (versioned.Interface, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}
	client, err := versioned.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// PreFilterState computed at PreFilter and used at PostFilter or Reserve.
type PreFilterState struct {
	utils.Resource
}
