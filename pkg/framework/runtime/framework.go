/*
 Copyright 2021 The Kube-Queue Authors.

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

package runtime

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"

	"github.com/gin-gonic/gin"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	"github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/kube-queue/pkg/apis/config"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies"
	"github.com/kube-queue/kube-queue/pkg/utils"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
)

var _ framework.Framework = &frameworkImpl{}

type frameworkImpl struct {
	multiQueueSortPlugin   framework.MultiQueueSortPlugin
	queueUnitMappingPlugin framework.QueueUnitMappingPlugin
	filterPlugins          []framework.FilterPlugin
	apiHandlerPlugins      []framework.ApiHandlerPlugin
	queueSortPlugins       []framework.QueueSortPlugin
	reservePlugins         []framework.ReservePlugin
	queueUnitInfoProvider  framework.QueueUnitInfoProvider
	kubeConfigPath         string
	sharedInformersFactory informers.SharedInformerFactory
	queueUnitClient        versioned.Interface
	queueInformersFactory  externalversions.SharedInformerFactory
	oversellRate           float64
	config                 *rest.Config
	recorder               record.EventRecorderLogger

	// snapshotLock protects queueUnitQuotaMapping and queueUnitsByQuota
	snapshotLock sync.RWMutex
	// queueUnitQuotaMapping maps QueueUnit UID to its quota names, used to maintain queueUnitsByQuota
	queueUnitQuotaMapping map[types.UID][]string
	// queueUnitsByQuota maps quota name -> NamespacedName -> QueueUnit, for O(1) GetQueueUnitInfoByQuota
	queueUnitsByQuota map[string]map[types.NamespacedName]apiv1alpha1.QueueUnit
}

func (f *frameworkImpl) EventRecorder() record.EventRecorderLogger {
	return f.recorder
}

func (f *frameworkImpl) KubeConfig() *rest.Config {
	return f.config
}

func (f *frameworkImpl) MultiQueueSortFunc() framework.MultiQueueLessFunc {
	return f.multiQueueSortPlugin.MultiQueueLess
}

func (f *frameworkImpl) QueueUnitMappingFunc() framework.QueueUnitMappingFunc {
	return f.queueUnitMappingPlugin.Mapping
}

func (f *frameworkImpl) GetQueueUnitQuotaName(unit *v1alpha1.QueueUnit) ([]string, error) {
	return f.queueUnitInfoProvider.GetQueueUnitQuotaName(unit)
}

// QueueUnit info returned by framework will not contain infomation about queue
// Queue information should be filled in controller
func (f *frameworkImpl) GetQueueUnitInfoByQuota(quota string) ([]apiv1alpha1.QueueUnit, error) {
	f.snapshotLock.RLock()
	defer f.snapshotLock.RUnlock()
	m, ok := f.queueUnitsByQuota[quota]
	if !ok {
		return nil, nil
	}
	result := make([]apiv1alpha1.QueueUnit, 0, len(m))
	for _, qu := range m {
		result = append(result, qu)
	}
	return result, nil
}

func (f *frameworkImpl) onQueueUnitAddOrUpdate(obj interface{}) {
	qu, ok := obj.(*v1alpha1.QueueUnit)
	if !ok {
		return
	}
	if f.queueUnitInfoProvider == nil {
		return
	}
	quotaNames, err := f.queueUnitInfoProvider.GetQueueUnitQuotaName(qu)
	if err != nil {
		return
	}
	nn := types.NamespacedName{Namespace: qu.Namespace, Name: qu.Name}

	f.snapshotLock.Lock()
	defer f.snapshotLock.Unlock()
	// Remove old quota mappings if the quota changed
	if oldQuotas, exists := f.queueUnitQuotaMapping[qu.UID]; exists {
		for _, oldQuota := range oldQuotas {
			if m, ok := f.queueUnitsByQuota[oldQuota]; ok {
				delete(m, nn)
			}
		}
	}
	// Add new quota mappings
	f.queueUnitQuotaMapping[qu.UID] = quotaNames
	for _, quotaName := range quotaNames {
		if _, ok := f.queueUnitsByQuota[quotaName]; !ok {
			f.queueUnitsByQuota[quotaName] = make(map[types.NamespacedName]apiv1alpha1.QueueUnit)
		}
		f.queueUnitsByQuota[quotaName][nn] = apiv1alpha1.QueueUnit{
			ObjectMeta: v1.ObjectMeta{
				Name:      qu.Name,
				Namespace: qu.Namespace,
			},
			QuotaName: quotaName,
		}
	}
}

func (f *frameworkImpl) onQueueUnitDelete(obj interface{}) {
	qu, ok := obj.(*v1alpha1.QueueUnit)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		qu, ok = tombstone.Obj.(*v1alpha1.QueueUnit)
		if !ok {
			return
		}
	}
	nn := types.NamespacedName{Namespace: qu.Namespace, Name: qu.Name}

	f.snapshotLock.Lock()
	defer f.snapshotLock.Unlock()
	if oldQuotas, exists := f.queueUnitQuotaMapping[qu.UID]; exists {
		for _, oldQuota := range oldQuotas {
			if m, ok := f.queueUnitsByQuota[oldQuota]; ok {
				delete(m, nn)
			}
		}
		delete(f.queueUnitQuotaMapping, qu.UID)
	}
}

func (f *frameworkImpl) RegisterApiHandler(engine *gin.Engine) {
	for _, pl := range f.apiHandlerPlugins {
		pl.RegisterApiHandler(engine)
	}
}

func (f *frameworkImpl) RunFilterPlugins(ctx context.Context, unit *framework.QueueUnitInfo) *framework.Status {
	reserved := map[string]framework.Admission{}
	for _, ad := range unit.Unit.Status.Admissions {
		reserved[ad.Name] = framework.Admission{Name: ad.Name, Replicas: ad.Replicas}
	}
	request := utils.GetQueueUnitResourceRequirementAds(unit.Unit)
	for _, ps := range request {
		if ps.Replicas > reserved[ps.Name].Replicas {
			request[ps.Name] = framework.Admission{Name: ps.Name, Replicas: ps.Replicas - reserved[ps.Name].Replicas}
		} else if ps.Replicas == reserved[ps.Name].Replicas {
			delete(request, ps.Name)
		}
	}
	result := map[string]framework.Admission{}
	for k := range request {
		result[k] = framework.Admission{Name: k, Replicas: math.MaxInt64}
	}

	for _, pl := range f.filterPlugins {
		pluginStatus := pl.Filter(ctx, unit, request)
		if pluginStatus.Code() != framework.Success {
			return pluginStatus
		}
		for _, admission := range pluginStatus.Admissions() {
			if ad, ok := result[admission.Name]; ok {
				if ad.Replicas > admission.Replicas {
					result[admission.Name] = framework.Admission{Name: admission.Name, Replicas: admission.Replicas}
				}
			}
		}
	}

	for n, ad := range result {
		if ad.Replicas == 0 || ad.Replicas == math.MaxInt64 {
			delete(result, n)
		}
	}
	return framework.NewStatus(framework.Success, "", result)
}

func (f *frameworkImpl) RunScorePlugins(ctx context.Context) (int64, bool) {
	return 0, false
}

func (f *frameworkImpl) RunReservePluginsResize(ctx context.Context, ounit, nunit *framework.QueueUnitInfo) {
	for _, pl := range f.reservePlugins {
		pl.Resize(ctx, ounit, nunit)
	}
}

func (f *frameworkImpl) RunReservePluginsReserve(ctx context.Context, unit *framework.QueueUnitInfo, result map[string]framework.Admission) *framework.Status {
	for _, pl := range f.reservePlugins {
		pluginStatus := pl.Reserve(ctx, unit, result)
		if pluginStatus.Code() != framework.Success {
			return pluginStatus
		}
	}

	return framework.NewStatus(framework.Success, "", result)
}

func (f *frameworkImpl) RunReservePluginsDequeued(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	for _, pl := range f.reservePlugins {
		pl.DequeueComplete(ctx, queueUnit)
	}
}

func (f *frameworkImpl) RunReservePluginsUnreserve(ctx context.Context, unit *framework.QueueUnitInfo) {
	for _, pl := range f.reservePlugins {
		pl.Unreserve(ctx, unit)
	}
}

func (f *frameworkImpl) RunReservePluginsAddAssignedJob(ctx context.Context, unit *framework.QueueUnitInfo) {
	for _, pl := range f.reservePlugins {
		pl.AddAssignedJob(ctx, unit)
	}
}

func (f *frameworkImpl) RunReservePluginsRemoveJob(ctx context.Context, unit *framework.QueueUnitInfo) {
	for _, pl := range f.reservePlugins {
		pl.Remove(ctx, unit)
	}
}

func (f *frameworkImpl) SharedInformerFactory() informers.SharedInformerFactory {
	return f.sharedInformersFactory
}

func (f *frameworkImpl) QueueInformerFactory() externalversions.SharedInformerFactory {
	return f.queueInformersFactory
}

func (f *frameworkImpl) KubeConfigPath() string {
	return f.kubeConfigPath
}

func (f *frameworkImpl) QueueUnitClient() versioned.Interface {
	return f.queueUnitClient
}

func (f *frameworkImpl) OversellRate() float64 {
	return f.oversellRate
}

func (f *frameworkImpl) CreateQueue(name, generateName, policy string, priority *int32, priorityClassName string, labels, annotations, args map[string]string) error {
	var b []byte
	var err error
	if len(args) > 0 {
		b, err = yaml.Marshal(args)
		if err != nil {
			return err
		}
	}
	newQueue := &v1alpha1.Queue{
		ObjectMeta: v1.ObjectMeta{
			Name:         name,
			GenerateName: generateName,
			Namespace:    "kube-queue",
			Labels: map[string]string{
				"create-by-kubequeue": "true",
			},
			Annotations: map[string]string{},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy:       v1alpha1.QueuePolicy(policy),
			Priority:          priority,
			PriorityClassName: priorityClassName,
		},
	}
	if len(b) > 0 {
		newQueue.Annotations[queuepolicies.QueueArgsAnnotationKey] = string(b)
	}
	for k, v := range annotations {
		newQueue.Annotations[k] = v
	}
	for k, v := range labels {
		newQueue.Labels[k] = v
	}

	return framework.RetryTooManyRequests(func() error {
		_, err := f.queueUnitClient.SchedulingV1alpha1().Queues("kube-queue").Create(context.Background(), newQueue, v1.CreateOptions{})
		return err
	})
}

func (f *frameworkImpl) DeleteQueue(name string) error {
	return framework.RetryTooManyRequests(func() error {
		err := f.queueUnitClient.SchedulingV1alpha1().Queues("kube-queue").Delete(context.Background(), name, v1.DeleteOptions{})
		return framework.IgnoreNotFound(err)
	})
}

func (f *frameworkImpl) UpdateQueue(queueName string, policy *string, priority *int32, priorityClassName string, labels, annotations map[string]string, kv ...string) error {
	queue, err := f.queueInformersFactory.Scheduling().V1alpha1().Queues().Lister().Queues("kube-queue").Get(queueName)
	if err != nil {
		return err
	}
	queue = queue.DeepCopy()
	if len(kv)%2 != 0 {
		return errors.New("len of args must be times of 2")
	}
	if priority != nil {
		queue.Spec.Priority = priority
	}
	if priorityClassName != "" {
		queue.Spec.PriorityClassName = priorityClassName
	}
	if policy != nil {
		queue.Spec.QueuePolicy = v1alpha1.QueuePolicy(*policy)
	}
	if queue.Annotations == nil {
		queue.Annotations = make(map[string]string)
	}
	for k, v := range annotations {
		queue.Annotations[k] = v
	}
	if queue.Labels == nil {
		queue.Labels = make(map[string]string)
	}
	for k, v := range labels {
		queue.Labels[k] = v
	}
	args := make(map[string]string, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		args[kv[i]] = args[kv[i+1]]
	}
	b, err := yaml.Marshal(args)
	if err != nil {
		return err
	}
	queue.Annotations[queuepolicies.QueueArgsAnnotationKey] = string(b)
	return framework.RetryTooManyRequests(func() error {
		_, err := f.queueUnitClient.SchedulingV1alpha1().Queues("kube-queue").Update(context.Background(), queue, v1.UpdateOptions{})
		return err
	})
}

func (f *frameworkImpl) UpdateQueueStatus(name string, details map[string][]v1alpha1.QueueItemDetail) error {
	queue, err := f.queueInformersFactory.Scheduling().V1alpha1().Queues().Lister().Queues("kube-queue").Get(name)
	if err != nil {
		return err
	}
	newQueue := queue.DeepCopy()
	newQueue.Status.QueueItemDetails = make(map[string][]v1alpha1.QueueItemDetail, len(details))
	for k, v := range details {
		newQueue.Status.QueueItemDetails[k] = utils.SliceCopy(v)
	}
	if reflect.DeepEqual(newQueue.Status, queue.Status) {
		return nil
	}
	return framework.RetryTooManyRequests(func() error {
		_, err := f.queueUnitClient.SchedulingV1alpha1().Queues("kube-queue").UpdateStatus(context.Background(), newQueue, v1.UpdateOptions{})
		return err
	})
}

func (f *frameworkImpl) ListQueus() ([]*v1alpha1.Queue, error) {
	return f.queueInformersFactory.Scheduling().V1alpha1().Queues().Lister().Queues("kube-queue").List(labels.SelectorFromSet(labels.Set{}))
}

func (f *frameworkImpl) StartQueueUnitMappingPlugin(ctx context.Context) {
	f.queueUnitMappingPlugin.Start(ctx, f)
}

func NewFramework(r Registry, config *rest.Config, kubeConfigPath string,
	informersFactory informers.SharedInformerFactory,
	queueFactory externalversions.SharedInformerFactory,
	recorder record.EventRecorderLogger,
	queueUnitClient versioned.Interface,
	oversellRate float64,
	pluginconfig *config.KubeQueueConfiguration,
) (framework.Framework, error) {
	apiHandlerPlugins := make([]framework.ApiHandlerPlugin, 0)
	filterPlugins := make([]framework.FilterPlugin, 0)
	queueSortPlugins := make([]framework.QueueSortPlugin, 0)
	reservePlugins := make([]framework.ReservePlugin, 0)
	var multiQueueSortPlugin framework.MultiQueueSortPlugin
	var queueUnitMappingPlugin framework.QueueUnitMappingPlugin

	f := &frameworkImpl{
		kubeConfigPath:         kubeConfigPath,
		sharedInformersFactory: informersFactory,
		queueUnitClient:        queueUnitClient,
		queueInformersFactory:  queueFactory,
		oversellRate:           oversellRate,
		config:                 config,
		recorder:               recorder,
		queueUnitQuotaMapping:  make(map[types.UID][]string),
		queueUnitsByQuota:      make(map[string]map[types.NamespacedName]apiv1alpha1.QueueUnit),
	}

	klog.V(1).Infof("Starting Koord Queue with plugins: %v, plugin configs: %v", pluginconfig.Plugins, pluginconfig.PluginConfigs)
	enabledPlugins := make(map[string]struct{}, 0)
	for _, plg := range pluginconfig.Plugins {
		enabledPlugins[plg.Name] = struct{}{}
	}
	for name, factory := range r {
		if _, ok := enabledPlugins[name]; !ok {
			continue
		}
		var args runtime.Object
		if pluginconfig != nil {
			if c, ok := pluginconfig.PluginConfigs[name]; ok {
				args = c
			}
		}
		p, err := factory(args, f)
		if err != nil {
			return nil, err
		}
		if i, ok := p.(framework.QueueSortPlugin); ok {
			queueSortPlugins = append(queueSortPlugins, i)
		}
		if i, ok := p.(framework.MultiQueueSortPlugin); ok {
			multiQueueSortPlugin = i
		}
		if i, ok := p.(framework.QueueUnitMappingPlugin); ok {
			queueUnitMappingPlugin = i
		}
		if i, ok := p.(framework.FilterPlugin); ok {
			filterPlugins = append(filterPlugins, i)
		}
		if i, ok := p.(framework.ReservePlugin); ok {
			reservePlugins = append(reservePlugins, i)
		}
		if i, ok := p.(framework.ApiHandlerPlugin); ok {
			apiHandlerPlugins = append(apiHandlerPlugins, i)
		}
		if i, ok := p.(framework.QueueUnitInfoProvider); ok {
			f.queueUnitInfoProvider = i
		}
	}

	f.queueSortPlugins = queueSortPlugins
	f.reservePlugins = reservePlugins
	f.multiQueueSortPlugin = multiQueueSortPlugin
	f.queueUnitMappingPlugin = queueUnitMappingPlugin
	f.filterPlugins = filterPlugins
	f.apiHandlerPlugins = apiHandlerPlugins

	if r != nil {
		f.queueUnitMappingPlugin.AddEventHandler(queueFactory.Scheduling().V1alpha1().Queues(), f)
		queueFactory.Scheduling().V1alpha1().QueueUnits().Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    f.onQueueUnitAddOrUpdate,
				UpdateFunc: func(_, newObj interface{}) { f.onQueueUnitAddOrUpdate(newObj) },
				DeleteFunc: f.onQueueUnitDelete,
			},
		)
	}
	return f, nil
}
