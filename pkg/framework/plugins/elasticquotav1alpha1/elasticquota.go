package elasticquotav1alpha1

import (
	"context"
	"fmt"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	queueunitversioned "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	clientv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions/scheduling/v1alpha1"
	queuev1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/informers/externalversions"
	eqlister "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/metrics"
)

var _ framework.FilterPlugin = &ElasticQuota{}
var _ framework.ReservePlugin = &ElasticQuota{}
var _ framework.QueueUnitMappingPlugin = &ElasticQuota{}
var _ framework.ApiHandlerPlugin = &ElasticQuota{}

const (
	QuotaNameLabelKey    = "quota.scheduling.koordinator.sh/name"
	AsiQuotaNameLabelKey = "alibabacloud.com/quota-name"
)

type ElasticQuota struct {
	handle          framework.Handle
	eqClient        versioned.Interface
	lister          eqlister.ElasticQuotaLister
	informer        cache.SharedIndexInformer
	queueUnitLister queuev1alpha1.QueueUnitLister
	failover        chan struct{}
	cache           Cache
}

var _ framework.QueueUnitInfoProvider = &ElasticQuota{}

func (eq *ElasticQuota) GetQueueUnitQuotaName(qu *v1alpha1.QueueUnit) ([]string, error) {
	return []string{getQuotaName(qu)}, nil
}

const Name = "ElasticQuotaV2"

// Name returns name of the plugin.
func (eq *ElasticQuota) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	client, err := versioned.NewForConfig(handle.KubeConfig())
	if err != nil {
		return nil, err
	}
	schedSharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	elasticQuotaInformer := schedSharedInformerFactory.Scheduling().V1alpha1().ElasticQuotas()
	plugin := &ElasticQuota{
		handle:          handle,
		eqClient:        client,
		lister:          elasticQuotaInformer.Lister(),
		informer:        elasticQuotaInformer.Informer(),
		queueUnitLister: handle.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister(),
		cache:           newElasticQuotaCache(),
		failover:        make(chan struct{}),
	}
	plugin.LoadQuotaAndQueueUnits()
	plugin.initHandler()

	ctx := context.Background().Done()
	schedSharedInformerFactory.Start(ctx)
	results := schedSharedInformerFactory.WaitForCacheSync(ctx)
	for t, r := range results {
		if !r {
			return nil, fmt.Errorf("failed to wait for caches to sync %v", t.Name())
		}
	}
	return plugin, nil
}

func (eq *ElasticQuota) initHandler() {
	eq.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    eq.Add,
		UpdateFunc: eq.Update,
		DeleteFunc: eq.Delete,
	})

	ctrl := NewQueueController(eq)
	ctrl.Start()
}

// LoadQuotaAndQueueUnits
// when the queue units reserved to the children, if parent is not loaded yet, the usage will not recursively add to
// the parent.
func (eq *ElasticQuota) LoadQuotaAndQueueUnits() {
	quotas, err := eq.eqClient.SchedulingV1alpha1().ElasticQuotas("kube-system").List(context.Background(), metav1.ListOptions{})
	for _, q := range quotas.Items {
		quota := q.DeepCopy()
		eq.tryCreateOrUpdateQueueCr(quota)
		eq.cache.AddOrUpdateQuota(quota)
	}

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

	klog.Infof("LoadQuotaAndQueueUnits done")
	close(eq.failover)
}

func (eq *ElasticQuota) WaitForFailOverDone() {
	<-eq.failover
}

func (eq *ElasticQuota) GetElasticQuotaClient() versioned.Interface {
	return eq.eqClient
}

func (eq *ElasticQuota) GetQueueUnitClient() queueunitversioned.Interface {
	return eq.handle.QueueUnitClient()
}

func (eq *ElasticQuota) GetElasticQuotaInfo4Test(quotaName string) *ElasticQuotaInfo {
	return eq.cache.GetElasticQuotaInfo4Test(quotaName)
}

// TODO: 如果要支持Scale和重建，需要把这里改成支持ads的模式
func (eq *ElasticQuota) Filter(ctx context.Context, queueUnit *framework.QueueUnitInfo, ads map[string]framework.Admission) *framework.Status {
	quotaName := getQuotaName(queueUnit.Unit)

	overSellRate := float64(1)
	if eq.handle != nil {
		overSellRate = eq.handle.OversellRate()
	}

	var err error
	if queueUnit.Unit.Annotations == nil {
		queueUnit.Unit.Annotations = make(map[string]string)
	}
	err = eq.cache.CheckUsage(quotaName, queueUnit, overSellRate)
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error(), nil)
	}

	klog.Infof("success determine queueUnit oversoldType, queueName:%v, itemName:%v",
		quotaName, queueUnit.Name)

	return framework.NewStatus(framework.Success, "", ads)
}

func (eq *ElasticQuota) Reserve(ctx context.Context, queueUnit *framework.QueueUnitInfo, _ map[string]framework.Admission) *framework.Status {
	quotaName := getQuotaName(queueUnit.Unit)
	ResourceUsageRecord(queueUnit.Unit.Spec.Resource, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace, 1)
	err := eq.cache.Reserve(quotaName, queueUnit)
	if err != nil {
		klog.ErrorS(err, "fail to reserve", "queueunit", queueUnit.Name)
		return framework.NewStatus(framework.Error, err.Error(), nil)
	}
	return framework.NewStatus(framework.Success, "", nil)
}

func (eq *ElasticQuota) Resize(ctx context.Context, old, new *framework.QueueUnitInfo) {
	err := eq.cache.Resize(old, new)
	if err != nil {
		klog.ErrorS(err, "fail to resize", "queueunit", new.Name)
	}
}

func (eq *ElasticQuota) Unreserve(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	quotaName := getQuotaName(queueUnit.Unit)
	ResourceUsageRecord(queueUnit.Unit.Spec.Resource, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace, -1)
	err := eq.cache.Unreserve(quotaName, queueUnit)
	if err != nil {
		klog.ErrorS(err, "fail to unreserve", "queueunit", queueUnit.Name)
	}
}

func (eq *ElasticQuota) DequeueComplete(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	quotaName := getQuotaName(queueUnit.Unit)
	ResourceUsageRecord(queueUnit.Unit.Spec.Resource, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace, 1)
	err := eq.cache.Reserve(quotaName, queueUnit)
	if err != nil {
		klog.ErrorS(err, "fail to reserve", "queueunit", queueUnit.Name)
	}
}

func (eq *ElasticQuota) AddAssignedJob(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	quotaName := getQuotaName(queueUnit.Unit)
	ResourceUsageRecord(queueUnit.Unit.Spec.Resource, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace, 1)
	err := eq.cache.Reserve(quotaName, queueUnit)
	if err != nil {
		klog.ErrorS(err, "fail to reserve", "queueunit", queueUnit.Name)
	}
}

func (eq *ElasticQuota) Remove(ctx context.Context, queueUnit *framework.QueueUnitInfo) {
	quotaName := getQuotaName(queueUnit.Unit)
	ResourceUsageRecord(queueUnit.Unit.Spec.Resource, metrics.QuotaUsageByNamespace, queueUnit.Unit.Namespace, -1)
	err := eq.cache.Unreserve(quotaName, queueUnit)
	if err != nil {
		klog.ErrorS(err, "fail to unreserve", "queueunit", queueUnit.Name)
	}
}

func (eq *ElasticQuota) Mapping(qu *v1alpha1.QueueUnit) (string, error) {
	return getQuotaName(qu), nil
}

func (eq *ElasticQuota) AddEventHandler(queueInformer clientv1alpha1.QueueInformer, handle framework.QueueManageHandle) {
}

func (eq *ElasticQuota) Start(ctx context.Context, handle framework.QueueManageHandle) {
}
