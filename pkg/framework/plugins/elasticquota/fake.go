package elasticquotatree

import (
	"fmt"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	fakev1beta1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned/fake"
	schedinformer "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota/elasticquotatree"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// New initializes a new plugin and returns it.
func FakeNew(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.Infof("start elastic quota")
	args := &config.ElasticQuotaArgs{
		CheckQuotaOversold: false,
	}
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
		queueUnitLister:             queueUnitLister,
		queueToQuotas:               make(map[string]sets.Set[string]),
		namespaceToQuotas:           make(map[string]sets.Set[string]),
		wq:                          make(chan int, 1),
		allQueueUnitsMapping:        make(map[types.UID]string),
		queueUnitsSnapshot:          make(map[string]sets.Set[types.NamespacedName]),
		queueUnitAssignmentSnapshot: make(map[types.UID]*utils.Resource),
	}

	client := fakev1beta1.NewSimpleClientset()
	eq.eqClient = client
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
