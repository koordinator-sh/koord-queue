package elasticquotav1alpha1

import (
	"context"
	"fmt"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	fakev1beta1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned/fake"
	schedinformer "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/informers/externalversions"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// New initializes a new plugin and returns it.
func FakeNew(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.Infof("start elastic quota")

	client := fakev1beta1.NewSimpleClientset()
	schedSharedInformerFactory := schedinformer.NewSharedInformerFactory(client, 0)
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

	schedSharedInformerFactory.Start(context.Background().Done())
	results := schedSharedInformerFactory.WaitForCacheSync(context.Background().Done())
	for t, r := range results {
		if !r {
			return nil, fmt.Errorf("failed to wait for caches to sync %v", t.Name())
		}
	}

	handle.QueueInformerFactory().Start(context.Background().Done())
	results = handle.QueueInformerFactory().WaitForCacheSync(context.Background().Done())
	for t, r := range results {
		if !r {
			return nil, fmt.Errorf("failed to wait for caches to sync %v", t.Name())
		}
	}

	return plugin, nil
}
