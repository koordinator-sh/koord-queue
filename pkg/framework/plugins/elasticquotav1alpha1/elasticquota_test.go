package elasticquotav1alpha1

import (
	"context"
	"testing"
	"time"

	api "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	"github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

func TestPlugin_Filter_Reserve_Unreseve(t *testing.T) {
	plugin := &ElasticQuota{}
	plugin.cache = buildCache()

	quotas := []*v1alpha1.ElasticQuota{
		MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Min(map[string]int64{"cpu": 11}).Quota(),
		MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Min(map[string]int64{"cpu": 20}).Quota(),
	}
	for _, quota := range quotas {
		plugin.cache.AddOrUpdateQuota(quota)
	}

	unit1 := &api.QueueUnit{}
	unit1.UID = "1"
	unit1.Name = "job1"
	unit1.Labels = map[string]string{
		QuotaNameLabelKey: "test1",
	}
	unit1.Spec = api.QueueUnitSpec{}
	unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("5")}
	unit1.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
	}
	queueUnit1 := framework.NewQueueUnitInfo(unit1)

	unit2 := &api.QueueUnit{}
	unit2.UID = "2"
	unit2.Spec = api.QueueUnitSpec{}
	unit2.Name = "job2"
	unit2.Labels = map[string]string{
		QuotaNameLabelKey: "test1",
	}
	unit2.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
	unit2.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
	}
	queueUnit2 := framework.NewQueueUnitInfo(unit2)

	cache := plugin.cache.(*cacheImpl)
	{
		plugin.Reserve(context.Background(), queueUnit1, nil)
		plugin.Reserve(context.Background(), queueUnit2, nil)

		assert.Equal(t, len(cache.reserved), 2)
		assert.Equal(t, cache.reserved["1"], "test1")
		assert.Equal(t, cache.reserved["2"], "test1")

		assert.Equal(t, int64(8), cache.quotas["test1"].Used["cpu"])
		assert.Equal(t, 2, len(cache.quotas["test1"].Reserved))
		assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("2")].Unit.Name, "job2")

		assert.Equal(t, int64(8), cache.quotas["test3"].Used["cpu"])
		assert.Equal(t, 2, len(cache.quotas["test3"].Reserved))
		assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("2")].Unit.Name, "job2")
	}
	{
		{
			unit3 := &api.QueueUnit{}
			unit3.UID = "3"
			unit3.Spec = api.QueueUnitSpec{}
			unit3.Name = "job3"
			unit3.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit3.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeForce,
			}
			unit3.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
			queueUnit3 := framework.NewQueueUnitInfo(unit3)

			status := plugin.Filter(context.Background(), queueUnit3, nil)
			assert.Equal(t, status.Code(), framework.Success)
			assert.Equal(t, utils.QuotaOversoldTypeForce, unit3.Annotations[utils.AnnotationActualQuotaOversoldType])
		}
		{
			unit4 := &api.QueueUnit{}
			unit4.UID = "4"
			unit4.Spec = api.QueueUnitSpec{}
			unit4.Name = "job4"
			unit4.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit4.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeForce,
			}
			unit4.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("300000")}
			queueUnit4 := framework.NewQueueUnitInfo(unit4)

			status := plugin.Filter(context.Background(), queueUnit4, nil)
			assert.Equal(t, status.Code(), framework.Unschedulable)
			assert.Equal(t, "", unit4.Annotations[utils.AnnotationActualQuotaOversoldType])
		}
		{
			unit5 := &api.QueueUnit{}
			unit5.UID = "5"
			unit5.Spec = api.QueueUnitSpec{}
			unit5.Name = "job5"
			unit5.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit5.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
			}
			unit5.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
			queueUnit5 := framework.NewQueueUnitInfo(unit5)

			status := plugin.Filter(context.Background(), queueUnit5, nil)
			assert.Equal(t, status.Code(), framework.Success)
			assert.Equal(t, utils.QuotaOversoldTypeForbidden, unit5.Annotations[utils.AnnotationActualQuotaOversoldType])
		}
		{
			unit6 := &api.QueueUnit{}
			unit6.UID = "6"
			unit6.Spec = api.QueueUnitSpec{}
			unit6.Name = "job6"
			unit6.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit6.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeAccept,
			}
			unit6.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
			queueUnit6 := framework.NewQueueUnitInfo(unit6)

			status := plugin.Filter(context.Background(), queueUnit6, nil)
			assert.Equal(t, status.Code(), framework.Success)
			assert.Equal(t, utils.QuotaOversoldTypeForbidden, unit6.Annotations[utils.AnnotationActualQuotaOversoldType])
		}
		{
			unit7 := &api.QueueUnit{}
			unit7.UID = "7"
			unit7.Spec = api.QueueUnitSpec{}
			unit7.Name = "job7"
			unit7.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit7.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeAccept,
			}
			unit7.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("4")}
			queueUnit7 := framework.NewQueueUnitInfo(unit7)

			status := plugin.Filter(context.Background(), queueUnit7, nil)
			assert.Equal(t, status.Code(), framework.Success)
		}
		{
			unit8 := &api.QueueUnit{}
			unit8.UID = "8"
			unit8.Spec = api.QueueUnitSpec{}
			unit8.Name = "job8"
			unit8.Labels = map[string]string{
				QuotaNameLabelKey: "test1",
			}
			unit8.Annotations = map[string]string{
				utils.AnnotationQuotaOversoldType: utils.QuotaOversoldTypeForce,
			}
			unit8.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("4")}
			queueUnit8 := framework.NewQueueUnitInfo(unit8)

			status := plugin.Filter(context.Background(), queueUnit8, nil)
			assert.Equal(t, status.Code(), framework.Unschedulable)
		}
	}
	{
		plugin.Unreserve(context.Background(), queueUnit2)
		cache.Unreserve("test1", queueUnit2)

		assert.Equal(t, len(cache.reserved), 1)
		assert.Equal(t, cache.reserved["1"], "test1")

		assert.Equal(t, int64(5), cache.quotas["test1"].Used["cpu"])
		assert.Equal(t, 1, len(cache.quotas["test1"].Reserved))
		assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("1")].Unit.Name, "job1")

		assert.Equal(t, int64(5), cache.quotas["test3"].Used["cpu"])
		assert.Equal(t, 1, len(cache.quotas["test3"].Reserved))
		assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("1")].Unit.Name, "job1")
	}
	{
		plugin.Unreserve(context.Background(), queueUnit1)

		assert.Equal(t, len(cache.reserved), 0)

		assert.Equal(t, int64(0), cache.quotas["test1"].Used["cpu"])
		assert.Equal(t, 0, len(cache.quotas["test1"].Reserved))

		assert.Equal(t, int64(0), cache.quotas["test3"].Used["cpu"])
		assert.Equal(t, 0, len(cache.quotas["test3"].Reserved))
	}
}

func NewFrameworkForTesting() (framework.Framework, map[string]framework.Plugin, versioned.Interface) {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset()
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	registry, plugins := NewFakeRegistry()
	fwk, err := runtime.NewFramework(
		registry,
		nil,
		"",
		informers,
		versionedInformers,
		record.NewFakeRecorder(100),
		versionedclient,
		1, nil)

	if err != nil {
		panic(err)
	}
	return fwk, plugins, versionedclient
}

func NewFakeRegistry() (runtime.Registry, map[string]framework.Plugin) {
	plugins := map[string]framework.Plugin{}
	return runtime.Registry{
		Name: pluginproxy(FakeNew, plugins),
	}, plugins
}

func pluginproxy(f func(_ apiruntime.Object, handle framework.Handle) (framework.Plugin, error), plugins map[string]framework.Plugin) func(_ apiruntime.Object, handle framework.Handle) (framework.Plugin, error) {
	return func(obj apiruntime.Object, handle framework.Handle) (framework.Plugin, error) {
		plg, err := f(obj, handle)
		plugins[plg.Name()] = plg
		return plg, err
	}
}
