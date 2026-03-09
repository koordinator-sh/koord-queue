package elasticquotav1alpha1

import (
	"fmt"
	"testing"
	"time"

	api "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/utils"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type ElasticQuotaWrapper struct {
	elasticquota *v1alpha1.ElasticQuota
}

func MakeElasticQuota(name string, namespace string) *ElasticQuotaWrapper {
	return &ElasticQuotaWrapper{elasticquota: &v1alpha1.ElasticQuota{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, CreationTimestamp: metav1.Now(), Labels: map[string]string{}}}}
}

func (q *ElasticQuotaWrapper) Max(res map[string]int64) *ElasticQuotaWrapper {
	q.elasticquota.Spec.Max = make(v1.ResourceList)
	for k, v := range res {
		q.elasticquota.Spec.Max[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
	}
	return q
}

func (q *ElasticQuotaWrapper) Min(res map[string]int64) *ElasticQuotaWrapper {
	q.elasticquota.Spec.Min = make(v1.ResourceList)
	for k, v := range res {
		q.elasticquota.Spec.Min[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
	}
	return q
}

func (q *ElasticQuotaWrapper) Parent(p string) *ElasticQuotaWrapper {
	q.elasticquota.Labels["quota.scheduling.koordinator.sh/parent"] = p
	return q
}

func (q *ElasticQuotaWrapper) SetIsParent(v bool) *ElasticQuotaWrapper {
	q.elasticquota.Labels["quota.scheduling.koordinator.sh/is-parent"] = fmt.Sprintf("%v", v)
	return q
}

func (q *ElasticQuotaWrapper) Quota() *v1alpha1.ElasticQuota {
	return q.elasticquota
}

func Test_cacheImpl_CheckUsage(t *testing.T) {
	type reservedRes struct {
		uid      types.UID
		quotaKey string
		res      v1.ResourceList
	}

	tests := []struct {
		name                     string
		quotaKey                 string
		request                  v1.ResourceList
		oversellrate             float64
		quotas                   []*v1alpha1.ElasticQuota
		reservedRes              []reservedRes
		reserveForbiddenOversold bool
		oversold                 bool
		hasError                 bool
	}{
		{
			name:         "quota not found",
			quotaKey:     "test",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test2").Max(map[string]int64{"cpu": 100}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 100}).Quota(),
				MakeElasticQuota("test3", "default").Parent("test1").Max(map[string]int64{"cpu": 100}).Quota(),
			},
			hasError: true,
		},
		{
			name:         "cycle reference",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test2").Max(map[string]int64{"cpu": 100}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 100}).Quota(),
				MakeElasticQuota("test3", "default").Parent("test1").Max(map[string]int64{"cpu": 100}).Quota(),
			},
			hasError: true,
		},
		{
			name:         "max exceed",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test2").Max(map[string]int64{"cpu": 9}).Quota(),
				MakeElasticQuota("test2", "default").Max(map[string]int64{"cpu": 100}).Quota(),
			},
			oversold: true,
			hasError: true,
		},
		{
			name:         "parent min exceed",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test2").Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test2", "default").Min(map[string]int64{"cpu": 9}).Quota(),
			},
			oversold: false,
			hasError: false,
		},
		{
			name:         "min exceed",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(12, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test2").Min(map[string]int64{"cpu": 11}).Quota(),
				MakeElasticQuota("test2", "default").Min(map[string]int64{"cpu": 13}).Quota(),
			},
			oversold: false,
			hasError: true,
		},
		{
			name:         "parent max exceed",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 20}).Quota(),
				MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Quota(),
			},
			reservedRes: []reservedRes{{
				uid:      "default/a",
				quotaKey: "test2",
				res:      v1.ResourceList{"cpu": resource.MustParse("15")},
			}},
			reserveForbiddenOversold: true,
			oversold:                 true,
			hasError:                 true,
		},
		{
			name:         "parent max exceed, reserve is forceOversold, job is forceOversold",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 20}).Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Min(map[string]int64{"cpu": 20}).Quota(),
			},
			reservedRes: []reservedRes{{
				uid:      "default/a",
				quotaKey: "test2",
				res:      v1.ResourceList{"cpu": resource.MustParse("20")},
			}},
			reserveForbiddenOversold: false,
			oversold:                 true,
			hasError:                 true,
		},
		{
			name:         "reserve is forceOversold, but job is forbiddenOversold, no need check parent max",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 20}).Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Min(map[string]int64{"cpu": 20}).Quota(),
			},
			reservedRes: []reservedRes{{
				uid:      "default/a",
				quotaKey: "test2",
				res:      v1.ResourceList{"cpu": resource.MustParse("20")},
			}},
			reserveForbiddenOversold: false,
			oversold:                 false,
			hasError:                 false,
		},
		{
			name:         "parent min exceed",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(11, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test3").Min(map[string]int64{"cpu": 11}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Min(map[string]int64{"cpu": 10}).Quota(),
				MakeElasticQuota("test3", "default").Min(map[string]int64{"cpu": 20}).Quota(),
			},
			reservedRes: []reservedRes{{
				uid:      "default/a",
				quotaKey: "test2",
				res:      v1.ResourceList{"cpu": resource.MustParse("10")},
			}},
			reserveForbiddenOversold: true,
			oversold:                 false,
			hasError:                 false,
		},
		{
			name:         "ok",
			quotaKey:     "test1",
			request:      v1.ResourceList{"cpu": *resource.NewQuantity(10, resource.DecimalSI)},
			oversellrate: 1,
			quotas: []*v1alpha1.ElasticQuota{
				MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Quota(),
				MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 20}).Quota(),
				MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Quota(),
			},
			reservedRes: []reservedRes{{
				uid:      "default/a",
				quotaKey: "test2",
				res:      v1.ResourceList{"cpu": resource.MustParse("1")},
			}},
			oversold: true,
			hasError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := buildCache()
			for _, quota := range tt.quotas {
				cache.AddOrUpdateQuota(quota)
			}

			time.Sleep(time.Millisecond * 10)
			for _, r := range tt.reservedRes {
				unit := &api.QueueUnit{}
				unit.UID = r.uid
				unit.Labels = map[string]string{
					QuotaNameLabelKey: r.quotaKey,
				}
				unit.Spec = api.QueueUnitSpec{}
				unit.Spec.Resource = r.res
				queueUnit := framework.NewQueueUnitInfo(unit)
				queueUnit.Unit.Annotations = map[string]string{}
				if tt.reserveForbiddenOversold {
					queueUnit.Unit.Annotations[utils.AnnotationActualQuotaOversoldType] = utils.QuotaOversoldTypeForbidden
				} else {
					queueUnit.Unit.Annotations[utils.AnnotationActualQuotaOversoldType] = utils.QuotaOversoldTypeForce
				}
				cache.Reserve(r.quotaKey, queueUnit)
			}

			unit := &api.QueueUnit{}
			unit.Labels = map[string]string{
				QuotaNameLabelKey: tt.quotaKey,
			}
			unit.Spec = api.QueueUnitSpec{}
			unit.Spec.Resource = tt.request
			unit.Annotations = map[string]string{}
			queueUnit := framework.NewQueueUnitInfo(unit)

			got := cache.CheckUsage(tt.quotaKey, queueUnit, tt.oversellrate)
			assert.Equal(t, got != nil, tt.hasError)
		})
	}
}

func TestThreeLevelQuotaE2E(t *testing.T) {
	{
		cache := buildCache()
		quota31 := MakeElasticQuota("test31", "default").Parent("test2").Min(map[string]int64{"cpu": 2}).Quota()
		quota32 := MakeElasticQuota("test32", "default").Parent("test2").Min(map[string]int64{"cpu": 3}).Quota()
		quota2 := MakeElasticQuota("test2", "default").Parent("test1").Min(map[string]int64{"cpu": 5}).Quota()
		quota1 := MakeElasticQuota("test1", "default").Min(map[string]int64{"cpu": 10}).Quota()

		cache.AddOrUpdateQuota(quota1)
		cache.AddOrUpdateQuota(quota2)
		cache.AddOrUpdateQuota(quota31)
		cache.AddOrUpdateQuota(quota32)

		unit31 := &api.QueueUnit{}
		unit31.UID = "31"
		unit31.Name = "job31"
		unit31.Spec = api.QueueUnitSpec{}
		unit31.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("2")}
		unit31.Labels = map[string]string{
			QuotaNameLabelKey: "test31",
		}
		unit31.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit31 := framework.NewQueueUnitInfo(unit31)
		cache.Reserve("test31", queueUnit31)

		unit32 := &api.QueueUnit{}
		unit32.UID = "32"
		unit32.Name = "job32"
		unit32.Spec = api.QueueUnitSpec{}
		unit32.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
		unit32.Labels = map[string]string{
			QuotaNameLabelKey: "test32",
		}
		unit32.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit32 := framework.NewQueueUnitInfo(unit32)
		cache.Reserve("test32", queueUnit32)

		unit2 := &api.QueueUnit{}
		unit2.UID = "2"
		unit2.Name = "job2"
		unit2.Spec = api.QueueUnitSpec{}
		unit2.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("5")}
		unit2.Labels = map[string]string{
			QuotaNameLabelKey: "test2",
		}
		unit2.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit2 := framework.NewQueueUnitInfo(unit2)
		err := cache.CheckUsage("test2", queueUnit2, 1)
		assert.True(t, err == nil)
		cache.Reserve("test2", queueUnit2)

		unit1 := &api.QueueUnit{}
		unit1.UID = "1"
		unit1.Name = "job1"
		unit1.Spec = api.QueueUnitSpec{}
		unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("10")}
		unit1.Labels = map[string]string{
			QuotaNameLabelKey: "test1",
		}
		unit1.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit1 := framework.NewQueueUnitInfo(unit1)
		err = cache.CheckUsage("test1", queueUnit1, 1)
		assert.True(t, err == nil)
		cache.Reserve("test1", queueUnit1)

		assert.Equal(t, int64(2), cache.quotas["test31"].Used["cpu"])
		assert.Equal(t, int64(2), cache.quotas["test31"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(2), cache.quotas["test31"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(2), cache.quotas["test31"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(3), cache.quotas["test32"].Used["cpu"])
		assert.Equal(t, int64(3), cache.quotas["test32"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(3), cache.quotas["test32"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(3), cache.quotas["test32"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(10), cache.quotas["test2"].Used["cpu"])
		assert.Equal(t, int64(5), cache.quotas["test2"].SelfUsed["cpu"])
		assert.Equal(t, int64(5), cache.quotas["test2"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(10), cache.quotas["test2"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(5), cache.quotas["test2"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(5), cache.quotas["test2"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(20), cache.quotas["test1"].Used["cpu"])
		assert.Equal(t, int64(10), cache.quotas["test1"].SelfUsed["cpu"])
		assert.Equal(t, int64(10), cache.quotas["test1"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(20), cache.quotas["test1"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(10), cache.quotas["test1"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(10), cache.quotas["test1"].ChildrenGuaranteedUsed["cpu"])

		cache.Unreserve("test31", queueUnit31)
		cache.Unreserve("test32", queueUnit32)
		cache.Unreserve("test2", queueUnit2)
		cache.Unreserve("test1", queueUnit1)

		assert.Equal(t, int64(0), cache.quotas["test31"].Used["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test31"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(0), cache.quotas["test32"].Used["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test32"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(0), cache.quotas["test2"].Used["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test2"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test2"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test2"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test2"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test2"].ChildrenGuaranteedUsed["cpu"])

		assert.Equal(t, int64(0), cache.quotas["test1"].Used["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test1"].SelfUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test1"].ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test1"].GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test1"].SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), cache.quotas["test1"].ChildrenGuaranteedUsed["cpu"])
	}
	{
		cache := buildCache()
		quota31 := MakeElasticQuota("test31", "default").Parent("test2").Min(map[string]int64{"cpu": 2}).Quota()
		quota32 := MakeElasticQuota("test32", "default").Parent("test2").Min(map[string]int64{"cpu": 3}).Quota()
		quota2 := MakeElasticQuota("test2", "default").Parent("test1").Min(map[string]int64{"cpu": 5}).Quota()
		quota1 := MakeElasticQuota("test1", "default").Min(map[string]int64{"cpu": 10}).Quota()

		cache.AddOrUpdateQuota(quota1)
		cache.AddOrUpdateQuota(quota2)
		cache.AddOrUpdateQuota(quota31)
		cache.AddOrUpdateQuota(quota32)

		unit2 := &api.QueueUnit{}
		unit2.UID = "2"
		unit2.Name = "job2"
		unit2.Spec = api.QueueUnitSpec{}
		unit2.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("5")}
		unit2.Labels = map[string]string{
			QuotaNameLabelKey: "test2",
		}
		unit2.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit2 := framework.NewQueueUnitInfo(unit2)
		err := cache.CheckUsage("test2", queueUnit2, 1)
		assert.True(t, err == nil)
		cache.Reserve("test2", queueUnit2)

		unit31 := &api.QueueUnit{}
		unit31.UID = "31"
		unit31.Name = "job31"
		unit31.Spec = api.QueueUnitSpec{}
		unit31.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("2")}
		unit31.Labels = map[string]string{
			QuotaNameLabelKey: "test31",
		}
		unit31.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit31 := framework.NewQueueUnitInfo(unit31)
		err = cache.CheckUsage("test31", queueUnit31, 1)
		assert.True(t, err == nil)

		unit32 := &api.QueueUnit{}
		unit32.UID = "32"
		unit32.Name = "job32"
		unit32.Spec = api.QueueUnitSpec{}
		unit32.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
		unit32.Labels = map[string]string{
			QuotaNameLabelKey: "test32",
		}
		unit32.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit32 := framework.NewQueueUnitInfo(unit32)
		err = cache.CheckUsage("test32", queueUnit32, 1)
		assert.True(t, err == nil)

		unit1 := &api.QueueUnit{}
		unit1.UID = "1"
		unit1.Name = "job1"
		unit1.Spec = api.QueueUnitSpec{}
		unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("10")}
		unit1.Labels = map[string]string{
			QuotaNameLabelKey: "test1",
		}
		unit1.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit1 := framework.NewQueueUnitInfo(unit1)
		err = cache.CheckUsage("test1", queueUnit1, 1)
		assert.True(t, err == nil)
	}
}

func Test_cacheImpl_Reserve_Unreserve(t *testing.T) {
	{
		cache := buildCache()
		quotas := []*v1alpha1.ElasticQuota{
			MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Quota(),
			MakeElasticQuota("test3", "default").Max(map[string]int64{"cpu": 20}).Quota(),
		}
		for _, quota := range quotas {
			cache.AddOrUpdateQuota(quota)
		}

		unit1 := &api.QueueUnit{}
		unit1.UID = "1"
		unit1.Name = "job1"
		unit1.Spec = api.QueueUnitSpec{}
		unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("5")}
		unit1.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit1 := framework.NewQueueUnitInfo(unit1)

		unit2 := &api.QueueUnit{}
		unit2.UID = "2"
		unit2.Spec = api.QueueUnitSpec{}
		unit2.Name = "job2"
		unit2.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("3")}
		unit2.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}
		queueUnit2 := framework.NewQueueUnitInfo(unit2)

		{
			err := cache.Reserve("test1", queueUnit1)
			assert.True(t, err == nil)
			cache.Reserve("test1", queueUnit1)
			assert.True(t, err == nil)

			err = cache.Reserve("test1", queueUnit2)
			assert.True(t, err == nil)
			err = cache.Reserve("test1", queueUnit2)
			assert.True(t, err == nil)

			assert.Equal(t, len(cache.GetReserved()), 2)
			assert.Equal(t, cache.GetReserved()["1"], "test1")
			assert.Equal(t, cache.GetReserved()["2"], "test1")

			assert.Equal(t, int64(8), cache.quotas["test1"].Used["cpu"])
			assert.Equal(t, int64(5), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 2, len(cache.quotas["test1"].Reserved))
			assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("1")].Unit.Name, "job1")
			assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("2")].Unit.Name, "job2")

			assert.Equal(t, int64(8), cache.quotas["test3"].Used["cpu"])
			assert.Equal(t, int64(5), cache.quotas["test3"].GuaranteedUsed["cpu"])
			assert.Equal(t, 2, len(cache.quotas["test3"].Reserved))
			assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("1")].Unit.Name, "job1")
			assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("2")].Unit.Name, "job2")
		}
		{
			err := cache.Unreserve("test1", queueUnit2)
			assert.True(t, err == nil)
			err = cache.Unreserve("test1", queueUnit2)
			assert.True(t, err == nil)

			assert.Equal(t, len(cache.GetReserved()), 1)
			assert.Equal(t, cache.GetReserved()["1"], "test1")

			assert.Equal(t, int64(5), cache.quotas["test1"].Used["cpu"])
			assert.Equal(t, int64(5), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 1, len(cache.quotas["test1"].Reserved))
			assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("1")].Unit.Name, "job1")

			assert.Equal(t, int64(5), cache.quotas["test3"].Used["cpu"])
			assert.Equal(t, int64(5), cache.quotas["test3"].GuaranteedUsed["cpu"])
			assert.Equal(t, 1, len(cache.quotas["test3"].Reserved))
			assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("1")].Unit.Name, "job1")
		}
		{
			err := cache.Unreserve("test1", queueUnit1)
			assert.True(t, err == nil)
			err = cache.Unreserve("test1", queueUnit1)
			assert.True(t, err == nil)

			assert.Equal(t, len(cache.GetReserved()), 0)

			assert.Equal(t, int64(0), cache.quotas["test1"].Used["cpu"])
			assert.Equal(t, int64(0), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 0, len(cache.quotas["test1"].Reserved))

			assert.Equal(t, int64(0), cache.quotas["test3"].Used["cpu"])
			assert.Equal(t, int64(0), cache.quotas["test3"].GuaranteedUsed["cpu"])
			assert.Equal(t, 0, len(cache.quotas["test3"].Reserved))
		}
	}
	{
		cache := buildCache()
		quotas := []*v1alpha1.ElasticQuota{
			MakeElasticQuota("test1", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Quota(),
		}
		for _, quota := range quotas {
			cache.AddOrUpdateQuota(quota)
		}

		unit1 := &api.QueueUnit{}
		unit1.UID = "1"
		unit1.Name = "job1"
		unit1.Spec = api.QueueUnitSpec{}
		unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("5")}
		queueUnit1 := framework.NewQueueUnitInfo(unit1)

		{
			err := cache.Reserve("test2", queueUnit1)
			assert.True(t, err != nil)

			cache.reserved["1"] = "test3"
			err = cache.Reserve("test1", queueUnit1)
			assert.True(t, err != nil)

			delete(cache.reserved, "1")
			err = cache.Reserve("test1", queueUnit1)
			assert.True(t, err == nil)
		}
	}
	{
		cache := buildCache()
		quotas := []*v1alpha1.ElasticQuota{
			MakeElasticQuota("test1", "default").Parent("test2").Max(map[string]int64{"cpu": 11}).Quota(),
			MakeElasticQuota("test2", "default").Parent("test3").Max(map[string]int64{"cpu": 11}).Quota(),
			MakeElasticQuota("test3", "default").Parent("test1").Max(map[string]int64{"cpu": 11}).Quota(),
		}
		for _, quota := range quotas {
			cache.AddOrUpdateQuota(quota)
		}

		unit1 := &api.QueueUnit{}
		unit1.UID = "1"
		unit1.Name = "job1"
		unit1.Spec = api.QueueUnitSpec{}
		unit1.Spec.Resource = v1.ResourceList{"cpu": resource.MustParse("8")}
		queueUnit1 := framework.NewQueueUnitInfo(unit1)
		unit1.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}

		{
			err := cache.Reserve("test1", queueUnit1)
			assert.True(t, err == nil)

			assert.Equal(t, len(cache.GetReserved()), 1)
			assert.Equal(t, cache.GetReserved()["1"], "test1")

			assert.Equal(t, int64(8), cache.quotas["test1"].Used["cpu"])
			assert.Equal(t, int64(8), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 1, len(cache.quotas["test1"].Reserved))
			assert.Equal(t, cache.quotas["test1"].Reserved[types.UID("1")].Unit.Name, "job1")

			assert.Equal(t, int64(8), cache.quotas["test2"].Used["cpu"])
			assert.Equal(t, int64(8), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 1, len(cache.quotas["test2"].Reserved))
			assert.Equal(t, cache.quotas["test2"].Reserved[types.UID("1")].Unit.Name, "job1")

			assert.Equal(t, int64(8), cache.quotas["test3"].Used["cpu"])
			assert.Equal(t, int64(8), cache.quotas["test1"].GuaranteedUsed["cpu"])
			assert.Equal(t, 1, len(cache.quotas["test3"].Reserved))
			assert.Equal(t, cache.quotas["test3"].Reserved[types.UID("1")].Unit.Name, "job1")
		}
	}
}

func Test_cacheImpl_AddOrUpdateQuota_DeleteQuota(t *testing.T) {
	cache := buildCache()
	quotas := []*v1alpha1.ElasticQuota{
		MakeElasticQuota("test1", "default").Parent("test3").
			Max(map[string]int64{"cpu": 11}).Min(map[string]int64{"cpu": 10}).Quota(),
	}
	for _, quota := range quotas {
		cache.AddOrUpdateQuota(quota)
	}

	assert.Equal(t, 1, len(cache.quotaParent))
	assert.Equal(t, cache.quotaParent["test1"], "test3")
	assert.Equal(t, 1, len(cache.quotas))
	assert.Equal(t, int64(11), cache.quotas["test1"].Max["cpu"])
	assert.Equal(t, int64(10), cache.quotas["test1"].Min["cpu"])

	quotas = []*v1alpha1.ElasticQuota{
		MakeElasticQuota("test1", "default").Parent("test3").
			Max(map[string]int64{"cpu": 111}).Min(map[string]int64{"cpu": 110}).Quota(),
	}
	for _, quota := range quotas {
		cache.AddOrUpdateQuota(quota)
	}

	assert.Equal(t, 1, len(cache.quotaParent))
	assert.Equal(t, cache.quotaParent["test1"], "test3")
	assert.Equal(t, 1, len(cache.quotas))
	assert.Equal(t, int64(111), cache.quotas["test1"].Max["cpu"])
	assert.Equal(t, int64(110), cache.quotas["test1"].Min["cpu"])

	q := &v1alpha1.ElasticQuota{}
	q.Name = "test2"
	cache.DeleteQuota(q)
	assert.Equal(t, 1, len(cache.quotaParent))
	assert.Equal(t, 1, len(cache.quotas))

	q = &v1alpha1.ElasticQuota{}
	q.Name = "test1"
	cache.DeleteQuota(q)
	assert.Equal(t, 0, len(cache.quotaParent))
	assert.Equal(t, 0, len(cache.quotas))
}
