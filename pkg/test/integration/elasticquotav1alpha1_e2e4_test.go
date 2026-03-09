package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	queuunitv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"

	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestElasticQuotav1alpha1_QueueAdd_Update_Delete(t *testing.T) {
	t.Skip("高版本修复")
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	_, plugins, _ := testutils.NewFrameworkForTesting()
	elasticQuotaPlugin := plugins[elasticquotav1alpha1.Name].(*elasticquotav1alpha1.ElasticQuota)
	//create queue unit
	priority := int32(9999)
	queueUnit1 := NewQueueuUnit().Namespace("kube-system").UID("11").Name("job11").
		Resource(v1.ResourceList{"cpu": resource.MustParse("9")}).QueueUnit()
	queueUnit1.Labels = map[string]string{
		elasticquotav1alpha1.QuotaNameLabelKey: "test10",
	}
	queueUnit1.Status.Phase = queuunitv1alpha1.Dequeued
	queueUnit1.Spec.Priority = &priority
	queueUnit1.CreationTimestamp = metav1.Time{
		Time: time.Now(),
	}

	queueUnit2 := NewQueueuUnit().Namespace("kube-system").UID("12").Name("job12").
		Resource(v1.ResourceList{"cpu": resource.MustParse("11")}).QueueUnit()
	queueUnit2.Labels = map[string]string{
		elasticquotav1alpha1.QuotaNameLabelKey: "test10",
	}
	queueUnit2.Spec.Priority = &priority
	queueUnit2.CreationTimestamp = metav1.Time{
		Time: time.Now(),
	}

	//create queue unit
	elasticQuotaPlugin.GetQueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").
		Create(context.Background(), queueUnit1, metav1.CreateOptions{})
	elasticQuotaPlugin.GetQueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").
		Create(context.Background(), queueUnit2, metav1.CreateOptions{})
	time.Sleep(time.Millisecond * 100)

	//create queue
	testElasticQuota := &eqv1alpha1.ElasticQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "test10",
		},
		Spec: eqv1alpha1.ElasticQuotaSpec{
			Min: v1.ResourceList{"cpu": resource.MustParse("20")},
			Max: v1.ResourceList{"cpu": resource.MustParse("21")},
		},
	}
	elasticQuotaPlugin.GetElasticQuotaClient().SchedulingV1alpha1().
		ElasticQuotas("kube-system").Create(context.Background(), testElasticQuota, metav1.CreateOptions{})
	time.Sleep(time.Millisecond * 100)

	queueCr, _ := elasticQuotaPlugin.GetQueueUnitClient().SchedulingV1alpha1().Queues(
		elasticquotav1alpha1.KoordQueueNamespace).Get(context.Background(), testElasticQuota.Name, metav1.GetOptions{})
	assert.Equal(t, queueCr.Name, testElasticQuota.Name)
	// assert.Equal(t, queueCr.Spec.QueuePolicy, queuunitv1alpha1.QueuePolicy(common.PaiStrategyRoundRobin))

	elasticQuotaInfo := elasticQuotaPlugin.GetElasticQuotaInfo4Test("test10")
	assert.Equal(t, int64(9), elasticQuotaInfo.Used["cpu"])
	assert.Equal(t, int64(20), elasticQuotaInfo.Min["cpu"])
	assert.Equal(t, int64(21), elasticQuotaInfo.Max["cpu"])

	//update queue
	testElasticQuota = &eqv1alpha1.ElasticQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "test10",
			Labels:    map[string]string{
				// queuepolicies.QueuePolicyLabelKey: common.PaiStrategyBalance,
			},
		},
		Spec: eqv1alpha1.ElasticQuotaSpec{
			Min: v1.ResourceList{"cpu": resource.MustParse("200")},
			Max: v1.ResourceList{"cpu": resource.MustParse("211")},
		},
	}
	elasticQuotaPlugin.GetElasticQuotaClient().SchedulingV1alpha1().
		ElasticQuotas("kube-system").Update(context.Background(), testElasticQuota, metav1.UpdateOptions{})
	time.Sleep(time.Millisecond * 100)

	queueCr, _ = elasticQuotaPlugin.GetQueueUnitClient().SchedulingV1alpha1().Queues(
		elasticquotav1alpha1.KoordQueueNamespace).Get(context.Background(), testElasticQuota.Name, metav1.GetOptions{})
	assert.Equal(t, queueCr.Name, testElasticQuota.Name)
	// assert.Equal(t, queueCr.Spec.QueuePolicy, queuunitv1alpha1.QueuePolicy(common.PaiStrategyBalance))

	elasticQuotaInfo = elasticQuotaPlugin.GetElasticQuotaInfo4Test("test10")
	assert.Equal(t, int64(9), elasticQuotaInfo.Used["cpu"])
	assert.Equal(t, int64(200), elasticQuotaInfo.Min["cpu"])
	assert.Equal(t, int64(211), elasticQuotaInfo.Max["cpu"])
	assert.Equal(t, int64(9), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
	assert.Equal(t, int64(9), elasticQuotaInfo.SelfUsed["cpu"])
	assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
	assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])

	debugResult := elasticQuotaPlugin.GetDebugInfoInternal(true)
	resultData, _ := json.MarshalIndent(debugResult, "", "\t")
	t.Logf("%v", string(resultData))

	//delete queue
	elasticQuotaPlugin.GetElasticQuotaClient().SchedulingV1alpha1().
		ElasticQuotas("kube-system").Delete(context.Background(), testElasticQuota.Name, metav1.DeleteOptions{})
	time.Sleep(time.Millisecond * 100)
	elasticQuotaInfo = elasticQuotaPlugin.GetElasticQuotaInfo4Test("test10")
	assert.True(t, elasticQuotaInfo == nil)

	queueCr, _ = elasticQuotaPlugin.GetQueueUnitClient().SchedulingV1alpha1().Queues(
		elasticquotav1alpha1.KoordQueueNamespace).Get(context.Background(), testElasticQuota.Name, metav1.GetOptions{})
	assert.True(t, queueCr == nil)
}
