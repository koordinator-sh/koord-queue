package integration

import (
	"context"
	"os"
	"testing"
	"time"

	queuunitv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	controller2 "github.com/koordinator-sh/koord-queue/pkg/controller"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAddQueueInfoWillUpdateQueueInitStatus(t *testing.T) {
	t.Skip("高版本修复")
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	fw, plugins, _ := testutils.NewFrameworkForTesting()
	elasticQuotaPlugin := plugins[elasticquotav1alpha1.Name].(*elasticquotav1alpha1.ElasticQuota)

	controller := &controller2.Controller{}
	{
		controller.SetFramework(fw)

		queueUnitInformerFactory := externalversions.NewSharedInformerFactory(fw.QueueUnitClient(), 0)
		queueUnitLister := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
		queueUnitInformer := queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
		queueInformer := queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()
		queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Lister()

		multiSchedulingQueue, _ := multischedulingqueue.NewMultiSchedulingQueue(fw,
			0, 0, queueUnitLister, false)
		controller.SetMultiSchedulingQueue(multiSchedulingQueue)

		controller.AddAllEventHandlers(queueUnitInformer, queueInformer)
		go queueInformer.Run(wait.NeverStop)
		go queueUnitInformer.Run(wait.NeverStop)
		queueUnitInformerFactory.Start(wait.NeverStop)
		time.Sleep(time.Millisecond * 100)
	}

	testElasticQuota := &eqv1alpha1.ElasticQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "test",
			Labels:    map[string]string{
				// queuepolicies.QueuePolicyLabelKey: common.PaiStrategyBalance,
			},
			Annotations: map[string]string{
				utils.QuotaKoordQueueEnable: "true",
			},
		},
		Spec: eqv1alpha1.ElasticQuotaSpec{
			Min: v1.ResourceList{"cpu": resource.MustParse("200")},
			Max: v1.ResourceList{"cpu": resource.MustParse("211")},
		},
	}
	elasticQuotaPlugin.GetElasticQuotaClient().SchedulingV1alpha1().
		ElasticQuotas("kube-system").Create(context.Background(), testElasticQuota, metav1.CreateOptions{})
	time.Sleep(time.Millisecond * 1000)

	{
		priority := int32(9999)
		queueUnit1 := NewQueueuUnit().Namespace("kube-system").UID("1").Name("job1").
			Resource(v1.ResourceList{"cpu": resource.MustParse("9")}).QueueUnit()
		queueUnit1.Labels = map[string]string{
			elasticquotav1alpha1.QuotaNameLabelKey: "test",
		}
		queueUnit1.Status.Phase = ""
		queueUnit1.Spec.Priority = &priority
		queueUnit1.CreationTimestamp = metav1.Time{
			Time: time.Now(),
		}

		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").
			Create(context.Background(), queueUnit1, metav1.CreateOptions{})
		time.Sleep(time.Microsecond * 1000)

		queueUnitNew, _ := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").Get(
			context.Background(), "job1", metav1.GetOptions{})
		assert.Equal(t, queueUnitNew.Status.Phase, queuunitv1alpha1.Enqueued)
	}
	{
		priority := int32(9999)
		queueUnit2 := NewQueueuUnit().Namespace("kube-system").UID("1").Name("job2").
			Resource(v1.ResourceList{"cpu": resource.MustParse("9")}).QueueUnit()
		queueUnit2.Labels = map[string]string{
			elasticquotav1alpha1.QuotaNameLabelKey: "test",
		}
		queueUnit2.Status.Phase = queuunitv1alpha1.Dequeued
		queueUnit2.Spec.Priority = &priority
		queueUnit2.CreationTimestamp = metav1.Time{
			Time: time.Now(),
		}

		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").
			Create(context.Background(), queueUnit2, metav1.CreateOptions{})
		time.Sleep(time.Microsecond * 100)

		queueUnitNew, _ := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").Get(
			context.Background(), "job2", metav1.GetOptions{})
		assert.Equal(t, queueUnitNew.Status.Phase, queuunitv1alpha1.Dequeued)
	}

	testElasticQuota.Annotations[utils.QuotaKoordQueueEnable] = "false"
	elasticQuotaPlugin.GetElasticQuotaClient().SchedulingV1alpha1().
		ElasticQuotas("kube-system").Update(context.Background(), testElasticQuota, metav1.UpdateOptions{})
	time.Sleep(time.Millisecond * 3000)

	{
		priority := int32(9999)
		queueUnit3 := NewQueueuUnit().Namespace("kube-system").UID("1").Name("job3").
			Resource(v1.ResourceList{"cpu": resource.MustParse("9")}).QueueUnit()
		queueUnit3.Labels = map[string]string{
			elasticquotav1alpha1.QuotaNameLabelKey: "test",
		}
		queueUnit3.Status.Phase = ""
		queueUnit3.Spec.Priority = &priority
		queueUnit3.CreationTimestamp = metav1.Time{
			Time: time.Now(),
		}

		fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").
			Create(context.Background(), queueUnit3, metav1.CreateOptions{})
		time.Sleep(time.Microsecond * 1000)

		queueUnitNew, _ := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("kube-system").Get(
			context.Background(), "job3", metav1.GetOptions{})
		assert.Equal(t, queueUnitNew.Status.Phase, queuunitv1alpha1.QueueUnitPhase(""))
	}
}
