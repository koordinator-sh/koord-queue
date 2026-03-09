package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/cmd/app/options"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	elasticquotatree "github.com/kube-queue/kube-queue/pkg/framework/plugins/elasticquota"
	"github.com/kube-queue/kube-queue/pkg/test/testutils"
	"github.com/kube-queue/kube-queue/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func TestElasticQuotaReserve(t *testing.T) {
	t.Skip("高版本修复")

	os.Setenv("QueueGroupPlugin", "elasticquota")
	options.SetDefaultPreemptibleForTest(true)
	_, plugins, _ := testutils.NewFrameworkForTesting()
	elasticquotaplugin := plugins[elasticquotatree.Name].(*elasticquotatree.ElasticQuota)

	testElasticQuotaTree := &v1beta1.ElasticQuotaTree{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "test",
		},
		Spec: v1beta1.ElasticQuotaTreeSpec{
			Root: v1beta1.ElasticQuotaSpec{
				Name: "root",
				Min:  v1.ResourceList{"cpu": resource.MustParse("21")},
				Max:  v1.ResourceList{"cpu": resource.MustParse("21")},
				Children: []v1beta1.ElasticQuotaSpec{
					{
						Name: "child1",
						Min:  v1.ResourceList{"cpu": resource.MustParse("15")},
						Max:  v1.ResourceList{"cpu": resource.MustParse("15")},
						Children: []v1beta1.ElasticQuotaSpec{
							{
								Name:       "child1_1",
								Min:        v1.ResourceList{"cpu": resource.MustParse("10")},
								Max:        v1.ResourceList{"cpu": resource.MustParse("10")},
								Children:   []v1beta1.ElasticQuotaSpec{},
								Namespaces: []string{"quota-1-1"},
							},
							{
								Name:       "child1_2",
								Min:        v1.ResourceList{"cpu": resource.MustParse("5")},
								Max:        v1.ResourceList{"cpu": resource.MustParse("5")},
								Children:   []v1beta1.ElasticQuotaSpec{},
								Namespaces: []string{"quota-1-2"},
							},
						},
					},
					{
						Name: "child2",
						Min:  v1.ResourceList{"cpu": resource.MustParse("6")},
						Max:  v1.ResourceList{"cpu": resource.MustParse("6"), "kube-queue/max-jobs": resource.MustParse("2")},
						Children: []v1beta1.ElasticQuotaSpec{
							{
								Name:       "child2_1",
								Min:        v1.ResourceList{"cpu": resource.MustParse("3")},
								Max:        v1.ResourceList{"cpu": resource.MustParse("3"), "kube-queue/max-jobs": resource.MustParse("2")},
								Children:   []v1beta1.ElasticQuotaSpec{},
								Namespaces: []string{"quota-2-1"},
							},
							{
								Name:       "child2_2",
								Min:        v1.ResourceList{"cpu": resource.MustParse("3")},
								Max:        v1.ResourceList{"cpu": resource.MustParse("3"), "kube-queue/max-jobs": resource.MustParse("1")},
								Children:   []v1beta1.ElasticQuotaSpec{},
								Namespaces: []string{"quota-2-2"},
							},
						},
					},
				},
			},
		},
	}
	elasticquotaplugin.AddElasticQuotaTree(testElasticQuotaTree)
	testQuota1 := NewQueueuUnit().Namespace("quota-1-1").UID("test-1").Name("test-1").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 2}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("2")}).Status(v1alpha1.Dequeued).QueueUnit()
	testQuota2 := NewQueueuUnit().Namespace("quota-1-2").UID("test-2").Name("test-2").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 2}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("2")}).Status(v1alpha1.Dequeued).QueueUnit()
	testQuota3 := NewQueueuUnit().Namespace("quota-2-1").UID("test-3").Name("test-3").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 2}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("2")}).Status(v1alpha1.Dequeued).QueueUnit()
	testQuota4 := NewQueueuUnit().Namespace("quota-2-2").UID("test-4").Name("test-4").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 2}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("2")}).Status(v1alpha1.Dequeued).QueueUnit()

	// 1. can reserve jobs
	// cli.SchedulingV1alpha1().QueueUnits(testQuota1.Namespace).Create(context.Background(), testQuota1, metav1.CreateOptions{})
	// cli.SchedulingV1alpha1().QueueUnits(testQuota2.Namespace).Create(context.Background(), testQuota2, metav1.CreateOptions{})
	// cli.SchedulingV1alpha1().QueueUnits(testQuota3.Namespace).Create(context.Background(), testQuota3, metav1.CreateOptions{})
	// cli.SchedulingV1alpha1().QueueUnits(testQuota4.Namespace).Create(context.Background(), testQuota4, metav1.CreateOptions{})

	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota1), utils.GetQueueUnitResourceRequirementAds(testQuota1))
	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota2), utils.GetQueueUnitResourceRequirementAds(testQuota2))
	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota3), utils.GetQueueUnitResourceRequirementAds(testQuota3))
	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota4), utils.GetQueueUnitResourceRequirementAds(testQuota4))

	info1, _ := elasticquotaplugin.GetElasticQuotaInfo(testQuota1)
	if diff := cmp.Diff(info1.Count, int64(1)); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info1.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	info2, _ := elasticquotaplugin.GetElasticQuotaInfo(testQuota2)
	if diff := cmp.Diff(info2.Count, int64(1)); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info2.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	info3, _ := elasticquotaplugin.GetElasticQuotaInfo(testQuota3)
	if diff := cmp.Diff(info3.Count, int64(1)); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info3.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	info4, _ := elasticquotaplugin.GetElasticQuotaInfo(testQuota4)
	if diff := cmp.Diff(info4.Count, int64(1)); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info4.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	// 2. cpu exceed
	testQuota5 := NewQueueuUnit().Namespace("quota-1-2").UID("test-5").Name("test-5").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 4}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("4")}).QueueUnit()
	status := elasticquotaplugin.Filter(context.Background(), framework.NewQueueUnitInfo(testQuota5), utils.GetQueueUnitResourceRequirementAds(testQuota5))
	if diff := cmp.Diff(status.Message(), fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, max %v, used %v, oversellreate %v. Wait for running jobs to complete",
		"cpu", "root/child1/child1_2", "4", "5", "2", "1")); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	// 3. job count exceed
	testQuota6 := NewQueueuUnit().Namespace("quota-2-2").UID("test-6").Name("test-6").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 1}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("1")}).QueueUnit()
	status = elasticquotaplugin.Filter(context.Background(), framework.NewQueueUnitInfo(testQuota6), utils.GetQueueUnitResourceRequirementAds(testQuota6))
	if diff := cmp.Diff(status.Message(), fmt.Sprintf("Insufficient quota(%v) in quota %v: request %v, max %v, used %v, oversellreate %v. Wait for running jobs to complete",
		"jobs", "root/child2/child2_2", "1", "1", "1", "1")); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	testQuota7 := NewQueueuUnit().Namespace("quota-2-1").UID("test-7").Name("test-7").PodSets([]kueue.PodSet{{
		Name: "admission-1",
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			}}}}}}, Count: 1}}...).Resource(v1.ResourceList{"cpu": resource.MustParse("1")}).QueueUnit()
	status = elasticquotaplugin.Filter(context.Background(), framework.NewQueueUnitInfo(testQuota7), utils.GetQueueUnitResourceRequirementAds(testQuota7))
	if diff := cmp.Diff(status.Message(), fmt.Sprintf("Insufficient quota(%v) in parent quota %v: request %v, max %v, used %v, oversellreate %v. Wait for running jobs to complete",
		"jobs", "root/child2", "1", "2", "2", "1")); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	// 4. can unreserve jobs
	// cli.SchedulingV1alpha1().QueueUnits(testQuota4.Namespace).Delete(context.Background(), testQuota4.Name, metav1.DeleteOptions{})
	elasticquotaplugin.Unreserve(context.Background(), framework.NewQueueUnitInfo(testQuota4))
	info4, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota4)
	if diff := cmp.Diff(info4.Count, int64(0), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info4.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 0}}, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	// 5. resize the job to reduce consumption
	testQuota1.Status.Admissions = []v1alpha1.Admission{
		{
			Name:     "admission-1",
			Replicas: 4,
		},
	}
	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota1), utils.GetQueueUnitResourceRequirementAds(testQuota1))
	newTestQuota1 := testQuota1.DeepCopy()
	newTestQuota1.Status.Admissions = []v1alpha1.Admission{
		{
			Name:     "admission-1",
			Replicas: 2,
		},
	}
	elasticquotaplugin.Resize(context.Background(), framework.NewQueueUnitInfo(testQuota1), framework.NewQueueUnitInfo(newTestQuota1))
	info1, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota1)
	if diff := cmp.Diff(info1.Count, int64(1), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info1.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	// 6. resize the job to increase consumption
	testQuota4.Status.Admissions = []v1alpha1.Admission{
		{
			Name:     "admission-1",
			Replicas: 0,
		},
	}
	newTestQuota4 := testQuota4.DeepCopy()
	newTestQuota4.Status.Admissions = []v1alpha1.Admission{
		{
			Name:     "admission-1",
			Replicas: 2,
		},
	}
	elasticquotaplugin.Resize(context.Background(), framework.NewQueueUnitInfo(testQuota4), framework.NewQueueUnitInfo(newTestQuota4))
	// elasticquotaplugin.Unreserve(context.Background(), framework.NewQueueUnitInfo(testQuota4))
	info4, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota4)
	if diff := cmp.Diff(info4.Count, int64(1), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info4.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 2000}}, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}

	// 7. can delete quota tree
	elasticquotaplugin.DeleteElasticQuotaTree(testElasticQuotaTree)
	info1, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota1)
	if info1 != nil {
		t.Errorf("Unexpected result (-got,+want):\n%v", "not nil")
	}
	info2, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota2)
	if info2 != nil {
		t.Errorf("Unexpected result (-got,+want):\n%v", "not nil")
	}
	info3, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota3)
	if info3 != nil {
		t.Errorf("Unexpected result (-got,+want):\n%v", "not nil")
	}
	info4, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota4)
	if info4 != nil {
		t.Errorf("Unexpected result (-got,+want):\n%v", "not nil")
	}

	// 8. can reconstruct quota tree
	elasticquotaplugin.AddElasticQuotaTree(testElasticQuotaTree)

	// 9. reserve
	elasticquotaplugin.Reserve(context.Background(), framework.NewQueueUnitInfo(testQuota6), utils.GetQueueUnitResourceRequirementAds(testQuota6))
	info4, _ = elasticquotaplugin.GetElasticQuotaInfo(testQuota6)
	if diff := cmp.Diff(info4.Count, int64(1), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
	if diff := cmp.Diff(info4.Used, &utils.Resource{Resources: map[v1.ResourceName]int64{"cpu": 1000}}, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("Unexpected result (-got,+want):\n%s", diff)
	}
}

type QueueUnitWraper struct {
	qu *v1alpha1.QueueUnit
}

func NewQueueuUnit() *QueueUnitWraper {
	return &QueueUnitWraper{&v1alpha1.QueueUnit{}}
}

func (q *QueueUnitWraper) Namespace(ns string) *QueueUnitWraper { q.qu.Namespace = ns; return q }
func (q *QueueUnitWraper) UID(uid types.UID) *QueueUnitWraper   { q.qu.UID = uid; return q }
func (q *QueueUnitWraper) Name(n string) *QueueUnitWraper       { q.qu.Name = n; return q }

func (q *QueueUnitWraper) PodSets(ps ...kueue.PodSet) *QueueUnitWraper {
	q.qu.Spec.PodSets = make([]kueue.PodSet, 0)
	q.qu.Spec.PodSets = append(q.qu.Spec.PodSets, ps...)
	return q
}

func (q *QueueUnitWraper) Resource(res v1.ResourceList) *QueueUnitWraper {
	q.qu.Spec.Resource = res
	return q
}
func (q *QueueUnitWraper) Queue(queue string) *QueueUnitWraper { q.qu.Spec.Queue = queue; return q }
func (q *QueueUnitWraper) QueueUnit() *v1alpha1.QueueUnit      { return q.qu }
func (q *QueueUnitWraper) Status(p v1alpha1.QueueUnitPhase) *QueueUnitWraper {
	q.qu.Status.Phase = p
	return q
}
