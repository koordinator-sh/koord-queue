package queueunits

import (
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	eqv1beta1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type QueueWrapper struct {
	queue *v1alpha1.Queue
}

func NewQueue(name string) *QueueWrapper {
	return &QueueWrapper{queue: &v1alpha1.Queue{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "koord-queue"}}}
}

func (q *QueueWrapper) Policy(p string) *QueueWrapper {
	q.queue.Spec.QueuePolicy = v1alpha1.QueuePolicy(p)
	return q
}

func (q *QueueWrapper) Queue() *v1alpha1.Queue {
	return q.queue
}

type QueueUnitWrapper struct {
	queueunit *v1alpha1.QueueUnit
}

func MakeQueueUnitWrapper(qu *v1alpha1.QueueUnit) *QueueUnitWrapper {
	return &QueueUnitWrapper{queueunit: qu.DeepCopy()}
}

func MakeQueueUnit(name string, namespace string) *QueueUnitWrapper {
	return &QueueUnitWrapper{queueunit: &v1alpha1.QueueUnit{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), Namespace: namespace, CreationTimestamp: metav1.Now()}, Spec: v1alpha1.QueueUnitSpec{
		Resource: v1.ResourceList{}, Request: v1.ResourceList{},
	}}}
}

func (q *QueueUnitWrapper) PodSets(ps ...kueue.PodSet) *QueueUnitWrapper {
	q.queueunit.Spec.PodSets = make([]kueue.PodSet, 0)
	q.queueunit.Spec.PodSets = append(q.queueunit.Spec.PodSets, ps...)
	return q
}

func (q *QueueUnitWrapper) PodSetSimple(request map[string]int64, replicas int32) *QueueUnitWrapper {
	if len(q.queueunit.Spec.PodSets) == 0 {
		q.queueunit.Spec.PodSets = make([]kueue.PodSet, 0)
	}
	req := v1.ResourceList{}
	for k, v := range request {
		req[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
	}
	q.queueunit.Spec.PodSets = append(q.queueunit.Spec.PodSets, kueue.PodSet{
		Name:  "default",
		Count: replicas,
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Requests: req,
			},
		}}}}})
	return q
}

func (q *QueueUnitWrapper) Annotations(res map[string]string) *QueueUnitWrapper {
	q.queueunit.Annotations = res
	return q
}

func (q *QueueUnitWrapper) Labels(res map[string]string) *QueueUnitWrapper {
	q.queueunit.Labels = res
	return q
}

func (q *QueueUnitWrapper) Resources(res map[string]int64) *QueueUnitWrapper {
	for k, v := range res {
		q.queueunit.Spec.Resource[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
		q.queueunit.Spec.Request[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
	}
	return q
}

func (q *QueueUnitWrapper) MilliResources(res map[string]int64) *QueueUnitWrapper {
	for k, v := range res {
		if k == "cpu" {
			q.queueunit.Spec.Resource[v1.ResourceName(k)] = *resource.NewMilliQuantity(v, resource.DecimalSI)
			q.queueunit.Spec.Request[v1.ResourceName(k)] = *resource.NewMilliQuantity(v, resource.DecimalSI)
		} else {
			q.queueunit.Spec.Resource[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
			q.queueunit.Spec.Request[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
		}
	}
	return q
}

func (q *QueueUnitWrapper) Priority(p int32) *QueueUnitWrapper {
	q.queueunit.Spec.Priority = ptr.To(p)
	return q
}

// Active sets spec.active, which controls whether the queue unit may be admitted.
func (q *QueueUnitWrapper) Active(active bool) *QueueUnitWrapper {
	q.queueunit.Spec.Active = ptr.To(active)
	return q
}

// MaximumExecutionTime caps how long the job may execute once its pods start running.
func (q *QueueUnitWrapper) MaximumExecutionTime(seconds int32) *QueueUnitWrapper {
	q.queueunit.Spec.MaximumExecutionTimeSeconds = ptr.To(seconds)
	return q
}

func (q *QueueUnitWrapper) QueueUnit() *v1alpha1.QueueUnit {
	return q.queueunit
}

func (q *QueueUnitWrapper) Phase(phase v1alpha1.QueueUnitPhase) *QueueUnitWrapper {
	q.queueunit.Status.Phase = phase
	return q
}

func (q *QueueUnitWrapper) AdmissionCheck(name string, state kueue.CheckState) *QueueUnitWrapper {
	q.queueunit.Status.AdmissionChecks = append(q.queueunit.Status.AdmissionChecks, kueue.AdmissionCheckState{
		Name:  name,
		State: state,
	})
	return q
}

type ElasticQuotaTreeWrapper struct {
	root    *eqv1beta1.ElasticQuotaTree
	current *eqv1beta1.ElasticQuotaSpec
}

func ElasticQuotaTree(min, max v1.ResourceList) *ElasticQuotaTreeWrapper {
	root := &eqv1beta1.ElasticQuotaTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eqt",
			Namespace: "kube-system",
		},
		Spec: eqv1beta1.ElasticQuotaTreeSpec{
			Root: eqv1beta1.ElasticQuotaSpec{
				Name:     "root",
				Min:      min,
				Max:      max,
				Children: make([]eqv1beta1.ElasticQuotaSpec, 0),
			},
		},
	}
	return &ElasticQuotaTreeWrapper{
		root:    root,
		current: &root.Spec.Root,
	}
}

func (e *ElasticQuotaTreeWrapper) Child(name string, namespace []string, min, max v1.ResourceList) *ElasticQuotaTreeWrapper {
	newChild := eqv1beta1.ElasticQuotaSpec{
		Name:       name,
		Namespaces: namespace,
		Min:        min,
		Max:        max,
		Children:   make([]eqv1beta1.ElasticQuotaSpec, 0),
	}
	e.current.Children = append(e.current.Children, newChild)
	return &ElasticQuotaTreeWrapper{
		root:    e.root,
		current: &newChild,
	}
}

func (e *ElasticQuotaTreeWrapper) Obj() *eqv1beta1.ElasticQuotaTree {
	return e.root
}

// ElasticQuotaWrapper wraps Koordinator ElasticQuota CRD (v1alpha1)
type ElasticQuotaWrapper struct {
	elasticQuota *eqv1alpha1.ElasticQuota
}

// MakeElasticQuota creates a new ElasticQuota wrapper
func MakeElasticQuota(name, namespace string) *ElasticQuotaWrapper {
	return &ElasticQuotaWrapper{
		elasticQuota: &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}

// Min sets the min resources for the ElasticQuota
func (e *ElasticQuotaWrapper) Min(min v1.ResourceList) *ElasticQuotaWrapper {
	e.elasticQuota.Spec.Min = min
	return e
}

// Max sets the max resources for the ElasticQuota
func (e *ElasticQuotaWrapper) Max(max v1.ResourceList) *ElasticQuotaWrapper {
	e.elasticQuota.Spec.Max = max
	return e
}

// Labels sets the labels for the ElasticQuota
func (e *ElasticQuotaWrapper) Labels(labels map[string]string) *ElasticQuotaWrapper {
	e.elasticQuota.Labels = labels
	return e
}

// Annotations sets the annotations for the ElasticQuota
func (e *ElasticQuotaWrapper) Annotations(annotations map[string]string) *ElasticQuotaWrapper {
	e.elasticQuota.Annotations = annotations
	return e
}

// QueuePolicy sets the queue policy label for the ElasticQuota
func (e *ElasticQuotaWrapper) QueuePolicy(policy string) *ElasticQuotaWrapper {
	if e.elasticQuota.Labels == nil {
		e.elasticQuota.Labels = make(map[string]string)
	}
	e.elasticQuota.Labels[queuepolicies.QueuePolicyLabelKey] = policy
	return e
}

// Obj returns the ElasticQuota object
func (e *ElasticQuotaWrapper) Obj() *eqv1alpha1.ElasticQuota {
	return e.elasticQuota
}
