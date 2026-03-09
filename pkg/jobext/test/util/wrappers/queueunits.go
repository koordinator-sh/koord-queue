package wrappers

import (
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
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
	return &QueueWrapper{queue: &v1alpha1.Queue{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kube-queue"}}}
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

func (q *QueueUnitWrapper) Admission(name string, resources map[string]int64, replicas int32) *QueueUnitWrapper {
	if len(q.queueunit.Status.Admissions) == 0 {
		q.queueunit.Status.Admissions = make([]v1alpha1.Admission, 0)
	}
	req := v1.ResourceList{}
	for k, v := range resources {
		if k == "cpu" {
			req[v1.ResourceName(k)] = *resource.NewMilliQuantity(v, resource.DecimalSI)
		} else {
			req[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
		}
	}
	q.queueunit.Status.Admissions = append(q.queueunit.Status.Admissions, v1alpha1.Admission{
		Name:      name,
		Resources: req,
		Replicas:  int64(replicas),
	})
	return q
}

func (q *QueueUnitWrapper) PodSetSimple(name string, request map[string]int64, replicas int32) *QueueUnitWrapper {
	if len(q.queueunit.Spec.PodSets) == 0 {
		q.queueunit.Spec.PodSets = make([]kueue.PodSet, 0)
	}
	req := v1.ResourceList{}
	for k, v := range request {
		if k == "cpu" {
			req[v1.ResourceName(k)] = *resource.NewMilliQuantity(v, resource.DecimalSI)
		} else {
			req[v1.ResourceName(k)] = *resource.NewQuantity(v, resource.DecimalSI)
		}
	}
	q.queueunit.Spec.PodSets = append(q.queueunit.Spec.PodSets, kueue.PodSet{
		Name:  name,
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

func (q *QueueUnitWrapper) Priority(p int32) *QueueUnitWrapper {
	q.queueunit.Spec.Priority = ptr.To(p)
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
