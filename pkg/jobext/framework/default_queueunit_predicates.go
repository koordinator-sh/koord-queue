package framework

import (
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type UpdatePredicate struct {
}

// Create returns true if the Create event should be processed
func (p *UpdatePredicate) Create(obj event.CreateEvent) bool {
	// if st, _ := p.d.genericJobExtension.GetJobStatus(context.TODO(), obj.Object, p.d.client); st == Running {
	// 	klog.InfoS("skip to add running job when job extension start", "job", obj.Object.GetName())
	// 	return false
	// }
	// klog.InfoS("create event", "job", obj.Object.GetName())
	return true
}

// Delete returns true if the Delete event should be processed
func (p *UpdatePredicate) Delete(obj event.DeleteEvent) bool {
	return true
}

// Update returns true if the Update event should be processed
func (p *UpdatePredicate) Update(e event.UpdateEvent) bool {
	return e.ObjectNew.GetResourceVersion() != e.ObjectOld.GetResourceVersion()
}

// Generic returns true if the Generic event should be processed
func (p *UpdatePredicate) Generic(obj event.GenericEvent) bool {
	return true
}

type Predicate struct {
	GroupVersion string
	Kind         string
}

// Create returns true if the Create event should be processed
func (p *Predicate) Create(obj event.CreateEvent) bool {
	qu := obj.Object.(*v1alpha1.QueueUnit)
	if qu == nil {
		return false
	}
	return qu.Spec.ConsumerRef.Kind == p.Kind && qu.Spec.ConsumerRef.APIVersion == p.GroupVersion
}

// Delete returns true if the Delete event should be processed
func (p *Predicate) Delete(obj event.DeleteEvent) bool {
	qu := obj.Object.(*v1alpha1.QueueUnit)
	if qu == nil {
		return false
	}
	return qu.Spec.ConsumerRef.Kind == p.Kind && qu.Spec.ConsumerRef.APIVersion == p.GroupVersion
}

// Update returns true if the Update event should be processed
func (p *Predicate) Update(obj event.UpdateEvent) bool {
	qu := obj.ObjectNew.(*v1alpha1.QueueUnit)
	if qu == nil {
		return false
	}
	return qu.Spec.ConsumerRef.Kind == p.Kind && qu.Spec.ConsumerRef.APIVersion == p.GroupVersion
}

// Generic returns true if the Generic event should be processed
func (p *Predicate) Generic(obj event.GenericEvent) bool {
	qu := obj.Object.(*v1alpha1.QueueUnit)
	if qu == nil {
		return false
	}
	return qu.Spec.ConsumerRef.Kind == p.Kind && qu.Spec.ConsumerRef.APIVersion == p.GroupVersion
}

type StatuePredicate struct {
	IsFinished func(obj client.Object) bool
}

// Create returns true if the Create event should be processed
func (p *StatuePredicate) Create(obj event.CreateEvent) bool {
	return !p.IsFinished(obj.Object)
}

// Delete returns true if the Delete event should be processed
func (p *StatuePredicate) Delete(obj event.DeleteEvent) bool {
	return !p.IsFinished(obj.Object)
}

// Update returns true if the Update event should be processed
func (p *StatuePredicate) Update(obj event.UpdateEvent) bool {
	return !p.IsFinished(obj.ObjectNew)
}

// Generic returns true if the Generic event should be processed
func (p *StatuePredicate) Generic(obj event.GenericEvent) bool {
	return !p.IsFinished(obj.Object)
}
