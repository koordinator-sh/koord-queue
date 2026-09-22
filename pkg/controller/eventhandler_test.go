package controller

import (
	"os"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Regression test: Controller.DeleteQueueUnit must clear the qu->queue mapping
// held by MultiSchedulingQueue. It used to only remove the unit from its queue,
// leaking one queueUnitToQueue entry per deleted QueueUnit.
func TestControllerDeleteQueueUnitClearsMapping(t *testing.T) {
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	fw, _, _ := NewFrameworkForTesting(nil)

	controller := &Controller{}
	controller.scheduler = &scheduler.Scheduler{}
	controller.SetFramework(fw)

	queueUnitLister := fw.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister()
	mq, err := multischedulingqueue.NewMultiSchedulingQueue(fw, 0, 0, queueUnitLister, false, nil)
	assert.NoError(t, err)
	controller.SetMultiSchedulingQueue(mq)

	q := &v1alpha1.Queue{}
	q.Name = "q1"
	q.Spec.QueuePolicy = "Block"
	assert.NoError(t, mq.Add(q))

	unit := &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "job1",
		},
	}
	mq.SetQueueForQueueUnit(unit, "q1")
	assert.Equal(t, "q1", mq.GetQueueForQueueUnit(unit))

	controller.DeleteQueueUnit(unit)

	assert.Equal(t, "", mq.GetQueueForQueueUnit(unit), "qu->queue mapping should be cleared on delete")
	_, ok := mq.GetQueueByName("q1")
	assert.True(t, ok, "queue itself must not be removed")

	// deleting a unit that has no mapping must not panic
	assert.NotPanics(t, func() {
		controller.DeleteQueueUnit(unit)
	})
}

// Regression test: Update() on a queue that MultiSchedulingQueue has not
// registered yet must not deadlock. It used to call Add() while already holding
// the non-reentrant write lock, which blocked the scheduling queue forever
// whenever a Queue update event arrived before its add event.
func TestUpdateUnregisteredQueueDoesNotDeadlock(t *testing.T) {
	os.Setenv("QueueGroupPlugin", "elasticquotav2")
	fw, _, _ := NewFrameworkForTesting(nil)

	queueUnitLister := fw.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister()
	mq, err := multischedulingqueue.NewMultiSchedulingQueue(fw, 0, 0, queueUnitLister, false, nil)
	assert.NoError(t, err)

	q := &v1alpha1.Queue{}
	q.Name = "q1"
	q.Spec.QueuePolicy = "Block"

	done := make(chan error, 1)
	go func() {
		done <- mq.Update(q, q)
	}()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Update() deadlocked while registering an unknown queue")
	}

	_, ok := mq.GetQueueByName("q1")
	assert.True(t, ok, "Update() should have registered the queue")
}
