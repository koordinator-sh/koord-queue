package multischedulingqueue

import (
	"testing"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

func newMultiSchedulingQueueForTesting(queueNames ...string) *MultiSchedulingQueue {
	queueMap := map[string]*queue.Queue{}
	for _, name := range queueNames {
		queueMap[name] = queue.NewQueueForTesting(name, nil)
	}
	return &MultiSchedulingQueue{
		queueMap:                  queueMap,
		queueUnitToQueue:          map[string]string{},
		queueUnitFindNoQueue:      []*framework.QueueUnitInfo{},
		queueUnitNotFoundNotified: sets.New[string](),
	}
}

func newQueueUnitForTesting(namespace, name string) *v1alpha1.QueueUnit {
	return &v1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
}

// DeleteQueueUnit should remove the qu->queue mapping and the notified marker,
// otherwise entries leak as QueueUnits come and go.
func TestDeleteQueueUnitClearsMapping(t *testing.T) {
	mq := newMultiSchedulingQueueForTesting("q1")
	qu := newQueueUnitForTesting("test", "job1")
	key := "test/job1"

	mq.SetQueueForQueueUnit(qu, "q1")
	mq.queueUnitNotFoundNotified.Insert(key)

	mq.DeleteQueueUnit(qu)

	assert.Equal(t, "", mq.GetQueueForQueueUnit(qu), "qu->queue mapping should be cleared")
	assert.NotContains(t, mq.queueUnitToQueue, key)
	assert.False(t, mq.queueUnitNotFoundNotified.Has(key), "notified marker should be cleared")
	assert.Contains(t, mq.queueMap, "q1", "queueMap must not be touched")
}

// DeleteQueueUnit should be a safe no-op for a QueueUnit that has no mapping,
// e.g. one that never matched any queue.
func TestDeleteQueueUnitWithoutMapping(t *testing.T) {
	mq := newMultiSchedulingQueueForTesting("q1")
	qu := newQueueUnitForTesting("test", "job-unmapped")

	assert.NotPanics(t, func() {
		mq.DeleteQueueUnit(qu)
	})
	assert.Contains(t, mq.queueMap, "q1", "queueMap must not be touched")
}

// AddUnitsFindNoQueue is documented to clear the qu->queue mapping. It used to
// delete from queueMap with a ns/name key instead, which was always a no-op and
// left a stale mapping behind.
func TestAddUnitsFindNoQueueClearsMapping(t *testing.T) {
	mq := newMultiSchedulingQueueForTesting("q1")
	qu := newQueueUnitForTesting("test", "job1")
	key := "test/job1"

	mq.SetQueueForQueueUnit(qu, "q1")

	mq.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(qu))

	assert.NotContains(t, mq.queueUnitToQueue, key, "stale qu->queue mapping should be cleared")
	assert.Contains(t, mq.queueMap, "q1", "queueMap must not be touched")
	if assert.Len(t, mq.queueUnitFindNoQueue, 1) {
		assert.Equal(t, key, mq.queueUnitFindNoQueue[0].Name)
	}
}
