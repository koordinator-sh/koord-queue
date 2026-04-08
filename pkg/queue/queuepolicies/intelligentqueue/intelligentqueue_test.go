package intelligentqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"
	"github.com/koordinator-sh/koord-queue/pkg/test/fake"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// Helper function to create a test queue unit
func makeTestQueueUnit(namespace, name string, priority int32) *framework.QueueUnitInfo {
	return &framework.QueueUnitInfo{
		Name: namespace + "/" + name,
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				UID:       types.UID(namespace + "/" + name),
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: ptr.To(priority),
				PodSets: []kueue.PodSet{{
					Name:  "test-pod-set",
					Count: 1,
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("100m"),
											v1.ResourceMemory: resource.MustParse("100Mi"),
										},
									},
								},
							},
						},
					},
				}},
			},
			Status: v1alpha1.QueueUnitStatus{
				Phase: v1alpha1.Enqueued,
			},
		},
		InitialAttemptTimestamp: time.Now(),
	}
}

// TestPriorityLess tests priority comparison function
func TestPriorityLess(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		a        *framework.QueueUnitInfo
		b        *framework.QueueUnitInfo
		expected int
	}{
		{
			name: "higher priority should come first",
			a: &framework.QueueUnitInfo{
				Unit: &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{UID: "job1"},
					Spec:       v1alpha1.QueueUnitSpec{Priority: ptr.To(int32(10))},
				},
				InitialAttemptTimestamp: now,
			},
			b: &framework.QueueUnitInfo{
				Unit: &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{UID: "job2"},
					Spec:       v1alpha1.QueueUnitSpec{Priority: ptr.To(int32(5))},
				},
				InitialAttemptTimestamp: now,
			},
			expected: -1,
		},
		{
			name: "lower priority should come after",
			a: &framework.QueueUnitInfo{
				Unit: &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{UID: "job1"},
					Spec:       v1alpha1.QueueUnitSpec{Priority: ptr.To(int32(5))},
				},
				InitialAttemptTimestamp: now,
			},
			b: &framework.QueueUnitInfo{
				Unit: &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{UID: "job2"},
					Spec:       v1alpha1.QueueUnitSpec{Priority: ptr.To(int32(10))},
				},
				InitialAttemptTimestamp: now,
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := priorityLess(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIntelligentQueue_DistributeToQueue tests queue distribution
func TestIntelligentQueue_DistributeToQueue(t *testing.T) {
	fw := fake.NewFakeHandle(map[string]string{})

	q := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			Annotations: map[string]string{
				queuepolicies.PriorityThresholdAnnotationKey: "4",
			},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Intelligent",
		},
	}

	iq := NewIntelligentQueue("test", q, fw, nil, nil, nil).(*IntelligentQueue)

	// Test high priority task (priority >= 4)
	highPriorityTask := makeTestQueueUnit("default", "high-job", 5)
	iq.distributeToQueue(highPriorityTask)
	assert.Equal(t, 1, len(iq.highPriorityQueue), "High priority task should be in high priority queue")
	assert.Equal(t, 0, len(iq.lowPriorityQueue), "Low priority queue should be empty")

	// Test low priority task (priority < 4)
	lowPriorityTask := makeTestQueueUnit("default", "low-job", 2)
	iq.distributeToQueue(lowPriorityTask)
	assert.Equal(t, 1, len(iq.highPriorityQueue), "High priority queue should still have 1 task")
	assert.Equal(t, 1, len(iq.lowPriorityQueue), "Low priority task should be in low priority queue")

	// Test boundary case (priority == 4, should go to high priority)
	boundaryTask := makeTestQueueUnit("default", "boundary-job", 4)
	iq.distributeToQueue(boundaryTask)
	assert.Equal(t, 2, len(iq.highPriorityQueue), "Boundary task should be in high priority queue")
	assert.Equal(t, 1, len(iq.lowPriorityQueue), "Low priority queue should still have 1 task")
}

// TestIntelligentQueue_AddQueueUnitInfo tests adding queue units
func TestIntelligentQueue_AddQueueUnitInfo(t *testing.T) {
	fw := fake.NewFakeHandle(map[string]string{})

	q := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			Annotations: map[string]string{
				queuepolicies.PriorityThresholdAnnotationKey: "4",
			},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Intelligent",
		},
	}

	iq := NewIntelligentQueue("test", q, fw, nil, nil, nil).(*IntelligentQueue)

	// Add high priority task
	highTask := makeTestQueueUnit("default", "high-job", 5)
	added, err := iq.AddQueueUnitInfo(highTask)
	assert.True(t, added)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(iq.highPriorityQueue))
	assert.Equal(t, 1, len(iq.QueueUnits))

	// Add low priority task
	lowTask := makeTestQueueUnit("default", "low-job", 2)
	added, err = iq.AddQueueUnitInfo(lowTask)
	assert.True(t, added)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(iq.highPriorityQueue))
	assert.Equal(t, 1, len(iq.lowPriorityQueue))
	assert.Equal(t, 2, len(iq.QueueUnits))

	// Try to add duplicate
	added, err = iq.AddQueueUnitInfo(highTask)
	assert.False(t, added)
	assert.Error(t, err)
}

// TestIntelligentQueue_Top tests getting top queue unit
func TestIntelligentQueue_Top(t *testing.T) {
	fw := fake.NewFakeHandle(map[string]string{})

	q := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			Annotations: map[string]string{
				queuepolicies.PriorityThresholdAnnotationKey: "4",
			},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Intelligent",
		},
	}

	iq := NewIntelligentQueue("test", q, fw, nil, nil, nil).(*IntelligentQueue)

	// Empty queue should return nil
	top := iq.Top(context.Background())
	assert.Nil(t, top)

	// Add low priority task
	lowTask := makeTestQueueUnit("default", "low-job", 2)
	iq.AddQueueUnitInfo(lowTask)

	// Top should return low priority task
	top = iq.Top(context.Background())
	assert.NotNil(t, top)
	assert.Equal(t, "low-job", top.Unit.Name)

	// Add high priority task
	highTask := makeTestQueueUnit("default", "high-job", 5)
	iq.AddQueueUnitInfo(highTask)

	// Top should now return high priority task (higher priority takes precedence)
	top = iq.Top(context.Background())
	assert.NotNil(t, top)
	assert.Equal(t, "high-job", top.Unit.Name)
}

// TestIntelligentQueue_ThresholdUpdate tests dynamic threshold update
func TestIntelligentQueue_ThresholdUpdate(t *testing.T) {
	fw := fake.NewFakeHandle(map[string]string{})

	q := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			Annotations: map[string]string{
				queuepolicies.PriorityThresholdAnnotationKey: "4",
			},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Intelligent",
		},
	}

	iq := NewIntelligentQueue("test", q, fw, nil, nil, nil).(*IntelligentQueue)

	// Add tasks with different priorities
	task1 := makeTestQueueUnit("default", "job1", 3) // Low priority (< 4)
	task2 := makeTestQueueUnit("default", "job2", 5) // High priority (>= 4)
	iq.AddQueueUnitInfo(task1)
	iq.AddQueueUnitInfo(task2)

	assert.Equal(t, 1, len(iq.highPriorityQueue))
	assert.Equal(t, 1, len(iq.lowPriorityQueue))

	// Update threshold to 6
	newQ := q.DeepCopy()
	newQ.Annotations[queuepolicies.PriorityThresholdAnnotationKey] = "6"
	iq.UpdateQueueCr(newQ)

	// Now both tasks should be in low priority queue (both < 6)
	assert.Equal(t, 0, len(iq.highPriorityQueue))
	assert.Equal(t, 2, len(iq.lowPriorityQueue))

	// Update threshold to 2
	newQ2 := newQ.DeepCopy()
	newQ2.Annotations[queuepolicies.PriorityThresholdAnnotationKey] = "2"
	iq.UpdateQueueCr(newQ2)

	// Now both tasks should be in high priority queue (both >= 2)
	assert.Equal(t, 2, len(iq.highPriorityQueue))
	assert.Equal(t, 0, len(iq.lowPriorityQueue))
}

// TestIntelligentQueue_HighPriorityBlockingBehavior tests that high priority tasks
// block queue via nextIdx not incrementing, while low priority tasks poll via nextIdx incrementing
func TestIntelligentQueue_HighPriorityBlockingBehavior(t *testing.T) {
	fw := fake.NewFakeHandle(map[string]string{})

	q := &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			Annotations: map[string]string{
				queuepolicies.PriorityThresholdAnnotationKey: "4",
			},
		},
		Spec: v1alpha1.QueueSpec{
			QueuePolicy: "Intelligent",
		},
	}

	// Create initial tasks
	highTask := makeTestQueueUnit("default", "high-job", 5)

	lowTask1 := makeTestQueueUnit("default", "low-job-1", 2)
	time.Sleep(time.Millisecond * 5)
	lowTask2 := makeTestQueueUnit("default", "low-job-2", 2)
	time.Sleep(time.Millisecond * 5)
	lowTask3 := makeTestQueueUnit("default", "low-job-3", 2)

	// Create queue with initial items
	iq := NewIntelligentQueue("test", q, fw, nil, nil, nil, highTask, lowTask1, lowTask2, lowTask3).(*IntelligentQueue)

	assert.Equal(t, 1, len(iq.highPriorityQueue), "Should have 1 high priority task")
	assert.Equal(t, 3, len(iq.lowPriorityQueue), "Should have 3 low priority tasks")

	// Step 1: Test high priority blocking behavior
	// Directly verify queue contents
	assert.NotNil(t, iq.highPriorityQueue[0], "First high priority task should exist")
	assert.Equal(t, "high-job", iq.highPriorityQueue[0].Unit.Name)
	assert.Equal(t, int32(0), iq.highNextIdx, "highNextIdx should start at 0")

	// Simulate failure by calling AddUnschedulableIfNotPresent
	highTaskCopy := iq.highPriorityQueue[0]
	iq.AddUnschedulableIfNotPresent(context.Background(), highTaskCopy, false)

	// Step 2: Verify that highNextIdx did NOT increment (blocking behavior)
	assert.Equal(t, int32(0), iq.highNextIdx, "highNextIdx should NOT increment after high priority task failure (blocking behavior)")

	// Step 3: Delete high priority task to enable low priority polling
	iq.Delete(highTask.Unit)
	iq.ResetNextIdxFlag = true // Force reset

	// Step 4: Test low priority polling behavior
	// After reset, lowNextIdx should be 0
	assert.Equal(t, int32(0), iq.lowNextIdx, "lowNextIdx should be 0 after reset")

	polledTasks := make([]string, 0)

	for i := 0; i < 3; i++ {
		// Directly check the low priority queue
		if int(iq.lowNextIdx) < len(iq.lowPriorityQueue) {
			qu := iq.lowPriorityQueue[iq.lowNextIdx]
			polledTasks = append(polledTasks, qu.Unit.Name)

			// Simulate failure
			iq.AddUnschedulableIfNotPresent(context.Background(), qu, false)

			// Step 5: Verify that lowNextIdx DID increment (polling behavior)
			expectedIdx := int32(i + 1)
			assert.Equal(t, expectedIdx, iq.lowNextIdx,
				fmt.Sprintf("lowNextIdx should increment to %d after iteration %d", expectedIdx, i+1))
		}
	}

	// Step 6: Verify all tasks were polled
	assert.Equal(t, 3, len(polledTasks), "Should have polled 3 low priority tasks")
	assert.Equal(t, []string{"low-job-1", "low-job-2", "low-job-3"}, polledTasks,
		"Low priority tasks should be polled in FIFO order by timestamp")
}
