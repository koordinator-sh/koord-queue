package schedulingqueuev2

import (
	"slices"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/test/fake"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

func TestFindQueueUnitsToPreempt(t *testing.T) {
	now := time.Now()
	highPrio := &framework.QueueUnitInfo{
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "high-priority-unit",
				Namespace: "default",
				UID:       "high-priority-unit",
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: ptr.To(int32(100)),
				Resource: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("4"),
					v1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
		},
	}
	mid := &framework.QueueUnitInfo{
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mid-priority-unit",
				Namespace: "default",
				UID:       "mid-priority-unit",
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: ptr.To(int32(10)),
				Resource: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		},
	}
	lowPrio := &framework.QueueUnitInfo{
		InitialAttemptTimestamp: now,
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "low-priority-unit",
				Namespace: "default",
				UID:       "low-priority-unit-uid",
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: ptr.To(int32(1)),
				Resource: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		},
	}
	lowPrio2 := &framework.QueueUnitInfo{
		InitialAttemptTimestamp: now.Add(time.Second),
		Unit: &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "low2-priority-unit",
				Namespace: "default",
				UID:       "low2-priority-unit",
			},
			Spec: v1alpha1.QueueUnitSpec{
				Priority: ptr.To(int32(1)),
				Resource: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		},
	}
	quToQuotas := map[string]string{
		klog.KObj(highPrio.Unit).String(): "quota1",
		klog.KObj(mid.Unit).String():      "quota2",
		klog.KObj(lowPrio.Unit).String():  "quota3",
		klog.KObj(lowPrio2.Unit).String(): "quota3",
	}

	tests := []struct {
		name           string
		queue          []*framework.QueueUnitInfo
		assumed        []*framework.QueueUnitInfo
		blockedQuota   []string
		expectedResult []string
	}{
		{
			name:           "Test case 1: Preemption required",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio},
			assumed:        []*framework.QueueUnitInfo{lowPrio},
			expectedResult: []string{"default/low-priority-unit"},
		},
		{
			name:           "Test case 2: No preemption required",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio},
			assumed:        []*framework.QueueUnitInfo{highPrio},
			expectedResult: nil,
		},
		{
			name:           "Test case 3: preempt multiple queue units",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio, lowPrio2},
			assumed:        []*framework.QueueUnitInfo{lowPrio, lowPrio2},
			expectedResult: []string{"default/low-priority-unit", "default/low2-priority-unit"},
		},
		{
			name:           "Test case 4: preempt mid queue units",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio, lowPrio2},
			assumed:        []*framework.QueueUnitInfo{mid, lowPrio},
			expectedResult: []string{"default/mid-priority-unit", "default/low-priority-unit"},
		},
		{
			name:           "Test case 5: preempt some assumed queue units",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio, lowPrio2},
			assumed:        []*framework.QueueUnitInfo{highPrio, lowPrio, lowPrio2},
			expectedResult: []string{"default/low2-priority-unit"},
		},
		{
			name:           "Test case 6: preempt some assumed queue units",
			queue:          []*framework.QueueUnitInfo{highPrio, mid, lowPrio, lowPrio2},
			assumed:        []*framework.QueueUnitInfo{highPrio, lowPrio2},
			expectedResult: []string{"default/low2-priority-unit"},
		},
		{
			name:           "Test case 7: queueUnits in blocked quota cannot preempt other queueUnits",
			queue:          []*framework.QueueUnitInfo{highPrio},
			assumed:        []*framework.QueueUnitInfo{mid, lowPrio, lowPrio2},
			expectedResult: []string{},
			blockedQuota:   []string{"quota1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := &PriorityQueue{
				queue:      test.queue,
				queueUnits: map[string]*framework.QueueUnitInfo{},
				assumed:    make(map[string]struct{}),
				name:       "test-queue",
				lessFunc:   less,
				fw:         fake.NewFakeHandle(quToQuotas),
			}
			if len(test.blockedQuota) > 0 {
				q.blocked = true
				for _, quota := range test.blockedQuota {
					q.blockedQuota.Store(quota, 1)
				}
			}

			// Populate assumed map
			for _, unit := range test.assumed {
				key := unit.Unit.Namespace + "/" + unit.Unit.Name
				q.assumed[key] = struct{}{}
			}
			for _, unit := range test.queue {
				key := unit.Unit.Namespace + "/" + unit.Unit.Name
				q.queueUnits[key] = unit
			}

			preemptedQueueUnitKeys, _ := q.findQueueUnitsToPreempt(test.assumed)

			if len(preemptedQueueUnitKeys) != len(test.expectedResult) {
				t.Errorf("Expected %d preempted units, got %d", len(test.expectedResult), len(preemptedQueueUnitKeys))
			} else {
				slices.Sort(preemptedQueueUnitKeys)
				slices.Sort(test.expectedResult)
				for i := 0; i < len(preemptedQueueUnitKeys); i++ {
					if preemptedQueueUnitKeys[i] != test.expectedResult[i] {
						t.Errorf("Expected key %s, got %s", test.expectedResult[i], preemptedQueueUnitKeys[i])
					}
				}
			}
		})
	}
}
