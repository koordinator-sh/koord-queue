package intelligentqueue

import (
	"context"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/test/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestIntelligentQueueAdmissionChangeNotification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas int64
		released bool
	}{
		{name: "admission_added", replicas: 2},
		{name: "admission_added_and_replicas_reduced", replicas: 1, released: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newUnit := func(name string, created int64) *framework.QueueUnitInfo {
				unit := makeTestQueueUnit("default", name, 1).Unit
				unit.UID = types.UID(t.Name() + "/" + name)
				unit.ResourceVersion = "1"
				unit.CreationTimestamp = metav1.NewTime(time.Unix(created, 0))
				return framework.NewQueueUnitInfo(unit)
			}
			older, newer, partial := newUnit("older", 1), newUnit("newer", 2), newUnit("partial", 3)
			partial.Unit.Spec.PodSets[0].Count = 4
			partial.Unit.Status.Phase = v1alpha1.Dequeued
			partial.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "test-pod-set", Replicas: 2}}
			handle := fake.NewFakeHandle(map[string]string{older.Name: "quota", newer.Name: "quota", partial.Name: "quota"})
			q := NewIntelligentQueue("test", &v1alpha1.Queue{}, handle, nil, nil, nil).(*IntelligentQueue)
			for _, unit := range []*framework.QueueUnitInfo{older, newer, partial} {
				if added, err := q.AddQueueUnitInfo(unit); err != nil || !added {
					t.Fatalf("enqueue %s: added=%t err=%v", unit.Name, added, err)
				}
			}
			ctx := context.Background()
			first, err := q.Next(ctx)
			if err != nil || first == nil || first.Name != older.Name {
				t.Fatalf("first candidate must be A: got=%v err=%v", first, err)
			}
			if err := q.AddUnschedulableIfNotPresent(ctx, first, false); err != nil {
				t.Fatal(err)
			}
			updated := partial.Unit.DeepCopy()
			updated.ResourceVersion = "2"
			updated.Status.Admissions = []v1alpha1.Admission{{Name: "test-pod-set", Replicas: tc.replicas}, {Name: "extra", Replicas: 1}}
			if err := q.Update(partial.Unit, updated); err != nil {
				t.Fatal(err)
			}
			_, blocked := q.BlockedQuota.Load("quota")
			if blocked == tc.released || q.ResetNextIdxFlag != tc.released {
				t.Fatalf("released=%t: quota blocked=%t, rewind=%t", tc.released, blocked, q.ResetNextIdxFlag)
			}
			if !tc.released {
				next, err := q.Next(ctx)
				if err != nil || next == nil || next.Name != newer.Name {
					t.Fatalf("admission growth must not restart scanning at A: next=%v err=%v", next, err)
				}
			}
		})
	}
}
