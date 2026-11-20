package schedulingqueuev2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/test/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newResourceReleaseTestQueue() *PriorityQueue {
	// Initialize synchronously to exclude background queue maintenance.
	q := &PriorityQueue{
		name: "test-queue", maxDepth: -1, lastResetTime: time.Now(),
		queueCr:    &v1alpha1.Queue{Spec: v1alpha1.QueueSpec{QueuePolicy: "Priority"}},
		queueUnits: make(map[string]*framework.QueueUnitInfo),
		assumed:    make(map[string]struct{}),
		updating:   make(map[string]struct{}),
		lessFunc:   less,
		fw:         fake.NewFakeHandle(nil),
	}
	q.cond = sync.NewCond(&q.lock)
	return q
}

func newResourceReleaseTestUnit(t *testing.T, name string, created int64) *framework.QueueUnitInfo {
	t.Helper()
	// All jobs request the same one CPU and 1 GiB. Use a distinct UID
	// so the framework initializes the creation time for this fixture.
	unit := makeTestQueueUnit("default", name, 1000900).Unit.DeepCopy()
	unit.UID = types.UID(t.Name() + "/" + name)
	unit.CreationTimestamp = metav1.NewTime(time.Unix(created, 0))
	return framework.NewQueueUnitInfo(unit)
}

// TestPriorityQueueRetryAfterResourceRelease fixes the event order explicitly:
// A fails admission, C releases resources, then the queue chooses again. The
// controller updates quota accounting before notifying the queue; this test
// exercises that subsequent notification, not the quota plugin or informer.
// No goroutines, sleeps, or direct cursor changes are needed to reproduce it.
func TestPriorityQueueRetryAfterResourceRelease(t *testing.T) {
	for _, tc := range []struct {
		name       string
		phase      v1alpha1.QueueUnitPhase
		delete     bool
		retryOlder bool
	}{
		{name: "completed", phase: v1alpha1.Succeed, retryOlder: true},
		{name: "failed", phase: v1alpha1.Failed, retryOlder: true},
		{name: "deleted", delete: true, retryOlder: true},
		{name: "status_only", phase: v1alpha1.Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newResourceReleaseTestQueue()
			// Names sort in the opposite order: creation time must put A first.
			older := newResourceReleaseTestUnit(t, "z-older", 1)
			newer := newResourceReleaseTestUnit(t, "a-newer", 2)
			running := newResourceReleaseTestUnit(t, "running", 0)
			running.Unit.Status.Phase = v1alpha1.Running
			running.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 1, Running: 1}}
			if added, err := q.AddQueueUnitInfo(running); err != nil || added {
				t.Fatalf("fully running C must be outside the pending queue: added=%t err=%v", added, err)
			}
			for _, unit := range []*framework.QueueUnitInfo{newer, older} {
				if added, err := q.AddQueueUnitInfo(unit); err != nil || !added {
					t.Fatalf("enqueue %s: added=%t err=%v", unit.Name, added, err)
				}
			}
			ctx := context.Background()
			first, err := q.Next(ctx)
			if err != nil || first == nil || first.Name != older.Name {
				t.Fatalf("first candidate must be older A: got=%v err=%v", first, err)
			}
			// Simulate A's failed quota check. There was no resource change
			// during this attempt, so Priority may continue scanning afterward.
			if err := q.AddUnschedulableIfNotPresent(ctx, first, false); err != nil {
				t.Fatal(err)
			}

			// C's event arrives before the next selection. Completion/failure
			// clears admissions; a status-only update releases nothing.
			updated := running.Unit.DeepCopy()
			if tc.delete {
				if err := q.Delete(running.Unit); err != nil {
					t.Fatal(err)
				}
			} else {
				updated.ResourceVersion = "2"
				updated.Status.Phase = tc.phase
				updated.Status.Message = "status updated"
				if tc.retryOlder {
					updated.Status.Admissions = nil
				}
				if err := q.Update(running.Unit, updated); err != nil {
					t.Fatal(err)
				}
			}

			want := newer.Name
			if tc.retryOlder {
				want = older.Name
			}
			next, err := q.Next(ctx)
			if err != nil || next == nil {
				t.Fatalf("next candidate: got=%v err=%v", next, err)
			}
			if next.Name != want {
				t.Fatalf("after C's %s event, next candidate = %s, want %s", tc.name, next.Name, want)
			}
			if tc.retryOlder {
				// If C released too little quota for A, scanning must still reach B.
				// A later status-only event on completed C must not restart the scan.
				if err := q.AddUnschedulableIfNotPresent(ctx, next, false); err != nil {
					t.Fatal(err)
				}
				if !tc.delete {
					statusOnly := updated.DeepCopy()
					statusOnly.ResourceVersion = "3"
					statusOnly.Status.Message = "another status update"
					if err := q.Update(updated, statusOnly); err != nil {
						t.Fatal(err)
					}
				}
				next, err = q.Next(ctx)
				if err != nil || next == nil || next.Name != newer.Name {
					t.Fatalf("after A fails again, scanning must continue to B: got=%v err=%v", next, err)
				}
			}
		})
	}
}

func TestResourceReleaseWakesBlockedQuota(t *testing.T) {
	q := newResourceReleaseTestQueue()
	q.blocked = true
	q.queueCr.Spec.QueuePolicy = "Block"
	waiting := newResourceReleaseTestUnit(t, "waiting", 1)
	running := newResourceReleaseTestUnit(t, "running", 0)
	running.Unit.Status.Phase = v1alpha1.Running
	running.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 1, Running: 1}}
	q.fw = fake.NewFakeHandle(map[string]string{waiting.Name: "shared-quota", running.Name: "shared-quota"})
	if added, err := q.AddQueueUnitInfo(waiting); err != nil || !added {
		t.Fatalf("enqueue waiting job: added=%t err=%v", added, err)
	}
	if added, err := q.AddQueueUnitInfo(running); err != nil || added {
		t.Fatalf("running job must be outside the pending queue: added=%t err=%v", added, err)
	}
	ctx := context.Background()
	first, err := q.Next(ctx)
	if err != nil || first == nil || first.Name != waiting.Name {
		t.Fatalf("first candidate: got=%v err=%v", first, err)
	}
	if err := q.AddUnschedulableIfNotPresent(ctx, first, false); err != nil {
		t.Fatal(err)
	}
	q.lock.Lock()
	_, candidate := q.findNextQueueUnit()
	q.lock.Unlock()
	if candidate != nil {
		t.Fatal("the failed quota must block its waiting job")
	}

	// Wait under the queue lock so Update cannot broadcast before Wait begins.
	// Observe the notification directly, without Next's throttling timers.
	ready, woken := make(chan struct{}), make(chan struct{})
	go func() {
		q.lock.Lock()
		close(ready)
		q.cond.Wait()
		q.lock.Unlock()
		close(woken)
	}()
	<-ready
	t.Cleanup(func() {
		q.lock.Lock()
		q.cond.Broadcast()
		q.lock.Unlock()
		<-woken
	})
	completed := running.Unit.DeepCopy()
	completed.ResourceVersion = "2"
	completed.Status.Phase = v1alpha1.Succeed
	completed.Status.Admissions = nil
	if err := q.Update(running.Unit, completed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-woken:
	case <-time.After(5 * time.Second):
		t.Fatal("resource release did not wake the blocked queue")
	}
	q.lock.Lock()
	_, candidate = q.findNextQueueUnit()
	q.lock.Unlock()
	if candidate == nil || candidate.Name != waiting.Name {
		t.Fatalf("resource release must unblock the quota and retry its waiting job: got=%v", candidate)
	}
}

func TestResourceReleasePreservesSchedFailedScan(t *testing.T) {
	q := newResourceReleaseTestQueue()
	older := newResourceReleaseTestUnit(t, "z-older", 1)
	newer := newResourceReleaseTestUnit(t, "a-newer", 2)
	// A has reserved quota but its Pod has not started. Priority can scan on to B.
	older.Unit.Status.Phase = v1alpha1.Reserved
	older.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 1}}
	for _, unit := range []*framework.QueueUnitInfo{older, newer} {
		if added, err := q.AddQueueUnitInfo(unit); err != nil || !added {
			t.Fatalf("enqueue %s: added=%t err=%v", unit.Name, added, err)
		}
	}
	ctx := context.Background()
	first, err := q.Next(ctx)
	if err != nil || first == nil || first.Name != newer.Name {
		t.Fatalf("reserved A must be skipped: got=%v err=%v", first, err)
	}
	failed := older.Unit.DeepCopy()
	failed.ResourceVersion = "2"
	failed.Status.Phase = v1alpha1.SchedFailed
	failed.Status.Admissions = nil
	if err := q.Update(older.Unit, failed); err != nil {
		t.Fatal(err)
	}
	next, err := q.Next(ctx)
	if err != nil || next == nil || next.Name != newer.Name {
		t.Fatalf("SchedFailed must not restart the scan at A: got=%v err=%v", next, err)
	}
}

func TestResourceReleaseForDequeuedUnit(t *testing.T) {
	ad := func(name string, replicas int64) v1alpha1.Admission {
		return v1alpha1.Admission{Name: name, Replicas: replicas, Running: replicas}
	}
	for _, policy := range []string{"Priority", "Block"} {
		t.Run(policy, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				old, new []v1alpha1.Admission
				released bool
			}{
				{
					name: "admission_added",
					old:  []v1alpha1.Admission{ad("default", 1)},
					new:  []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
				},
				{
					name: "first_admission_reported",
					new:  []v1alpha1.Admission{ad("default", 1)},
				},
				{
					name: "replicas_increased",
					old:  []v1alpha1.Admission{ad("default", 1)},
					new:  []v1alpha1.Admission{ad("default", 2)},
				},
				{
					name: "admissions_reordered",
					old:  []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
					new:  []v1alpha1.Admission{ad("extra", 1), ad("default", 1)},
				},
				{
					name:     "admission_removed",
					old:      []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
					new:      []v1alpha1.Admission{ad("default", 1)},
					released: true,
				},
				{
					name:     "replicas_reduced",
					old:      []v1alpha1.Admission{ad("default", 2)},
					new:      []v1alpha1.Admission{ad("default", 1)},
					released: true,
				},
				{
					name:     "admission_replaced",
					old:      []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
					new:      []v1alpha1.Admission{ad("default", 1), ad("replacement", 1)},
					released: true,
				},
				{
					name:     "admission_added_and_replicas_reduced",
					old:      []v1alpha1.Admission{ad("default", 2)},
					new:      []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
					released: true,
				},
				{
					name:     "more_admissions_but_old_entry_removed",
					old:      []v1alpha1.Admission{ad("default", 1), ad("extra", 1)},
					new:      []v1alpha1.Admission{ad("default", 1), ad("replacement", 1), ad("another", 1)},
					released: true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					q := newResourceReleaseTestQueue()
					q.blocked = policy == "Block"
					q.queueCr.Spec.QueuePolicy = v1alpha1.QueuePolicy(policy)
					older := newResourceReleaseTestUnit(t, "older", 1)
					newer := newResourceReleaseTestUnit(t, "newer", 2)
					running := newResourceReleaseTestUnit(t, "running", 0)
					running.Unit.Status.Phase = v1alpha1.Running
					running.Unit.Status.Admissions = tc.old
					q.fw = fake.NewFakeHandle(map[string]string{
						older.Name: "shared-quota", newer.Name: "shared-quota", running.Name: "shared-quota",
					})
					if added, err := q.AddQueueUnitInfo(running); err != nil || added {
						t.Fatalf("running C must be outside the pending queue: added=%t err=%v", added, err)
					}
					for _, unit := range []*framework.QueueUnitInfo{older, newer} {
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
					q.blockedQuota.Store("unrelated-quota", 1)
					updated := running.Unit.DeepCopy()
					updated.ResourceVersion = "2"
					updated.Status.Admissions = tc.new
					if err := q.Update(running.Unit, updated); err != nil {
						t.Fatal(err)
					}
					if len(q.List()) != 2 {
						t.Fatal("running C must remain outside the pending queue after Update")
					}
					// Inspect eligibility directly so a correctly blocked queue does
					// not wait in Next's throttling loop.
					q.lock.Lock()
					_, next := q.findNextQueueUnit()
					q.lock.Unlock()
					if _, blocked := q.blockedQuota.Load("unrelated-quota"); !blocked {
						t.Fatal("C's update must not unblock an unrelated quota")
					}
					if !tc.released && q.blocked {
						if next != nil {
							t.Fatalf("no resource release: shared quota must stay blocked, got=%s", next.Name)
						}
						return
					}
					want := newer.Name
					if tc.released {
						want = older.Name
					}
					if next == nil || next.Name != want {
						t.Fatalf("released=%t: next=%v, want=%s", tc.released, next, want)
					}
				})
			}
		})
	}
}

func TestResourceReleaseBeforeFailureCallback(t *testing.T) {
	q := newResourceReleaseTestQueue()
	older := newResourceReleaseTestUnit(t, "older", 1)
	newer := newResourceReleaseTestUnit(t, "newer", 2)
	running := newResourceReleaseTestUnit(t, "running", 0)
	running.Unit.Status.Phase = v1alpha1.Running
	running.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 1, Running: 1}}
	for _, unit := range []*framework.QueueUnitInfo{older, newer} {
		if added, err := q.AddQueueUnitInfo(unit); err != nil || !added {
			t.Fatalf("enqueue %s: added=%t err=%v", unit.Name, added, err)
		}
	}
	ctx := context.Background()
	first, err := q.Next(ctx)
	if err != nil || first == nil || first.Name != older.Name {
		t.Fatalf("first candidate must be A: got=%v err=%v", first, err)
	}
	completed := running.Unit.DeepCopy()
	completed.ResourceVersion = "2"
	completed.Status.Phase = v1alpha1.Succeed
	completed.Status.Admissions = nil
	if err := q.Update(running.Unit, completed); err != nil {
		t.Fatal(err)
	}
	if err := q.AddUnschedulableIfNotPresent(ctx, first, false); err != nil {
		t.Fatal(err)
	}
	next, err := q.Next(ctx)
	if err != nil || next == nil || next.Name != older.Name {
		t.Fatalf("failure callback must preserve the resource-release reset: next=%v err=%v", next, err)
	}
}

func TestResourceReleaseForPendingUnit(t *testing.T) {
	for _, policy := range []string{"Priority", "Block"} {
		for _, tc := range []struct {
			name     string
			replicas int64
			released bool
		}{
			{name: "admission_added", replicas: 2},
			{name: "admission_added_and_replicas_reduced", replicas: 1, released: true},
		} {
			t.Run(policy+"/"+tc.name, func(t *testing.T) {
				q := newResourceReleaseTestQueue()
				q.blocked = policy == "Block"
				q.queueCr.Spec.QueuePolicy = v1alpha1.QueuePolicy(policy)
				older := newResourceReleaseTestUnit(t, "older", 1)
				newer := newResourceReleaseTestUnit(t, "newer", 2)
				partial := newResourceReleaseTestUnit(t, "partial", 3)
				partial.Unit.Spec.PodSets[0].Count = 4
				partial.Unit.Status.Phase = v1alpha1.Dequeued
				partial.Unit.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: 2}}
				q.fw = fake.NewFakeHandle(map[string]string{older.Name: "quota", newer.Name: "quota", partial.Name: "quota"})
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
				updated.Status.Admissions = []v1alpha1.Admission{{Name: "default", Replicas: tc.replicas}, {Name: "extra", Replicas: 1}}
				if err := q.Update(partial.Unit, updated); err != nil {
					t.Fatal(err)
				}
				q.lock.Lock()
				_, next := q.findNextQueueUnit()
				q.lock.Unlock()
				if q.blocked && !tc.released {
					if next != nil {
						t.Fatalf("admission growth must not unblock the quota: got=%v", next)
					}
					return
				}
				want := newer.Name
				if tc.released {
					want = older.Name
				}
				if next == nil || next.Name != want {
					t.Fatalf("released=%t: next=%v, want=%s", tc.released, next, want)
				}
			})
		}
	}
}
