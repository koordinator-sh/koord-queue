package intelligentqueue

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/basequeue"
	"github.com/koordinator-sh/koord-queue/pkg/test/fake"
)

// TestNewIntelligentQueueAppliesMaxDepth guards the max-depth annotation on the Intelligent
// policy. MaxDepth is read by SortedList to bound how deep the high and low priority queues
// are scanned, so a value that never reaches the embedded base queue silently disables the
// limit.
func TestNewIntelligentQueueAppliesMaxDepth(t *testing.T) {
	build := func(annotations map[string]string) *IntelligentQueue {
		return NewIntelligentQueue("test",
			&v1alpha1.Queue{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: annotations}},
			fake.NewFakeHandle(map[string]string{}), nil, nil, nil).(*IntelligentQueue)
	}

	t.Run("annotation reaches the embedded base queue", func(t *testing.T) {
		iq := build(map[string]string{basequeue.MaxDepthAnnotation: "6"})
		if iq.BaseQueue == nil {
			t.Fatal("expected an embedded base queue")
		}
		if iq.BaseQueue.MaxDepth != 6 {
			t.Errorf("MaxDepth = %v, want 6", iq.BaseQueue.MaxDepth)
		}
	})

	t.Run("absent annotation keeps the default", func(t *testing.T) {
		iq := build(nil)
		if iq.BaseQueue == nil {
			t.Fatal("expected an embedded base queue")
		}
		if iq.BaseQueue.MaxDepth != -1 {
			t.Errorf("MaxDepth = %v, want the -1 default", iq.BaseQueue.MaxDepth)
		}
	})

	t.Run("unparsable value keeps the default", func(t *testing.T) {
		iq := build(map[string]string{basequeue.MaxDepthAnnotation: "not-a-number"})
		if iq.BaseQueue.MaxDepth != -1 {
			t.Errorf("MaxDepth = %v, want the -1 default", iq.BaseQueue.MaxDepth)
		}
	})

	t.Run("zero and negative values are preserved", func(t *testing.T) {
		// SortedList treats MaxDepth <= 0 as "unbounded", so these must survive parsing
		// rather than being mistaken for the -1 default.
		for _, tc := range []struct {
			value string
			want  int32
		}{{"0", 0}, {"-5", -5}} {
			iq := build(map[string]string{basequeue.MaxDepthAnnotation: tc.value})
			if iq.BaseQueue.MaxDepth != tc.want {
				t.Errorf("MaxDepth for %q = %v, want %v", tc.value, iq.BaseQueue.MaxDepth, tc.want)
			}
		}
	})
}
