package elasticquotav1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	queuev1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/utils"
)

// Controller is a controller that update queue crd
type Controller struct {
	plugin *ElasticQuota
}

func NewQueueController(plugin *ElasticQuota) *Controller {
	ctrl := &Controller{
		plugin: plugin,
	}
	return ctrl
}

func (ctrl *Controller) Start() {
	klog.Infof("Start sync queue status")
	go wait.Until(ctrl.syncQueueStatusWorker, 1*time.Second, context.TODO().Done())
}

func (ctrl *Controller) syncQueueStatusWorker() {
	queues, err := ctrl.plugin.handle.QueueInformerFactory().Scheduling().V1alpha1().Queues().
		Lister().Queues("kube-queue").List(labels.Everything())
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to list queues in syncQueueStatusWorker")
		return
	}
	summaries := ctrl.plugin.GetDebugInfoInternal(false)
	for _, queue := range queues {
		if summaries[queue.Name] == nil {
			klog.Warningf("failed get queue summary for queue %v", queue.Name)
			continue
		}
		ctrl.syncQueueStatus(queue, summaries[queue.Name])
	}

	return
}

func (ctrl *Controller) syncQueueStatus(q *queuev1.Queue, summary *ElasticQuotaDebugInfo) {
	newQueue, err := updateQueueStatusIfChanged(q, summary, klog.V(5).Enabled())
	if err != nil {
		klog.ErrorS(err, "failed to updateQueueStatusIfChanged", "queue", q.Name)
		return
	}
	if newQueue == nil {
		if klog.V(5).Enabled() {
			klog.InfoS("Skip updating queue because there are no changes", "queue", q.Name)
		}
		return
	}

	if klog.V(5).Enabled() {
		klog.InfoS("Try updating queue since it has changed", "queue", q.Name)
	}

	err = framework.RetryTooManyRequests(func() error {
		_, err := ctrl.plugin.handle.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").
			Update(context.TODO(), newQueue, metav1.UpdateOptions{})
		return err
	})

	if err != nil {
		klog.ErrorS(err, "Failed to patch queue", "queue", q.Name)
	} else {
		if klog.V(5).Enabled() {
			klog.InfoS("Successfully patch queue", "queue", q.Name)
		}
	}
}

type traceChange struct {
	key      string
	original map[v1.ResourceName]int64
	current  map[v1.ResourceName]int64
}

func updateQueueStatusIfChanged(q *queuev1.Queue, summary *ElasticQuotaDebugInfo, logChanges bool) (*queuev1.Queue, error) {
	m := map[string]map[v1.ResourceName]int64{
		utils.QueueUsed:                   summary.Used,
		utils.QueueGuaranteedUsed:         summary.GuaranteedUsed,
		utils.QueueSelfUsed:               summary.SelfUsed,
		utils.QueueSelfGuaranteedUsed:     summary.SelfGuaranteedUsed,
		utils.QueueChildrenUsed:           summary.ChildrenUsed,
		utils.QueueChildrenGuaranteedUsed: summary.ChildrenGuaranteedUsed,
	}

	var newQueue *queuev1.Queue
	var changes []traceChange

	for k, v := range m {
		diff, original, err := isQueueAnnotationDiff(q, k, v)
		if err != nil {
			return nil, err
		}
		if !diff {
			continue
		}

		if logChanges {
			changes = append(changes, traceChange{key: k, original: original, current: v})
		}

		if newQueue == nil {
			newQueue = q.DeepCopy()
			if newQueue.Annotations == nil {
				newQueue.Annotations = map[string]string{}
			}
		}

		if err := updateQueueAnnotation(newQueue, k, v); err != nil {
			return nil, err
		}
	}

	if logChanges && len(changes) > 0 {
		sb := strings.Builder{}
		for i, v := range changes {
			if i > 0 {
				fmt.Fprintf(&sb, ", ")
			}
			fmt.Fprintf(&sb, "%s changed from %v to %v", v.key, printResourceList(v.original), printResourceList(v.current))
		}
		klog.InfoS("Queue changed", "queue", q.Name, "changed", sb.String())
	}

	return newQueue, nil
}

func isQueueAnnotationDiff(q *queuev1.Queue, key string, res map[v1.ResourceName]int64) (bool, map[v1.ResourceName]int64, error) {
	var originalResourceList map[v1.ResourceName]int64
	if val := q.Annotations[key]; val != "" {
		if err := json.Unmarshal([]byte(val), &originalResourceList); err != nil {
			return false, nil, err
		}
	}
	changed := !Equals(RemoveZeros(originalResourceList), RemoveZeros(res))
	return changed, originalResourceList, nil
}

func updateQueueAnnotation(q *queuev1.Queue, k string, res map[v1.ResourceName]int64) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	q.Annotations[k] = string(data)
	return nil
}

func printResourceList(rl map[v1.ResourceName]int64) string {
	if len(rl) == 0 {
		return "<empty>"
	}
	res := make([]string, 0)
	for k, v := range rl {
		tmp := string(k) + ":" + fmt.Sprintf("%d", v)
		res = append(res, tmp)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	return strings.Join(res, ",")
}

func Equals(a map[v1.ResourceName]int64, b map[v1.ResourceName]int64) bool {
	if len(a) != len(b) {
		return false
	}

	for k, value1 := range a {
		value2, found := b[k]
		if !found {
			return false
		}
		if value1 != value2 {
			return false
		}
	}

	return true
}

// RemoveZeros returns a new resource list that only has no zero values
func RemoveZeros(a map[v1.ResourceName]int64) map[v1.ResourceName]int64 {
	result := map[v1.ResourceName]int64{}
	for k, value := range a {
		if value != 0 {
			result[k] = value
		}
	}
	return result
}
