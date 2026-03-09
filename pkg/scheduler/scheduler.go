/*
 Copyright 2021 The Kube-Queue Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/metrics"
	"github.com/kube-queue/kube-queue/pkg/queue"
	"github.com/kube-queue/kube-queue/pkg/utils"
)

type Scheduler struct {
	multiSchedulingQueue queue.MultiSchedulingQueue
	fw                   framework.Framework
	QueueClient          versioned.Interface
	recorder             record.EventRecorder

	dequeuing int
	condMutex sync.Mutex
	cond      *sync.Cond

	enableStrictConsistency         bool
	finished                        int64
	scheduleSuspendTimeInMillSecond int64
	enableParentLimit               bool
	enableStrictDequeueMode         bool
	queueList                       sets.Set[string]
	queueListNotIn                  sets.Set[string]

	lastScheduledPriority int
}

func NewScheduler(multiSchedulingQueue queue.MultiSchedulingQueue, fw framework.Framework, queueClient versioned.Interface,
	recorder record.EventRecorder, enableStrictConsistency, enableStrictDequeueMode, enableParentLimit bool, scheduleSuspendTimeInMillSecond int64,
	quotas string) (*Scheduler, error) {

	quotalistopt := strings.Split(quotas, ",")
	ql := sets.New[string]()
	qln := sets.New[string]()
	for _, quota := range quotalistopt {
		if quota == "" {
			continue
		}
		if strings.HasPrefix(quota, "!") {
			qln.Insert(strings.TrimPrefix(quota, "!"))
		} else {
			ql.Insert(quota)
		}
	}
	klog.V(0).Infof("responsible quota %v", ql.UnsortedList())
	klog.V(0).Infof("not responsible quota %v", qln.UnsortedList())
	sche := &Scheduler{
		multiSchedulingQueue:            multiSchedulingQueue,
		fw:                              fw,
		QueueClient:                     queueClient,
		recorder:                        recorder,
		enableStrictConsistency:         enableStrictConsistency,
		scheduleSuspendTimeInMillSecond: scheduleSuspendTimeInMillSecond,
		enableParentLimit:               enableParentLimit,
		enableStrictDequeueMode:         enableStrictDequeueMode,
		queueList:                       ql,
		queueListNotIn:                  qln,
	}
	sche.cond = sync.NewCond(&sche.condMutex)
	klog.Infof("finish init scheduler, scheduleSuspendTimeInMillSecond:%v, enableParentLimit:%v",
		sche.scheduleSuspendTimeInMillSecond, enableParentLimit)
	return sche, nil
}

func (s *Scheduler) Start(ctx context.Context) {
	s.multiSchedulingQueue.Start(ctx)
	s.fw.StartQueueUnitMappingPlugin(ctx)
	s.internalSchedule(ctx)
}

func (s *Scheduler) FinishedInc() {
	atomic.AddInt64(&s.finished, 1)
}

// Internal start scheduling
func (s *Scheduler) internalSchedule(ctx context.Context) {
	startedQueue := map[string]context.CancelFunc{}
	eventChan := s.multiSchedulingQueue.QueueChan()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			eventChan.L.Lock()
			s.multiSchedulingQueue.SortedQueue()
			existingQueue := map[string]struct{}{}
			for _, q := range s.multiSchedulingQueue.SortedQueue() {
				existingQueue[q.Name()] = struct{}{}
				if _, ok := startedQueue[q.Name()]; !ok {
					newCtx, cancel := context.WithCancel(ctx)
					startedQueue[q.Name()] = cancel
					go func(newCtx context.Context, q *queue.Queue) {
						klog.V(1).InfoS("Add a new queue", "queue", q.Name())
						for {
							select {
							case <-newCtx.Done():
								return
							default:
								s.schedule(newCtx, q)
							}
						}
					}(newCtx, q)
				}
			}
			for q := range startedQueue {
				if _, ok := existingQueue[q]; !ok {
					klog.V(1).InfoS("Remove a queue", "queue", q)
					startedQueue[q]()
					delete(startedQueue, q)
				}
			}
			eventChan.Wait()
			eventChan.L.Unlock()
		}
	}

}

func (s *Scheduler) HandleQueueNotFound(ctx context.Context, unitInfo *framework.QueueUnitInfo, expectQueueName string) {
	queue, ok := s.multiSchedulingQueue.GetQueueByName(expectQueueName)
	if ok {
		klog.Infof("add qu %v to queue %v", unitInfo.Name, expectQueueName)
		queue.AddQueueUnitInfo(unitInfo)
		return
	}
	klog.Errorf("queue %v is not exist when schedule qu %v", expectQueueName, unitInfo.Name)
	if unitInfo.Unit == nil {
		klog.Errorf("drop queueUnitInfo %v because the queueUnit is nil", unitInfo.Name)
		return
	}
	msg := fmt.Sprintf("queue %v is not exist", expectQueueName)
	qu := unitInfo.Unit.DeepCopy()
	qu.Status.Message = msg
	updatedUnit, err := s.QueueClient.SchedulingV1alpha1().QueueUnits(qu.Namespace).UpdateStatus(ctx, qu, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("update qu %v error %v", unitInfo.Name, err)
	} else {
		s.recorder.Event(updatedUnit, "Warning", "FailedScheduling", msg)
	}
	unitInfo.Unit = updatedUnit
	s.multiSchedulingQueue.AddUnitsFindNoQueue(unitInfo)
}

func (s *Scheduler) quotaListEnabled(logger logr.Logger, queue string) bool {
	if len(s.queueListNotIn) != 0 && s.queueListNotIn.Has(queue) {
		if rand.Intn(100) < 10 {
			logger.V(4).Info("queue is in queueListNotIn, skip", "queue", queue, "queueListNotIn", s.queueListNotIn)
		}
		return false
	}
	if len(s.queueList) != 0 && !s.queueList.Has(queue) {
		if rand.Intn(100) < 10 {
			logger.V(4).Info("queue is not in queueList, skip", "queue", queue, "queueList", s.queueList)
		}
		return false
	}
	if rand.Intn(100) < 10 {
		logger.V(4).Info("queuelist enabled for queue", "queue", queue, "queueList", s.queueList, "queueListNotIn", s.queueListNotIn)
	}
	return true
}

func (s *Scheduler) schedule(ctx context.Context, q *queue.Queue) {
	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		if s.scheduleSuspendTimeInMillSecond > 0 {
			time.Sleep(time.Millisecond * time.Duration(s.scheduleSuspendTimeInMillSecond))
		}
	}()

	finished := s.finished
	klog.Infof("-------------------------------------")
	magicNumber := rand.Int31()
	logger := klog.FromContext(schedulingCycleCtx).WithValues("queue", q.Name(), "scheduleNumber", magicNumber)
	ctx = logr.NewContext(ctx, logger)
	unitInfo, err := q.Next(ctx)
	// fix bug:
	// t0: q.Length() > 0 with only one item
	// t1: delete queueUnit
	// t2: here will panic with unitInfo.Unit, cause unitInfo is empty with q.Pop()
	if err != nil || unitInfo == nil {
		logger.Error(err, "failed to get queueUnit from queue")
		return
	}
	priority := 0
	if unitInfo.Unit.Spec.Priority != nil {
		priority = int(*unitInfo.Unit.Spec.Priority)
	}
	logger = logger.WithValues("finished", s.finished, "queueUnit", unitInfo.Unit.Namespace+"/"+unitInfo.Unit.Name, "queue", q.Name(), "priority", priority, "lastPriority", s.lastScheduledPriority)
	logger.V(2).Info("------------------ schedule begin 1------------------")
	if !s.enableStrictDequeueMode ||
		s.enableStrictDequeueMode && (!s.quotaListEnabled(logger, q.Name()) || priority >= s.lastScheduledPriority || q.WaitingForSchedulingLen() == 0) {
		s.lastScheduledPriority = priority

		startTime := time.Now()
		status := s.fw.RunFilterPlugins(schedulingCycleCtx, unitInfo)
		metrics.JobSchedulingAlgorithmLatency.Observe(float64(time.Since(startTime)) / float64(time.Millisecond))
		if status.Code() != framework.Success {
			err = q.Preempt(ctx, unitInfo)
			if err != nil {
				logger.Error(err, "failed to preempt queueUnit")
				status.AppendMessage("; preempt failed")
			} else {
				status.AppendMessage("; preempt succeed")
			}
			metrics.JobScheduleAttempts.WithLabelValues(q.Name(), "unschedulable").Add(1)
			s.ErrorFunc(ctx, unitInfo, q, status.Message(), finished, err == nil)
			logger.V(2).Info("------------------ schedule end ------------------")
			return
		}
		metrics.JobScheduleAttempts.WithLabelValues(q.Name(), "scheduled").Add(1)
		// in first version, we admit all requests everytime
		// maybe in later version we will support partial admit
		res := status.Admissions()
		status = s.fw.RunReservePluginsReserve(schedulingCycleCtx, unitInfo, res)
		if status.Code() != framework.Success {
			logger.Info("failed to reserve queueUnit", "reason", status.Message())
			s.fw.RunReservePluginsUnreserve(schedulingCycleCtx, unitInfo)
			s.ErrorFunc(ctx, unitInfo, q, status.Message(), finished, false)
			logger.V(2).Info("------------------ schedule end ------------------")
			return
		}

		copiedUnit := unitInfo.Unit.DeepCopy()
		copiedUnit.Status.Phase = v1alpha1.Dequeued
		if matched := MatchedAdmissionChecks(copiedUnit, q.GetAdmissionChecks()); len(matched) > 0 {
			copiedUnit.Status.Phase = v1alpha1.Reserved
		} else if s.enableStrictDequeueMode && s.quotaListEnabled(logger, q.Name()) {
			copiedUnit.Status.Phase = v1alpha1.SchedReady
		} else {
			copiedUnit.Status.Phase = v1alpha1.Dequeued
		}
		if err := q.Reserve(ctx, copiedUnit); err != nil {
			logger.Info("failed to reserve queueUnit in queue", "reason", err.Error())
			status.AppendMessage(err.Error())
			s.fw.RunReservePluginsUnreserve(schedulingCycleCtx, unitInfo)
			s.ErrorFunc(ctx, unitInfo, q, status.Message(), finished, false)
			logger.V(2).Info("------------------ schedule end ------------------")
			return
		}
		go func(currentQueue *queue.Queue, unitInfo *framework.QueueUnitInfo) {
			s.condMutex.Lock()
			for s.dequeuing >= 200 {
				s.cond.Wait()
			}
			s.dequeuing++
			s.condMutex.Unlock()

			s.handleDequeue(logger, unitInfo, ctx, schedulingCycleCtx, res, currentQueue)
			s.condMutex.Lock()
			s.dequeuing--
			s.condMutex.Unlock()
			s.cond.Signal()
		}(q, unitInfo)
		logger.V(2).Info("------------------ schedule end ------------------")
	} else {
		s.lastScheduledPriority = priority
		s.ErrorFunc(ctx, unitInfo, q, "job is block by others with high priority", s.finished, false)
	}
}

func (s *Scheduler) handleDequeue(logger logr.Logger, unitInfo *framework.QueueUnitInfo, ctx, schedulingCycleCtx context.Context, admissions map[string]framework.Admission, q *queue.Queue) bool {
	err := s.Dequeue(schedulingCycleCtx, logger, unitInfo.Unit, admissions, q)
	if err != nil {
		klog.Errorf("dequeue %v failed: %v", unitInfo.Name, err.Error())
		// 构建一个临时存储的位置
		s.fw.RunReservePluginsUnreserve(schedulingCycleCtx, unitInfo)
		s.ErrorFunc(ctx, unitInfo, q, err.Error(), 0, false)
		return false
	}
	klog.Infof("dequeue %v success", unitInfo.Name)
	return true
}

func (s *Scheduler) setAdmissionCheck(st *v1alpha1.QueueUnitStatus, admissionChecks sets.Set[string]) {
	now := metav1.Now()
	existingCheck := []string{}
	for _, existCheck := range st.AdmissionChecks {
		if !admissionChecks.Has(existCheck.Name) {
			existCheck.LastTransitionTime = now
			existCheck.Message = "AdmisssionCheck is removed from queue"
			existCheck.PodSetUpdates = make([]v1beta1.PodSetUpdate, 0)
			existCheck.State = v1beta1.CheckStateReady
			continue
		}
		existCheck.LastTransitionTime = now
		existCheck.Message = ""
		existCheck.PodSetUpdates = make([]v1beta1.PodSetUpdate, 0)
		existCheck.State = v1beta1.CheckStatePending
		existingCheck = append(existingCheck, existCheck.Name)
	}
	admissionChecks.Delete(existingCheck...)
	for newCheck := range admissionChecks {
		st.AdmissionChecks = append(st.AdmissionChecks, v1beta1.AdmissionCheckState{
			LastTransitionTime: now,
			Message:            "",
			PodSetUpdates:      make([]v1beta1.PodSetUpdate, 0),
			State:              v1beta1.CheckStatePending,
			Name:               newCheck,
		})
	}
}

func MatchedAdmissionChecks(queueUnit *v1alpha1.QueueUnit, admissionChecks map[string]labels.Selector) sets.Set[string] {
	matchAdmissionChecks := sets.New[string]()
	for k, ac := range admissionChecks {
		if ac.Matches(labels.Set(queueUnit.Labels)) {
			matchAdmissionChecks.Insert(k)
		}
	}
	return matchAdmissionChecks
}

func (s *Scheduler) Dequeue(schedulingCycleCtx context.Context, logger logr.Logger, queueUnit *v1alpha1.QueueUnit, admissions map[string]framework.Admission, qImpl *queue.Queue) error {
	newStatus := queueUnit.Status.DeepCopy()
	for _, schedAds := range admissions {
		name := schedAds.Name
		found := false
		for i, oldAd := range newStatus.Admissions {
			if oldAd.Name == name {
				oldAd.Replicas += schedAds.Replicas
				found = true
				newStatus.Admissions[i] = oldAd
				break
			}
		}
		if !found {
			newStatus.Admissions = append(newStatus.Admissions, v1alpha1.Admission{Name: name, Replicas: schedAds.Replicas})
		}
	}

	ad := qImpl.GetAdmissionChecks()
	if matched := MatchedAdmissionChecks(queueUnit, ad); len(matched) > 0 {
		// if strict dequeue mode is enabled and there are admission checks matched,
		// queueUnit controller will set queueUnit to SchedReady status after admissionChecks
		// are ready.
		newStatus.Phase = v1alpha1.Reserved
		newStatus.Message = "Reserved resources success, waiting admission checks to be ready"
		s.setAdmissionCheck(newStatus, matched)
	} else if s.enableStrictDequeueMode && s.quotaListEnabled(logger, qImpl.Name()) {
		gpuValue := queueUnit.Spec.Resource["nvidia.com/gpu"]
		if os.Getenv("SmallTaskAcclerate") == "true" && queueUnit.Spec.Request.Cpu().Value() <= 128 && gpuValue.Value() <= 8 {
			newStatus.Phase = v1alpha1.Dequeued
			newStatus.Message = "Dequeued because schedule successfully"
		} else {
			newStatus.Phase = v1alpha1.SchedReady
			newStatus.Attempts++
			newStatus.Message = "Waiting scheduler to schedule this queueUnit"
		}
	} else {
		newStatus.Phase = v1alpha1.Dequeued
		newStatus.Message = "Dequeued because schedule successfully"
	}
	timeNow := time.Now()
	newStatus.LastUpdateTime = &metav1.Time{Time: timeNow}
	newStatus.LastAllocateTime = &metav1.Time{Time: timeNow}

	annotations := make(map[string]string)
	if actualQuotaOversoldType := queueUnit.Annotations[utils.AnnotationActualQuotaOversoldType]; actualQuotaOversoldType != "" {
		annotations[utils.AnnotationActualQuotaOversoldType] = actualQuotaOversoldType
	}
	// annotations[api.DequeueFailReasonAnnotation] = ""

	updatedUnit, err := utils.UpdateQueueUnitStatusAndAnnotations(queueUnit.Name, queueUnit.Namespace, qImpl.Name(), newStatus, s.QueueClient, annotations)
	if err != nil {
		klog.Infof("failed to update QueueUnit, QueueUnitName:%v, oldStatus:%v, newStatus:%v",
			queueUnit.Name, queueUnit.Status, newStatus)
		return err
	} else {
		klog.Infof("success to update QueueUnit, QueueUnitName:%v, oldStatus:%v, newStatus:%v",
			queueUnit.Name, queueUnit.Status, newStatus)
	}

	if newStatus.Phase == v1alpha1.Dequeued {
		s.recorder.Event(updatedUnit, "Normal", "Scheduled", "dequeue success")
	}
	s.fw.RunReservePluginsDequeued(schedulingCycleCtx, framework.NewQueueUnitInfo(updatedUnit))
	qImpl.DequeueSuccess(updatedUnit)
	return nil
}

func (s *Scheduler) ErrorFunc(ctx context.Context, queueUnit *framework.QueueUnitInfo, q *queue.Queue, reasons string, finished int64, preempted bool) {
	logger := klog.FromContext(ctx)

	queueUnit.Attempts++
	queueUnit.Timestamp = time.Now()
	newQueueUnit, err := s.QueueClient.SchedulingV1alpha1().QueueUnits(queueUnit.Unit.Namespace).Get(ctx, queueUnit.Unit.Name, metav1.GetOptions{})
	if err != nil {
		logger.Error(fmt.Errorf("get qu %v error %v", queueUnit.Name, err), "")
		if errors.IsNotFound(err) {
			q.Delete(queueUnit.Unit)
			return
		}
		// we should drop the queueUnit, because the queueUnit may be outdated.
		err = q.AddUnschedulableIfNotPresent(ctx, queueUnit, s.finished != finished || preempted)
		if err != nil {
			klog.Errorf("Add Unschedulable QueueUnit %v failed %v", queueUnit.Name, err.Error())
		}
		return
	}

	newQueueUnit.Status.Message = reasons
	newQueueUnit.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
	// newQueueUnit.Status.Phase = v1alpha1.Enqueued
	updatedUnit, err := s.QueueClient.SchedulingV1alpha1().QueueUnits(queueUnit.Unit.Namespace).UpdateStatus(ctx, newQueueUnit, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("update qu %v error %v", queueUnit.Name, err)
		if errors.IsNotFound(err) {
			return
		}
	} else {
		s.recorder.Event(updatedUnit, "Warning", "FailedScheduling", reasons)
	}
	queueUnit.Unit = newQueueUnit
	err = q.AddUnschedulableIfNotPresent(ctx, queueUnit, s.finished != finished || preempted)
	if err != nil {
		logger.Error(fmt.Errorf("add unschedulable queueUnit %v failed %v", queueUnit.Name, err.Error()), "reason", reasons)
	} else {
		logger.V(3).Info("add unschedulable queueUnit success", "reason", reasons, "queueUnitName", queueUnit.Name, "finished", finished, "preempted", preempted, "s.finished", s.finished)
	}
}
