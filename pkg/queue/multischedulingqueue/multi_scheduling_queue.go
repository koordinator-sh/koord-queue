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

package multischedulingqueue

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	externalv1alpha1 "github.com/kube-queue/api/pkg/client/listers/scheduling/v1alpha1"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/queue"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
)

// Making sure that MultiSchedulingQueue implements MultiSchedulingQueue.
var _ queue.MultiSchedulingQueue = &MultiSchedulingQueue{}

type MultiSchedulingQueue struct {
	sync.RWMutex
	fw                       framework.MultiQueueHandle
	queueMap                 map[string]*queue.Queue
	queueUnitToQueue         map[string]string
	queueUnitFindNoQueue     []*framework.QueueUnitInfo
	lessFunc                 framework.MultiQueueLessFunc
	groupFunc                framework.QueueUnitMappingFunc
	queueUnitLister          externalv1alpha1.QueueUnitLister
	podInitialBackoffSeconds int
	podMaxBackoffSeconds     int
	enableStrictConsistency  bool
	started                  bool

	queueChan *sync.Cond
}

func (mq *MultiSchedulingQueue) QueueChan() *sync.Cond {
	return mq.queueChan
}

func (mq *MultiSchedulingQueue) Complete(qu *apiv1alpha1.QueueUnit) {
	mq.RLock()
	defer mq.RUnlock()

	queueName := mq.queueUnitToQueue[qu.Namespace+"/"+qu.Name]
	if queueName != "" {
		qu.QueueName = queueName
		queueImpl, ok := mq.queueMap[queueName]
		if !ok {
			return
		}
		queueImpl.Complete(qu)
	} else {
		//TODO: for units that do not belong to any queue
	}
}

func (mq *MultiSchedulingQueue) SetQueueForQueueUnit(qu *v1alpha1.QueueUnit, qName string) {
	mq.Lock()
	mq.queueUnitToQueue[qu.Namespace+"/"+qu.Name] = qName
	mq.Unlock()
}

func (mq *MultiSchedulingQueue) GetQueueForQueueUnit(qu *v1alpha1.QueueUnit) string {
	mq.RLock()
	defer mq.RUnlock()
	return mq.queueUnitToQueue[qu.Namespace+"/"+qu.Name]
}

func (mq *MultiSchedulingQueue) DeleteQueueUnit(qu *v1alpha1.QueueUnit) {
	mq.Lock()
	defer mq.Unlock()
	q := mq.queueMap[mq.queueUnitToQueue[qu.Namespace+"/"+qu.Name]]
	if q != nil {
		q.Delete(qu)
	}
	delete(mq.queueUnitToQueue, qu.Namespace+"/"+qu.Name)
}

func NewMultiSchedulingQueue(fw framework.MultiQueueHandle, podInitialBackoffSeconds int, podMaxBackoffSeconds int, queueUnitLister externalv1alpha1.QueueUnitLister, enableStrictConsistency bool) (queue.MultiSchedulingQueue, error) {
	mq := &MultiSchedulingQueue{
		fw:                       fw,
		queueMap:                 make(map[string]*queue.Queue),
		queueUnitToQueue:         make(map[string]string),
		queueUnitFindNoQueue:     []*framework.QueueUnitInfo{},
		lessFunc:                 fw.MultiQueueSortFunc(),
		groupFunc:                fw.QueueUnitMappingFunc(),
		podInitialBackoffSeconds: podInitialBackoffSeconds,
		podMaxBackoffSeconds:     podMaxBackoffSeconds,
		queueUnitLister:          queueUnitLister,
		enableStrictConsistency:  enableStrictConsistency,
		queueChan:                sync.NewCond(&sync.Mutex{}),
	}

	return mq, nil
}

func (mq *MultiSchedulingQueue) FixQueues() {
	mq.Lock()
	defer mq.Unlock()
	unitsToRequeue := []*framework.QueueUnitInfo{}
	unitsToRequeue = append(unitsToRequeue, mq.queueUnitFindNoQueue...)
	mq.queueUnitFindNoQueue = []*framework.QueueUnitInfo{}
	for _, q := range mq.queueMap {
		unitsToRequeue = append(unitsToRequeue, q.Fix(mq.groupFunc)...)
	}
	for _, unit := range unitsToRequeue {
		queueName, err := mq.GetQueueNameByQueueUnit(unit.Unit)
		if err != nil {
			klog.Infof("reenqueue queueunit %v/%v failed: %v", unit.Unit.Namespace, unit.Name, err.Error())
			mq.queueUnitFindNoQueue = append(mq.queueUnitFindNoQueue, unit)
			continue
		}
		queue, ok := mq.queueMap[queueName]
		if ok {
			klog.Infof("reenqueue queueunit %v/%v to %v", unit.Unit.Namespace, unit.Name, queueName)
			mq.queueUnitToQueue[unit.Name] = queue.Name()
			queue.AddQueueUnitInfo(unit)
		} else {
			klog.Infof("reenqueue queueunit %v/%v to non exist queue", unit.Unit.Namespace, unit.Name)
			mq.queueUnitFindNoQueue = append(mq.queueUnitFindNoQueue, unit)
		}
	}
}

func (mq *MultiSchedulingQueue) Start(ctx context.Context) {
	go wait.Until(mq.FlushUnitsFindNoQueue, time.Second*10, ctx.Done())
	mq.started = true
	mq.Run()
}

func (mq *MultiSchedulingQueue) AddUnitsFindNoQueue(q *framework.QueueUnitInfo) {
	mq.Lock()
	defer mq.Unlock()
	delete(mq.queueMap, q.Unit.Namespace+"/"+q.Unit.Name)
	mq.queueUnitFindNoQueue = append(mq.queueUnitFindNoQueue, q)
}

func (mq *MultiSchedulingQueue) flushUnitsFindNoQueue() {
	newUnitsList := []*framework.QueueUnitInfo{}
	for _, qu := range mq.queueUnitFindNoQueue {
		newQu, err := mq.queueUnitLister.QueueUnits(qu.Unit.Namespace).Get(qu.Unit.Name)
		if err != nil {
			continue
		}
		qu.Unit = newQu
		qname, err := mq.GetQueueNameByQueueUnit(newQu)
		if err != nil {
			klog.Infof("qu %v not available: %v", qu.Name, err.Error())
			continue
		}
		if qname == "" {
			klog.Infof("qu %v doesn't belong to any queue", qu.Name)
			newUnitsList = append(newUnitsList, qu)
			continue
		}
		queue, ok := mq.queueMap[qname]
		if !ok {
			klog.Infof("queue %v not found for qu %v", qname, qu.Name)
			newUnitsList = append(newUnitsList, qu)
		} else {
			klog.Infof("add qu %v to queue %v", qu.Name, qname)
			mq.queueUnitToQueue[qu.Unit.Namespace+"/"+qu.Unit.Name] = qname
			queue.AddQueueUnitInfo(qu)
		}
	}
	mq.queueUnitFindNoQueue = newUnitsList
}

func (mq *MultiSchedulingQueue) FlushUnitsFindNoQueue() {
	mq.Lock()
	defer mq.Unlock()
	mq.flushUnitsFindNoQueue()
}

func (mq *MultiSchedulingQueue) Run() {
	for _, q := range mq.queueMap {
		q.Run()
	}
}

func (mq *MultiSchedulingQueue) Close() {
	mq.Lock()
	defer mq.Unlock()

	for _, q := range mq.queueMap {
		q.Close()
	}
}

func (mq *MultiSchedulingQueue) Add(q *v1alpha1.Queue) error {
	mq.Lock()
	defer mq.Unlock()

	// Name is name for the moment
	name := q.Name
	if _, ok := mq.queueMap[name]; ok {
		return nil
	}

	argsStr := q.Annotations[queuepolicies.QueueArgsAnnotationKey]
	args := make(map[string]string)
	if args["podInitialBackoffSeconds"] == "" {
		args["podInitialBackoffSeconds"] = strconv.Itoa(mq.podInitialBackoffSeconds)
	}
	if args["podMaxBackoffSeconds"] == "" {
		args["podMaxBackoffSeconds"] = strconv.Itoa(mq.podMaxBackoffSeconds)
	}
	yaml.Unmarshal([]byte(argsStr), args)
	pq, err := queue.NewQueue(mq.fw, mq.queueUnitLister, name, string(q.Spec.QueuePolicy), q, args)
	if err != nil {
		return err
	}
	mq.queueMap[pq.Name()] = pq

	if mq.started {
		mq.Run()
	}

	mq.queueChan.Broadcast()
	return nil
}

func (mq *MultiSchedulingQueue) Delete(q *v1alpha1.Queue) error {
	mq.Lock()
	defer mq.Unlock()

	name := q.Name
	queue, ok := mq.queueMap[name]
	if ok {
		unitList := queue.List()
		// klog.Infof("units:%v need reenqueue", unitList)
		mq.queueUnitFindNoQueue = append(mq.queueUnitFindNoQueue, unitList...)
		mq.flushUnitsFindNoQueue()
	}
	delete(mq.queueMap, name)

	mq.queueChan.Broadcast()
	return nil
}

func (mq *MultiSchedulingQueue) Update(old *v1alpha1.Queue, new *v1alpha1.Queue) error {
	mq.Lock()
	defer mq.Unlock()

	name := new.Name
	currentQueue, ok := mq.queueMap[name]
	if !ok {
		return mq.Add(new)
	}

	oldTmp := &v1alpha1.Queue{ObjectMeta: v1.ObjectMeta{Labels: old.Labels, Annotations: old.Annotations}, Spec: old.Spec}
	newTmp := &v1alpha1.Queue{ObjectMeta: v1.ObjectMeta{Labels: new.Labels, Annotations: new.Annotations}, Spec: new.Spec}
	if reflect.DeepEqual(oldTmp, newTmp) {
		klog.V(1).InfoS("skip queue update", "queue", new.Name)
		return nil
	}

	return currentQueue.Sync(new)
}

func (mq *MultiSchedulingQueue) GetQueueNameByQueueUnit(q *v1alpha1.QueueUnit) (string, error) {
	return mq.groupFunc(q)
}

func (mq *MultiSchedulingQueue) GetAllQueues() map[string]*queue.Queue {
	mq.RLock()
	defer mq.RUnlock()

	result := make(map[string]*queue.Queue)
	for k, v := range mq.queueMap {
		result[k] = v
	}
	return result
}

func (mq *MultiSchedulingQueue) GetQueueByName(name string) (*queue.Queue, bool) {
	mq.RLock()
	defer mq.RUnlock()

	q, ok := mq.queueMap[name]
	return q, ok
}

func (mq *MultiSchedulingQueue) SortedQueue() []*queue.Queue {
	mq.RLock()
	defer mq.RUnlock()

	len := len(mq.queueMap)
	unSortedQueue := make([]*queue.Queue, len)

	index := 0
	for _, q := range mq.queueMap {
		unSortedQueue[index] = q
		index++
	}

	sort.Slice(unSortedQueue, func(i, j int) bool {
		return mq.lessFunc(unSortedQueue[i].Queue(), unSortedQueue[j].Queue())
	})

	return unSortedQueue
}

func (mq *MultiSchedulingQueue) GetQueueDebugInfo() map[string]queuepolicies.QueueDebugInfo {
	mq.RLock()
	defer mq.RUnlock()

	result := make(map[string]queuepolicies.QueueDebugInfo)

	for name, queue := range mq.queueMap {
		result[name] = queue.GetQueueDebugInfo()
	}

	return result
}

func (mq *MultiSchedulingQueue) GetUserQuotaDebugInfo() map[string]queuepolicies.UserQuotaDebugInfo {
	mq.RLock()
	defer mq.RUnlock()

	result := make(map[string]queuepolicies.UserQuotaDebugInfo)

	for name, queue := range mq.queueMap {
		result[name] = queue.GetUserQuotaDebugInfo()
	}

	return result
}
