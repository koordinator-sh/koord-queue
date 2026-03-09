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

package queue

import (
	"context"
	"sync"

	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies"

	schedv1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
)

// MultiSchedulingQueue is interface of Multi Scheduling Queue.
type MultiSchedulingQueue interface {
	Add(*schedv1alpha1.Queue) error
	Delete(*schedv1alpha1.Queue) error
	Update(*schedv1alpha1.Queue, *schedv1alpha1.Queue) error
	// map of queueUnit to queue will be cleared in this func
	AddUnitsFindNoQueue(q *framework.QueueUnitInfo)
	SortedQueue() []*Queue
	GetQueueByName(name string) (*Queue, bool)
	GetAllQueues() map[string]*Queue
	GetQueueNameByQueueUnit(*schedv1alpha1.QueueUnit) (string, error)
	// Record queueUnits' queue name to cache
	SetQueueForQueueUnit(qu *schedv1alpha1.QueueUnit, qName string)
	// Obtain queueUnits' queue name from cache
	GetQueueForQueueUnit(qu *schedv1alpha1.QueueUnit) string
	Run()
	Start(ctx context.Context)
	Close()
	FixQueues()
	GetQueueDebugInfo() map[string]queuepolicies.QueueDebugInfo
	GetUserQuotaDebugInfo() map[string]queuepolicies.UserQuotaDebugInfo

	Complete(*apiv1alpha1.QueueUnit)
	// When there are queues added or deleted, event will be sent to scheduler to start
	// a new goroutine to schedule the queue.
	QueueChan() *sync.Cond
}
