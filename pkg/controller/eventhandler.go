/*
 Copyright 2021 The Koord-Queue Authors.

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

package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/metrics"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

func (c *Controller) AddAllEventHandlers(queueUnitInformer cache.SharedIndexInformer, queueInformer cache.SharedIndexInformer) {
	queueUnitInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			oq, ok := oldObj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			nq, ok := newObj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			if nq.Status.Phase == v1alpha1.SchedSucceed && oq.Status.Phase != v1alpha1.SchedSucceed {
				c.recorder.Event(oq, "Normal", "Scheduled", "QueueUnit has been scheduled")
			}
			if nq.Status.Phase == v1alpha1.SchedFailed && oq.Status.Phase != v1alpha1.SchedFailed {
				c.recorder.Event(nq, "Normal", "FailedScheduling", nq.Status.Message)
			}
		},
	})

	queueUnitInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			qu, ok := obj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			assigned := utils.IsQueueUnitAssigned(qu)
			if assigned {
				c.fw.RunReservePluginsAddAssignedJob(context.Background(), framework.NewQueueUnitInfo(qu))
			}
			c.AddQueueUnit(qu)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldQu, ok := oldObj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			newQu, ok := newObj.(*v1alpha1.QueueUnit)
			if !ok {
				return
			}
			assignedOld := utils.IsQueueUnitAssigned(oldQu)
			assignedNew := utils.IsQueueUnitAssigned(newQu)
			if assignedOld && assignedNew {
				c.fw.RunReservePluginsResize(context.Background(), framework.NewQueueUnitInfo(oldQu), framework.NewQueueUnitInfo(newQu))
			} else if assignedOld && !assignedNew {
				c.fw.RunReservePluginsRemoveJob(context.Background(), framework.NewQueueUnitInfo(oldQu))
			} else if !assignedOld && assignedNew {
				c.fw.RunReservePluginsAddAssignedJob(context.Background(), framework.NewQueueUnitInfo(newQu))
			}
			c.UpdateQueueUnit(oldQu, newQu)
		},
		DeleteFunc: func(obj interface{}) {
			var qu *v1alpha1.QueueUnit
			switch t := obj.(type) {
			case *v1alpha1.QueueUnit:
				qu = t
			case cache.DeletedFinalStateUnknown:
				if _, ok := t.Obj.(*v1alpha1.QueueUnit); ok {
					qu = t.Obj.(*v1alpha1.QueueUnit)
				} else {
					return
				}
			default:
				return
			}
			assigned := utils.IsQueueUnitAssigned(qu)
			if assigned {
				c.fw.RunReservePluginsRemoveJob(context.Background(), framework.NewQueueUnitInfo(qu))
			}
			c.DeleteQueueUnit(qu)
		},
	})

	queueInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.AddQueue,
			UpdateFunc: c.UpdateQueue,
			DeleteFunc: c.DeleteQueue,
		},
	)

	if os.Getenv("TestENV") == "true" {
		return
	}
	go func() {
		if err := wait.PollUntilContextCancel(context.Background(), time.Minute, false, func(ctx context.Context) (done bool, err error) {
			framework.ForgetQueueUnitInfo(c.queueUnitInformer.GetIndexer())
			return false, nil
		}); err != nil {
			klog.Errorf("PollUntilContextCancel exited with error: %v", err)
		}
	}()
}

func (c *Controller) AddQueue(obj interface{}) {
	queue := obj.(*v1alpha1.Queue)
	queueName := queue.Name
	_, ok := c.multiSchedulingQueue.GetQueueByName(queueName)
	if ok {
		klog.V(2).Infof("queue is exist %s", queueName)
		return
	}
	err := c.multiSchedulingQueue.Add(queue)
	if err != nil {
		klog.Errorf("add queue err %v", err)
		c.recorder.Event(queue, "Warning", "AddQueueFail", err.Error())
	}
	klog.V(2).Infof("queue %s is added", queueName)
}

func (c *Controller) UpdateQueue(oldObj, newObj interface{}) {
	oldQ := oldObj.(*v1alpha1.Queue)
	newQ := newObj.(*v1alpha1.Queue)
	err := c.multiSchedulingQueue.Update(oldQ, newQ)
	if err != nil {
		klog.Errorf("queue %s update fail %v", oldQ.Namespace, err.Error())
		c.recorder.Event(newQ, "Warning", "AddQueueFail", err.Error())
	} else {
		klog.V(5).InfoS("queue changed", "queue", newQ.Name, "old policy", oldQ.Spec.QueuePolicy, "new policy", newQ.Spec.QueuePolicy)
	}
}

func (c *Controller) DeleteQueue(obj interface{}) {
	queue := obj.(*v1alpha1.Queue)
	err := c.multiSchedulingQueue.Delete(queue)
	if err != nil {
		klog.Errorf("queue %s delete fail %v", queue.Namespace, err.Error())
	}
}

func (c *Controller) getQueueByUnit(qu *v1alpha1.QueueUnit) (string, *queue.Queue, bool) {
	queueName := c.multiSchedulingQueue.GetQueueForQueueUnit(qu)
	q, ok := c.multiSchedulingQueue.GetQueueByName(queueName)
	return queueName, q, ok
}

func (c *Controller) AddQueueUnit(queueUnit *v1alpha1.QueueUnit) {
	if queueUnit.Spec.ConsumerRef != nil {
		metrics.QueueUnitsByJobType.WithLabelValues(queueUnit.Namespace, queueUnit.Spec.ConsumerRef.Kind).Add(1)
	}

	var err error
	var msg string
	logger := klog.Background().WithName("controller").WithValues("queueUnit", klog.KObj(queueUnit))
	defer func() {
		if err != nil {
			logger.V(1).Error(err, msg)
		} else {
			logger.V(1).Info("queueUnit add event")
		}
	}()
	queueName, err := c.multiSchedulingQueue.GetQueueNameByQueueUnit(queueUnit)
	if err != nil {
		msg = "failed to get queue name by queue unit"
		c.multiSchedulingQueue.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(queueUnit))
		return
	}
	q, ok := c.multiSchedulingQueue.GetQueueByName(queueName)
	if !ok {
		msg = "failed to get queue by name"
		logger = logger.WithValues("queueName", queueName)
		c.multiSchedulingQueue.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(queueUnit))
		return
	}
	logger = logger.WithValues("queueName", queueName)
	c.multiSchedulingQueue.SetQueueForQueueUnit(queueUnit, queueName)
	if err := q.AddQueueUnitInfo(framework.NewQueueUnitInfo(queueUnit)); err != nil {
		klog.Errorf("add queueUnitInfo to queue failed: %v", err)
	}
}

func (c *Controller) UpdateQueueUnit(oldQu, newQu *v1alpha1.QueueUnit) {
	logger := klog.Background().WithName("controller").WithValues("queueUnit", klog.KObj(oldQu))
	defer func() {
		logger.V(1).Info("queueUnit update event", "oldstate", oldQu.Status.Phase, "newstate", newQu.Status.Phase)
	}()

	oldQueueName, oldQ, oldQOk := c.getQueueByUnit(oldQu)
	queueName, err := c.multiSchedulingQueue.GetQueueNameByQueueUnit(newQu)
	if err != nil {
		logger.Error(err, "delete queueunit from queue and the queueunit cannot be added to another queue",
			"queueUnit", klog.KObj(newQu),
			"from", oldQueueName,
			"to", "")
		// when err is nil, it means newQu doesn't belong to any queue in the system
		// so we need to delete qu from old queue and and qu to UnitsFindNoQueue
		if oldQOk {
			if err := oldQ.Delete(newQu); err != nil {
				klog.Errorf("delete queueunit from old queue failed: %v", err)
			}
		}
		c.multiSchedulingQueue.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(newQu))
		return
	}
	q, ok := c.multiSchedulingQueue.GetQueueByName(queueName)
	if !ok {
		logger.Error(fmt.Errorf("failed to find the queue"), "delete queueunit from queue and the queueunit cannot be added to another queue",
			"queueUnit", klog.KObj(newQu),
			"from", oldQueueName,
			"to", queueName)
		if oldQOk {
			if err := oldQ.Delete(newQu); err != nil {
				klog.Errorf("delete queueunit from old queue failed: %v", err)
			}
		}
		c.multiSchedulingQueue.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(newQu))
		return
	}
	// when old queue name is equal to new queue name, we only update qu in old queue
	if oldQueueName == queueName {
		logger = logger.WithValues("queue", q.Name())
		err = q.Update(oldQu, newQu)
		if err != nil {
			klog.Errorf("queue %s update queueunit %v fail %v", queueName, newQu.Name, err.Error())
		}
	} else {
		logger = logger.WithValues("from", oldQueueName, "to", q.Name())
		if oldQOk {
			if err := oldQ.Delete(newQu); err != nil {
				klog.Errorf("delete queueunit from old queue failed: %v", err)
			}
		}
		c.multiSchedulingQueue.SetQueueForQueueUnit(newQu, q.Name())
		err = q.AddQueueUnitInfo(framework.NewQueueUnitInfo(newQu))
		if err != nil {
			klog.Errorf("add queueunit %v to queue %v fail %v", queueName, newQu.Name, err.Error())
			c.multiSchedulingQueue.AddUnitsFindNoQueue(framework.NewQueueUnitInfo(newQu))
		}
	}
}

func (c *Controller) DeleteQueueUnit(queueUnit *v1alpha1.QueueUnit) {
	logger := klog.Background().WithName("controller")
	logger.V(1).Info("queueUint delete event", "queueUnit", klog.KObj(queueUnit))
	if queueUnit.Spec.ConsumerRef != nil {
		metrics.QueueUnitsByJobType.WithLabelValues(queueUnit.Namespace, queueUnit.Spec.ConsumerRef.Kind).Sub(1)
	}

	c.scheduler.FinishedInc()
	_, q, ok := c.getQueueByUnit(queueUnit)
	if !ok {
		return
	}
	if err := q.Delete(queueUnit); err != nil {
		klog.Errorf("delete queueunit from queue failed: %v", err)
	}
}
