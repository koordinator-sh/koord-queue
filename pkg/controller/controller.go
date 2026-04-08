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
	"sync"
	"sync/atomic"

	"github.com/koordinator-sh/koord-queue/cmd/app/options"
	"github.com/koordinator-sh/koord-queue/pkg/config"
	"github.com/koordinator-sh/koord-queue/pkg/controllers"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	apiv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"
)

type Controller struct {
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder                record.EventRecorder
	multiSchedulingQueue    queue.MultiSchedulingQueue
	fw                      framework.Framework
	scheduler               *scheduler.Scheduler
	queueUnitInformer       cache.SharedIndexInformer
	queueUnitClient         versioned.Interface
	queueUnitLister         externalv1alpha1.QueueUnitLister
	queueInformer           cache.SharedIndexInformer
	enableStrictConsistency bool
	quController            *controllers.QueueUnitController
}

func NewController(kubeConfigPath string, enableStrictConsistency bool, stopCh <-chan struct{}, ctrlopts ...ControllerConfigOpt) (*Controller, error) {
	cfg := config.ControllerConfig{}
	for _, opt := range ctrlopts {
		opt(&cfg)
	}

	kubeConfig := cfg.KubeConfig
	kubeClient := cfg.KubeClient
	queueUnitClient := cfg.QueueUnitClient
	queueFactory := cfg.QueueFactory
	informerFactory := cfg.InformersFactory
	schemeModified := scheme.Scheme
	_ = v1alpha1.AddToScheme(schemeModified)
	queueInformer := queueFactory.Scheduling().V1alpha1().Queues().Informer()
	queueUnitLister := queueFactory.Scheduling().V1alpha1().QueueUnits().Lister()
	queueUnitInformer := queueFactory.Scheduling().V1alpha1().QueueUnits().Informer()

	opts := options.NewServerOption()
	// Create event broadcaster
	klog.V(1).Info("Starting Koord Queue EventBroadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(schemeModified, corev1.EventSource{Component: utils.ControllerAgentName})

	klog.V(1).Info("Creating framework")
	r := plugins.NewInTreeRegistry()
	fw, err := runtime.NewFramework(r,
		kubeConfig,
		kubeConfigPath,
		informerFactory,
		queueFactory,
		recorder,
		queueUnitClient,
		opts.OversellRate,
		cfg.Config)
	if err != nil {
		klog.Fatalf("new framework failed %v", err)
	}
	multiSchedulingQueue, err := multischedulingqueue.NewMultiSchedulingQueue(fw,
		opts.PodInitialBackoffSeconds,
		opts.PodMaxBackoffSeconds,
		queueUnitLister,
		enableStrictConsistency)
	if err != nil {
		klog.Fatalf("init multi scheduling queue failed %s", err)
	}

	controller := &Controller{
		recorder:                recorder,
		fw:                      fw,
		multiSchedulingQueue:    multiSchedulingQueue,
		queueUnitClient:         queueUnitClient,
		queueUnitInformer:       queueUnitInformer,
		queueInformer:           queueInformer,
		queueUnitLister:         queueUnitLister,
		enableStrictConsistency: enableStrictConsistency,
	}
	controller.quController = controllers.NewQueueUnitController(opts.AdmissionCheckControllerWorker, opts.StrictDequeueMode, queueUnitClient, queueUnitInformer, queueUnitLister)
	controller.AddAllEventHandlers(queueUnitInformer, queueInformer)

	controller.scheduler, err = scheduler.NewScheduler(multiSchedulingQueue,
		fw,
		queueUnitClient,
		recorder,
		enableStrictConsistency,
		opts.StrictDequeueMode,
		opts.EnableParentLimit,
		opts.ScheduleSuspendTimeInMillSecond,
		opts.QueueList)
	if err != nil {
		klog.Fatalf("init scheduler failed %s", err)
	}

	return controller, nil
}

func (c *Controller) GetFramework() framework.Framework {
	return c.fw
}

func (c *Controller) SetQuController(ctrl *controllers.QueueUnitController) {
	c.quController = ctrl
}

func (c *Controller) SetScheduler(sched *scheduler.Scheduler) {
	c.scheduler = sched
}

func (c *Controller) SetFramework(fw framework.Framework) {
	c.fw = fw
	c.recorder = fw.EventRecorder()
}

func (c *Controller) SetMultiSchedulingQueue(queue queue.MultiSchedulingQueue) {
	c.multiSchedulingQueue = queue
}

func (c *Controller) Start(ctx context.Context) {
	queueUnits, _ := c.fw.QueueInformerFactory().Scheduling().V1alpha1().QueueUnits().Lister().List(labels.Everything())
	pending, reserved, satisfied := 0, 0, 0
	for _, qu := range queueUnits {
		if len(qu.Status.Admissions) == 0 {
			pending++
		} else if utils.IsQueueUnitSatisfied(qu) {
			satisfied++
		} else {
			reserved++
		}
		c.AddQueueUnit(qu)
	}
	klog.InfoS("add existing queueunits to controller", "count", len(queueUnits), "pending", pending, "satisfied", satisfied, "reserved", reserved)
	queues, _ := c.fw.QueueInformerFactory().Scheduling().V1alpha1().Queues().Lister().Queues("koord-queue").List(labels.Everything())
	for _, q := range queues {
		c.AddQueue(q)
	}

	c.quController.Start(ctx.Done())
	c.scheduler.Start(ctx)
}

func (c *Controller) GetQueueDebugInfo() map[string]queuepolicies.QueueDebugInfo {
	return c.multiSchedulingQueue.GetQueueDebugInfo()
}

func (c *Controller) GetUserQuotaDebugInfo() map[string]queuepolicies.UserQuotaDebugInfo {
	return c.multiSchedulingQueue.GetUserQuotaDebugInfo()
}

func (c *Controller) GetQueueUnitsByQuota(quota string, opt *apiv1alpha1.QueueUnitOptions) []apiv1alpha1.QueueUnit {
	queueunits, err := c.fw.GetQueueUnitInfoByQuota(quota)
	if err != nil {
		return nil
	}

	res := make([]apiv1alpha1.QueueUnit, len(queueunits))
	var wg sync.WaitGroup
	var i int32 = 0
	completeQueueUnit := func(piece int) {
		defer wg.Done()
		unit := &queueunits[piece]
		unitCR, err := c.queueUnitLister.QueueUnits(unit.Namespace).Get(unit.Name)
		if err != nil {
			return
		}
		if opt.Phase != "" && unitCR.Status.Phase != v1alpha1.QueueUnitPhase(opt.Phase) {
			return
		}
		c.multiSchedulingQueue.Complete(unit)
		unit.Request = utils.ConvertResourceListToString(unitCR.Spec.Request)
		unit.Resources = utils.ConvertResourceListToString(unitCR.Spec.Resource)
		unit.Phase = string(unitCR.Status.Phase)
		unit.PodState = apiv1alpha1.PodState{
			Running: int32(unitCR.Status.PodState.Running),
			Pending: int32(unitCR.Status.PodState.Pending),
		}
		newIdx := atomic.AddInt32(&i, 1)
		res[newIdx-1] = *unit
	}
	for i := range queueunits {
		wg.Add(1)
		go completeQueueUnit(i)
	}
	wg.Wait()
	return res[:i]
}

// we only support to query enqueued units
func (c *Controller) GetQueueUnitsByQueue(queueName string) ([]apiv1alpha1.QueueUnit, error) {
	q, ok := c.multiSchedulingQueue.GetQueueByName(queueName)
	if !ok {
		return nil, errors.NewNotFound(v1alpha1.Resource("queue"), queueName)
	}

	units := q.List()
	res := make([]apiv1alpha1.QueueUnit, len(units))
	var wg sync.WaitGroup
	var i int32 = 0
	completeQueueUnit := func(piece int) {
		defer wg.Done()
		unitInfo := units[piece]
		unit := unitInfo.Unit
		unitCR, err := c.queueUnitLister.QueueUnits(unit.Namespace).Get(unit.Name)
		if err != nil {
			return
		}
		apiunit := apiv1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      unit.Name,
				Namespace: unit.Namespace,
			},
		}
		quota, err := c.fw.GetQueueUnitQuotaName(unitInfo.Unit)
		if err == nil {
			apiunit.QuotaName = quota[0]
		}
		apiunit.Request = utils.ConvertResourceListToString(unitCR.Spec.Request)
		apiunit.Resources = utils.ConvertResourceListToString(unitCR.Spec.Resource)
		apiunit.Phase = string(unitCR.Status.Phase)
		apiunit.PodState = apiv1alpha1.PodState{
			Running: int32(unitCR.Status.PodState.Running),
			Pending: int32(unitCR.Status.PodState.Pending),
		}
		newIdx := atomic.AddInt32(&i, 1)
		res[newIdx-1] = apiunit
	}
	for i := range units {
		wg.Add(1)
		go completeQueueUnit(i)
	}
	wg.Wait()
	return res, nil
}
