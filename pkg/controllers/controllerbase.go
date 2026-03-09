package controllers

import (
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller interface {
	Start(stop <-chan struct{})
	Reconcile(key string) (Result, error)
}

type ControllerBaseImpl struct {
	workers int
	logger  logr.Logger

	informerSynced []cache.InformerSynced
	syncHandler    func(key string) (Result, error)
	Wq             workqueue.TypedRateLimitingInterface[string]
}

func NewControllerBase(workers int, logger logr.Logger, informers []cache.InformerSynced, syncHandler func(key string) (Result, error), rateLimiter workqueue.TypedRateLimiter[string]) ControllerBaseImpl {
	return ControllerBaseImpl{
		workers:        workers,
		logger:         logger,
		informerSynced: informers,
		syncHandler:    syncHandler,
		Wq:             workqueue.NewTypedRateLimitingQueue[string](rateLimiter),
	}
}

func (ctrl *ControllerBaseImpl) Start(stop <-chan struct{}) {
	go ctrl.Run(stop)
}

// Run starts listening on channel events
func (ctrl *ControllerBaseImpl) Run(stopCh <-chan struct{}) {
	defer ctrl.Wq.ShutDown()
	ctrl.logger.Info("Starting SyncHandler")
	defer ctrl.logger.Info("Shutting SyncHandler")

	if !cache.WaitForCacheSync(stopCh, ctrl.informerSynced...) {
		ctrl.logger.Error(nil, "Cannot sync caches")
		return
	}
	ctrl.logger.Info("sync finished")

	for i := 0; i < ctrl.workers; i++ {
		go wait.Until(ctrl.handle, time.Second, stopCh)
	}

	<-stopCh
}

func (ctrl *ControllerBaseImpl) handle() {
	for ctrl.processNextWorkItem() {
	}
}

type Result struct {
	Requeue      bool
	RequeueAfter time.Duration
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *ControllerBaseImpl) processNextWorkItem() bool {
	keyObj, quit := ctrl.Wq.Get()
	if quit {
		return false
	}
	defer ctrl.Wq.Done(keyObj)

	key := keyObj
	if res, err := ctrl.syncHandler(key); err != nil {
		runtime.HandleError(err)
		ctrl.logger.Error(err, "Error syncing podGroup", "podGroup", key)
		return true
	} else if res.Requeue {
		if res.RequeueAfter > 0 {
			ctrl.Wq.AddAfter(key, res.RequeueAfter)
		} else {
			ctrl.Wq.AddRateLimited(key)
		}
	}
	return true
}
