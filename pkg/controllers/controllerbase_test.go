package controllers

import (
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

func TestNewControllerBase(t *testing.T) {
	// Arrange
	workers := 5
	logger := logr.Discard()
	var informers []cache.InformerSynced = []cache.InformerSynced{} // Fixed type
	syncHandler := func(key string) (Result, error) {
		return Result{}, nil
	}
	rateLimiter := workqueue.DefaultTypedItemBasedRateLimiter[string]()

	// Act
	controller := NewControllerBase(workers, logger, informers, syncHandler, rateLimiter)

	// Assert
	assert.Equal(t, workers, controller.workers)
	assert.Equal(t, logger, controller.logger)
	assert.Equal(t, informers, controller.informerSynced)
	// We can't directly compare functions, so we just check that syncHandler is not nil
	assert.NotNil(t, controller.syncHandler)
	assert.NotNil(t, controller.Wq)
}

func TestControllerBaseImpl_processNextWorkItem_Success(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	queue := workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]())
	queue.Add("test-key")

	var syncHandlerCalled bool
	var syncHandlerKey string
	syncHandler := func(key string) (Result, error) {
		syncHandlerCalled = true
		syncHandlerKey = key
		return Result{}, nil
	}

	controller := &ControllerBaseImpl{
		logger:      logger,
		syncHandler: syncHandler,
		Wq:          queue,
	}

	// Act
	result := controller.processNextWorkItem()

	// Assert
	assert.True(t, result)
	assert.True(t, syncHandlerCalled)
	assert.Equal(t, "test-key", syncHandlerKey)
}

func TestControllerBaseImpl_processNextWorkItem_SyncHandlerError(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	queue := workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]())
	queue.Add("test-key")

	syncHandler := func(key string) (Result, error) {
		return Result{}, errors.New("sync error")
	}

	controller := &ControllerBaseImpl{
		logger:      logger,
		syncHandler: syncHandler,
		Wq:          queue,
	}

	// Act
	result := controller.processNextWorkItem()

	// Assert
	assert.True(t, result)
	// Verify the item was removed from the queue
	assert.Equal(t, 0, queue.Len())
}

func TestControllerBaseImpl_processNextWorkItem_Requeue(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	queue := workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]())
	queue.Add("test-key")

	syncHandler := func(key string) (Result, error) {
		return Result{Requeue: true}, nil
	}

	controller := &ControllerBaseImpl{
		logger:      logger,
		syncHandler: syncHandler,
		Wq:          queue,
	}

	// Act
	result := controller.processNextWorkItem()

	// Assert
	assert.True(t, result)
	// Wait a bit for the rate limiter to process
	time.Sleep(10 * time.Millisecond)
	// Verify the item was requeued
	assert.Equal(t, 1, queue.NumRequeues("test-key"))
}

func TestControllerBaseImpl_processNextWorkItem_RequeueAfter(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	queue := workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]())
	queue.Add("test-key")

	syncHandler := func(key string) (Result, error) {
		return Result{Requeue: true, RequeueAfter: 50 * time.Millisecond}, nil
	}

	controller := &ControllerBaseImpl{
		logger:      logger,
		syncHandler: syncHandler,
		Wq:          queue,
	}

	// Act
	result := controller.processNextWorkItem()

	// Assert
	assert.True(t, result)
	// Initially the item should be gone from the queue
	assert.Equal(t, 0, queue.Len())
	// After some time it should be added back
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, queue.Len())
}

func TestControllerBaseImpl_processNextWorkItem_Quit(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	queue := workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]())
	// Don't add anything to the queue, then shut it down to simulate quit
	queue.ShutDown()

	syncHandler := func(key string) (Result, error) {
		t.Error("Sync handler should not be called")
		return Result{}, nil
	}

	controller := &ControllerBaseImpl{
		logger:      logger,
		syncHandler: syncHandler,
		Wq:          queue,
	}

	// Act
	result := controller.processNextWorkItem()

	// Assert
	assert.False(t, result)
}

func TestControllerBaseImpl_Start(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	controller := &ControllerBaseImpl{
		logger: logger,
		Wq:     workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]()),
	}

	// Act & Assert
	// We can't easily test the full Start functionality without a complex setup,
	// but we can verify it doesn't panic
	assert.NotPanics(t, func() {
		stopCh := make(chan struct{})
		controller.Start(stopCh)
		close(stopCh)
	})
}

func TestControllerBaseImpl_Run(t *testing.T) {
	// Arrange
	logger := logr.Discard()
	// Create a mock informer synced function that returns true
	informerSynced := func() bool { return true }
	controller := &ControllerBaseImpl{
		logger:         logger,
		informerSynced: []cache.InformerSynced{informerSynced},
		workers:        1,
		Wq:             workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedItemBasedRateLimiter[string]()),
	}

	// Act & Assert
	// We can't easily test the full Run functionality without a complex setup,
	// but we can verify it doesn't panic
	assert.NotPanics(t, func() {
		stopCh := make(chan struct{})
		go func() {
			time.Sleep(10 * time.Millisecond) // Let it run briefly
			close(stopCh)
		}()
		controller.Run(stopCh)
	})
}
