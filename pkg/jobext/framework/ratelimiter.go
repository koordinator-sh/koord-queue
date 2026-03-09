package framework

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
)

type ItemBasedRateLimiter struct {
	lock sync.RWMutex

	r      rate.Limit
	b      int
	itemMp map[string]*rate.Limiter
}

var _ workqueue.RateLimiter = &ItemBasedRateLimiter{}

func (r *ItemBasedRateLimiter) When(item interface{}) time.Duration {
	r.lock.RLock()
	limiter, ok := r.itemMp[item.(string)]
	r.lock.RUnlock()
	if !ok {
		r.lock.Lock()
		defer r.lock.Unlock()
		r.itemMp[item.(string)] = rate.NewLimiter(rate.Limit(r.r), r.b)
		return limiter.Reserve().Delay()
	}
	return limiter.Reserve().Delay()
}

func (r *ItemBasedRateLimiter) NumRequeues(item interface{}) int {
	return 0
}

func (r *ItemBasedRateLimiter) Forget(item interface{}) {
}
