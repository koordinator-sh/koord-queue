package framework

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
)

func RetryTooManyRequests(f func() error) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return errors.IsTooManyRequests(err)
	}, f)
}

// IgnoreNotFound returns nil on NotFound errors.
// All other values that are not NotFound errors or nil are returned unmodified.
func IgnoreNotFound(err error) error {
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

// IgnoreAlreadyExists returns nil on AlreadyExists errors.
// All other values that are not AlreadyExists errors or nil are returned unmodified.
func IgnoreAlreadyExists(err error) error {
	if errors.IsAlreadyExists(err) {
		return nil
	}

	return err
}
