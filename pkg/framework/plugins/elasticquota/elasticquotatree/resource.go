package elasticquotatree

import (
	"errors"

	v1 "k8s.io/api/core/v1"
)

// GetPodKey returns the string key of a pod.
func GetPodKey(pod *v1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}
