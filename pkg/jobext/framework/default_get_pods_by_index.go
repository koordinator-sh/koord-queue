package framework

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// By default, we get pods from client index
type JobType_Default struct{}

func (j *JobType_Default) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return nil
}

// For resourcebinding and clusterresourcebinding, just return nil
type JobType_NonPod struct{}

func (j *JobType_NonPod) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
		return nil, nil
	}
}
