package framework

import (
	"context"
	"fmt"
	"strings"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type manager struct {
	jobHandles map[string]JobHandle

	cli client.Client
}

func NewManager(cli client.Client, handles map[string]JobHandle) JobManager {
	return &manager{jobHandles: handles, cli: cli}
}

func (m *manager) FindWorkloadFromPod(ctx context.Context, pod *v1.Pod) *ownerInfo {
	var ow *ownerInfo
	if len(pod.OwnerReferences) == 0 {
		return nil
	}
	owner := pod.OwnerReferences[0]
	typeKey := owner.APIVersion + "/" + owner.Kind
	switch typeKey {
	case "apps/v1/Deployment":
		// TODO: Distinguish between a normal Deployment and a Deployment created by a Notebook.
		ow = &ownerInfo{
			groupVersionKind: "dsw.alibaba.com/v1/NoteBook",
			name:             owner.Name,
		}
	case "ray.io/v1/RayCluster":
		// TODO: Distinguish between a normal RayCluster and a RayCluster created by a RayJob.
		rayCluster := &rayv1.RayCluster{}
		if err := m.cli.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}, rayCluster); err != nil {
			if !errors.IsNotFound(err) {
				klog.Background().Error(err, "failed to get RayCluster", "namespace", pod.Namespace, "name", owner.Name)
			}
			// when raycluster is deleted, we cannot find the cr. thus we cannot find if raycluster has the owner.
			if typeKey := pod.Annotations[util.RelatedAPIVersionKindAnnoKey]; typeKey != "" {
				ow = &ownerInfo{
					groupVersionKind: typeKey,
					name:             pod.Annotations[util.RelatedObjectAnnoKey],
				}
			}
		} else {
			for _, indirectOwner := range rayCluster.OwnerReferences {
				if indirectOwner.Kind == "RayJob" {
					ow = &ownerInfo{
						groupVersionKind: "ray.io/v1/RayJob",
						name:             indirectOwner.Name,
					}
				}
			}
		}
	default:
		if _, ok := m.jobHandles[typeKey]; ok {
			ow = &ownerInfo{
				groupVersionKind: typeKey,
				name:             owner.Name,
			}
		}
	}
	return ow
}

func (m *manager) GetRelatedQueueUnit(ctx context.Context, pod *v1.Pod) types.NamespacedName {
	if ann := pod.Annotations[util.RelatedQueueUnitAnnoKey]; ann != "" {
		quInfo := strings.Split(ann, "/")
		if len(quInfo) != 2 {
			return types.NamespacedName{}
		}
		return types.NamespacedName{Namespace: quInfo[0], Name: quInfo[1]}
	}

	owner := m.FindWorkloadFromPod(ctx, pod)
	if owner == nil {
		return types.NamespacedName{}
	}
	handle, ok := m.jobHandles[owner.groupVersionKind]
	if !ok {
		return types.NamespacedName{}
	}
	obj := handle.GetHandle().Object()
	if err := m.cli.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: owner.name}, obj); err != nil {
		return types.NamespacedName{}
	}
	if qu, err := handle.GetHandle().GetRelatedQueueUnit(ctx, obj, m.cli); err != nil {
		return types.NamespacedName{}
	} else {
		return types.NamespacedName{Namespace: qu.Namespace, Name: qu.Name}
	}
}

func (m *manager) GetRelatedPods(ctx context.Context, qu *v1alpha1.QueueUnit) ([]*v1.Pod, error) {
	if qu.Spec.ConsumerRef == nil {
		pl := &v1.PodList{}
		if err := m.cli.List(ctx, pl, client.InNamespace(qu.Namespace), client.MatchingFields{util.RelatedQueueUnitCacheFields: qu.Namespace + "/" + qu.Name}); client.IgnoreNotFound(err) != nil {
			return nil, nil
		}
		pods := []*v1.Pod{}
		for _, p := range pl.Items {
			pods = append(pods, &p)
		}
		return pods, nil
	}
	typeKey := qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind
	handle, ok := m.jobHandles[typeKey]
	if !ok {
		return nil, fmt.Errorf("not supported job type %v", typeKey)
	}
	if f := handle.GetHandle().GetPodsFunc(); f == nil {
		pl := &v1.PodList{}
		if err := m.cli.List(ctx, pl, client.InNamespace(qu.Namespace), client.MatchingFields{util.PodsByOwnersCacheFields: qu.Spec.ConsumerRef.Kind + "/" + qu.Spec.ConsumerRef.Name}); client.IgnoreNotFound(err) != nil {
			return nil, nil
		}
		pods := []*v1.Pod{}
		for _, p := range pl.Items {
			pods = append(pods, &p)
		}
		return pods, nil
	} else {
		return f(ctx, types.NamespacedName{Namespace: qu.Namespace, Name: qu.Spec.ConsumerRef.Name})
	}
}

func (m *manager) GetPodSetName(ctx context.Context, qu *v1alpha1.QueueUnit, p *v1.Pod) string {
	if qu.Spec.ConsumerRef == nil {
		return p.Annotations[util.RelatedPodSetAnnoKey]
	}
	if _, ok := p.Annotations[util.RelatedPodSetAnnoKey]; ok {
		return p.Annotations[util.RelatedPodSetAnnoKey]
	}
	typeKey := qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind
	handle, ok := m.jobHandles[typeKey]
	if !ok {
		return ""
	}
	return handle.GetHandle().GetPodSetName(qu.Spec.ConsumerRef.Name, p)
}
