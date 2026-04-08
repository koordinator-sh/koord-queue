package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const SchedulerAdmissionLabelKey = "alibabacloud.com/schedule-admission"
const RelatedQueueUnitCacheFields = "koordqueue.relatedqueueunit.cache"
const RelatedQueueUnitPodSetCacheFieldsForTest = "koordqueue.relatedqueueunit.podset.test"
const PodsByOwnersCacheFields = "podsByOwners"
const RelatedQueueUnitAnnoKey = "koord-queue/related-queueunit"
const RelatedAPIVersionKindAnnoKey = "koord-queue/related-apiversion-kind"
const RelatedObjectAnnoKey = "koord-queue/related-object"
const RelatedPodSetAnnoKey = "koord-queue/related-podset-name"

func SetPodTemplateSpec(pts *corev1.PodTemplateSpec, jobNamespace, jobName, podset, suffix string) {
	if len(pts.Annotations) == 0 {
		pts.Annotations = make(map[string]string)
	}
	if suffix != "" {
		pts.Annotations[RelatedQueueUnitAnnoKey] = fmt.Sprintf("%s/%s-%s", jobNamespace, jobName, suffix)
	} else {
		pts.Annotations[RelatedQueueUnitAnnoKey] = fmt.Sprintf("%s/%s", jobNamespace, jobName)
	}
	pts.Annotations[RelatedPodSetAnnoKey] = podset
	if len(pts.Labels) == 0 {
		pts.Labels = make(map[string]string)
	}
	pts.Labels[SchedulerAdmissionLabelKey] = "false"
}

func GenQueueUnitAnnoKey(queueUnit *v1alpha1.QueueUnit) string {
	return fmt.Sprintf("%s/%s", queueUnit.Namespace, queueUnit.Name)
}

func UpdateQueueUnitStatus(queueUnit *v1alpha1.QueueUnit, status v1alpha1.QueueUnitPhase, msg string, cli client.Client) error {
	newQu := queueUnit
	newQu.Status.Phase = status
	newQu.Status.LastUpdateTime = &v1.Time{Time: time.Now()}
	newQu.Status.Message = msg
	if status == v1alpha1.Succeed || status == v1alpha1.Failed {
		newQu.Status.Admissions = []v1alpha1.Admission{}
	}

	return cli.Status().Update(context.Background(), newQu)
}

func GetQueueUnitForSSA(queueUnit *v1alpha1.QueueUnit) *v1alpha1.QueueUnit {
	return &v1alpha1.QueueUnit{
		TypeMeta: queueUnit.TypeMeta,
		ObjectMeta: v1.ObjectMeta{
			Name:      queueUnit.Name,
			Namespace: queueUnit.Namespace,
		},
	}
}

func ToQueueUnit(obj client.Object) (*v1alpha1.QueueUnit, error) {
	switch t := obj.(type) {
	case *v1alpha1.QueueUnit:
		return t, nil
	// case cache.DeletedFinalStateUnknown:
	// 	if qu, ok := t.Obj.(*v1alpha1.QueueUnit); ok {
	// 		return qu, nil
	// 	}
	// 	return nil, fmt.Errorf("invalid type %T", t.Obj)
	default:
		return nil, fmt.Errorf("invalid type %T", t)
	}
}

func AdmissionReplicaString(ads []v1alpha1.Admission) string {
	strs := make([]string, 0)
	for _, ad := range ads {
		strs = append(strs, fmt.Sprintf("%s:%d", ad.Name, ad.Replicas))
	}
	return strings.Join(strs, ",")
}
