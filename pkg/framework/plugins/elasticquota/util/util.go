package util

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kube-queue/kube-queue/cmd/app/options"
	"github.com/kube-queue/kube-queue/pkg/framework"
	"github.com/kube-queue/kube-queue/pkg/utils"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"
)

func IsJobPreemptible(qu *framework.QueueUnitInfo) bool {
	if qu.Unit.Labels["quota.scheduling.alibabacloud.com/preemptible"] == "" && qu.Unit.Labels["quota.scheduling.koordinator.sh/preemptible"] == "" {
		init, opt := options.DefaultPreemptible()
		return init && opt
	}

	return qu.Unit.Labels["quota.scheduling.alibabacloud.com/preemptible"] == "true" || qu.Unit.Labels["quota.scheduling.koordinator.sh/preemptible"] == "true"
}

func BuildRESTQueueUnit(qu *framework.QueueUnitInfo, quotaName string) apiv1alpha1.QueueUnit {
	unit := qu.Unit
	return apiv1alpha1.QueueUnit{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unit.Name,
			Namespace: unit.Name,
		},
		QuotaName: quotaName,
		QueueName: qu.Queue,
		Request:   utils.ConvertResourceListToString(unit.Spec.Request),
		Resources: utils.ConvertResourceListToString(unit.Spec.Resource),
		PodState: apiv1alpha1.PodState{
			Running: int32(unit.Status.PodState.Running),
			Pending: int32(unit.Status.PodState.Pending),
		},
	}
}
