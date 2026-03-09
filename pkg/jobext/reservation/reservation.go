package reservation

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	"github.com/kube-queue/kube-queue/pkg/jobext/util"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type blacklistNode struct {
	LastUpdateTimestamp time.Time `json:"lastUpdateTimestamp,omitempty"`
	ErrorMsg            string    `json:"errorMsg,omitempty"`
	ErrorCode           int       `json:"errorCode,omitempty"`
}

var lastConfigMapData string

func (r *ReservationController) GetJobBlackList(ctx context.Context, log logr.Logger) ([]corev1.NodeSelectorRequirement, string) {
	blacklistcm := &corev1.ConfigMap{}
	err := r.cli.Get(ctx, types.NamespacedName{Name: "kubedl-blacklist-data", Namespace: "kube-system"}, blacklistcm)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "GetJobBlackList error")
		}
		return nil, ""
	}
	data := blacklistcm.Data["global_anomalousNodesGlobal_xingyun-aiph"]
	blackList := map[string][]blacklistNode{}
	err = json.Unmarshal([]byte(data), &blackList)
	if err != nil {
		log.Error(err, "GetJobBlackList error", "data", data)
		return nil, ""
	}
	var nodeSelectorTerms []corev1.NodeSelectorRequirement
	now := time.Now()
	for nodeName, details := range blackList {
		latest := now
		for _, detail := range details {
			if detail.LastUpdateTimestamp.After(latest) {
				latest = detail.LastUpdateTimestamp
			}
		}
		if now.Sub(latest) > 12*time.Hour-10*time.Second {
			continue
		}
		nodeSelectorTerms = append(nodeSelectorTerms, corev1.NodeSelectorRequirement{
			Key:      "kubernetes.io/hostname",
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   []string{nodeName},
		})
	}
	lastConfigMapData = data
	if !reflect.DeepEqual(lastConfigMapData, data) {
		log.V(1).Info("GetJobBlackList update blacklist", "blacklist", data, "decodedRequirements", nodeSelectorTerms)
	}
	return nodeSelectorTerms, blacklistcm.ResourceVersion
}

func (r *ReservationController) ReconcileReservations(ctx context.Context, log logr.Logger, object client.Object, queueUnit *v1alpha1.QueueUnit) (needRequeue bool, resvStatus, msg string, err error) {
	jobTypeKey := queueUnit.Spec.ConsumerRef.APIVersion + "/" + queueUnit.Spec.ConsumerRef.Kind
	if r.reservationHandles[jobTypeKey] == nil {
		return false, "", "", fmt.Errorf("unsupported job type %v for reservation", jobTypeKey)
	}
	reservationHandle := r.reservationHandles[jobTypeKey]
	resvs, err := reservationHandle.Reservation(ctx, object)
	if err != nil {
		util.UpdateQueueUnitStatus(queueUnit, v1alpha1.SchedReady, err.Error(), r.cli)
		return false, "", "", err
	}

	blacklist, rv := r.GetJobBlackList(ctx, log)
	for i := range resvs {
		if resvs[i].Labels == nil {
			resvs[i].Labels = map[string]string{}
		}
		resvs[i].Labels["reserved-job-namespace"] = object.GetNamespace()
		resvs[i].Labels["reserved-job-name"] = object.GetName()
		resvs[i].Labels["total-reservations"] = fmt.Sprintf("%v", len(resvs))
		if resvs[i].Annotations == nil {
			resvs[i].Annotations = map[string]string{}
		}
		if len(queueUnit.OwnerReferences) > 0 {
			resvs[i].Annotations["scheduling.x-k8s.io/owner-apiversion"] = queueUnit.OwnerReferences[0].APIVersion
			resvs[i].Annotations["scheduling.x-k8s.io/owner-kind"] = queueUnit.OwnerReferences[0].Kind
			resvs[i].Annotations["scheduling.x-k8s.io/owner-name"] = queueUnit.OwnerReferences[0].Name
			resvs[i].Annotations["scheduling.x-k8s.io/owner-uid"] = string(queueUnit.OwnerReferences[0].UID)
		}
		resvs[i].Annotations["reserved-queueunit-name"] = queueUnit.Name
		resvs[i].Annotations["reserved-job-type"] = jobTypeKey
		resvs[i].Spec.TTL = &metav1.Duration{Duration: 30 * time.Minute}
		if len(blacklist) > 0 {
			resvs[i].Annotations["blacklist-rv"] = rv
			if resvs[i].Spec.Template != nil {
				if resvs[i].Spec.Template.Spec.Affinity == nil {
					resvs[i].Spec.Template.Spec.Affinity = &corev1.Affinity{}
				}
				if resvs[i].Spec.Template.Spec.Affinity.NodeAffinity == nil {
					resvs[i].Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
				}
				if resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
					resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
				}
				if len(resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
					resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []corev1.NodeSelectorTerm{}
				}
				for idx, term := range resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
					resvs[i].Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[idx].MatchExpressions = append(term.MatchExpressions, blacklist...)
				}
			}
		}
	}
	existingResvs := &koordinatorschedulerv1alpha1.ReservationList{}
	r.cli.List(ctx, existingResvs, client.MatchingLabels{
		"reserved-job-namespace": object.GetNamespace(),
		"reserved-job-name":      object.GetName(),
	})
	if len(existingResvs.Items) == 0 && len(resvs) > 0 {
		log.V(0).Info("no reservation found, try to create reservations for job")
		return true, "", "", r.CreateReservations(ctx, log, resvs)
	}
	if len(existingResvs.Items) > 0 && len(resvs) == 0 {
		log.V(0).Info("no reservation found, try to delete reservations for job")
		return true, "", "", r.cli.DeleteAllOf(ctx, &koordinatorschedulerv1alpha1.Reservation{},
			client.MatchingLabels{"reserved-job-namespace": object.GetNamespace(), "reserved-job-name": object.GetName()})
	}
	if len(existingResvs.Items) == 0 && len(resvs) == 0 {
		log.V(0).Info("reservation found, try to update reservations for job")
		return false, framework.ReservationStatus_Succeed, "No reservation needed", nil
	}
	deleted := false
	for _, existingResv := range existingResvs.Items {
		if existingResv.Status.Phase == "Failed" {
			deleted = true
			r.cli.Delete(ctx, &existingResv)
		}
	}
	if deleted {
		return true, "", "", nil
	}

	// We only check if blacklist has changed, if not, we don't need to update the reservation
	updated, err := r.CreateOrUpdateReservation(ctx, log, resvs, existingResvs.Items)
	if err != nil {
		return false, "", "", err
	}
	if updated {
		return true, "", "", nil
	}
	resvStatus, msg = reservationHandle.ReservationStatus(ctx, object, queueUnit, existingResvs.Items)
	return false, resvStatus, msg, nil
}

func (r *ReservationController) ReconcileJobNetworkTopology(ctx context.Context, log logr.Logger, object client.Object, qu *v1alpha1.QueueUnit) error {
	jobTypeKey := qu.Spec.ConsumerRef.APIVersion + "/" + qu.Spec.ConsumerRef.Kind
	networkTopologyHandle, ok := r.networktopologyHandles[jobTypeKey]
	if !ok {
		return nil
	}
	jn, err := networkTopologyHandle.GetJobNetworkTopologyCR(ctx, object, r.cli)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "get network topology error", "job", klog.KObj(jn), "err", err)
			return err
		}
		return nil
	}
	if jn == nil || jn.Name == "" {
		return nil
	}

	jn.ObjectMeta = metav1.ObjectMeta{
		Namespace:       jn.Namespace,
		Name:            qu.Name + "-" + framework.Suffix,
		Labels:          jn.Labels,
		Annotations:     jn.Annotations,
		OwnerReferences: jn.OwnerReferences,
	}
	if jn.Labels == nil {
		jn.Labels = map[string]string{}
	}
	jn.Labels["reserved-job-namespace"] = object.GetNamespace()
	jn.Labels["reserved-job-name"] = object.GetName()
	log.V(3).Info("update jobnetworktopology status for job", "namespacename", klog.KObj(jn))
	err = r.cli.Create(ctx, jn)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		log.Error(err, "create network topology error", "job", klog.KObj(jn), "err", err)
		return err
	}
	return nil
}
