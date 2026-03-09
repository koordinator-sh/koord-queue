package handles

import (
	"fmt"

	commonv1 "github.com/kube-queue/kube-queue/pkg/jobext/apis/common/job_controller/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecheckDeletionTimestamp returns a CanAdopt() function to recheck deletion.
//
// The CanAdopt() function calls getObject() to fetch the latest value,
// and denies adoption attempts if that object has a non-nil DeletionTimestamp.
func RecheckDeletionTimestamp(getObject func() (metav1.Object, error)) func() error {
	return func() error {
		obj, err := getObject()
		if err != nil {
			return fmt.Errorf("can't recheck DeletionTimestamp: %v", err)
		}
		if obj.GetDeletionTimestamp() != nil {
			return fmt.Errorf("%v/%v has just been deleted at %v", obj.GetNamespace(), obj.GetName(), obj.GetDeletionTimestamp())
		}
		return nil
	}
}

func setK8sCondition(old []metav1.Condition, ctype string, status metav1.ConditionStatus, msg string, reason string) (bool, []metav1.Condition) {
	for i, condition := range old {
		now := metav1.Now()
		if condition.Type == ctype && condition.Status != status {
			old[i].Status = status
			old[i].LastTransitionTime = now
			old[i].Message = msg
			old[i].Reason = reason
			return true, old
		} else if condition.Type == ctype && condition.Status == status {
			return false, old
		}
	}
	old = append(old, metav1.Condition{
		Type:               string(ctype),
		LastTransitionTime: metav1.Now(),
		Status:             status,
		Message:            msg,
		Reason:             reason,
	})
	return true, old
}

func setCondition(status *commonv1.JobStatus, t commonv1.JobConditionType, st v1.ConditionStatus, msg string) bool {
	needUpdate := false
	found := false
	if len(status.Conditions) == 0 {
		status.Conditions = make([]commonv1.JobCondition, 0)
	} else {
		for i, condition := range status.Conditions {
			now := metav1.Now()
			if condition.Type == t && condition.Status != st {
				status.Conditions[i].Status = st
				status.Conditions[i].LastUpdateTime = now
				status.Conditions[i].LastTransitionTime = now
				status.Conditions[i].Message = msg
				needUpdate = true
				found = true
				break
			} else if condition.Type == t && condition.Status == st {
				found = true
				needUpdate = false
				break
			}
		}
	}
	if !found {
		now := metav1.Now()
		status.Conditions = append(status.Conditions, commonv1.JobCondition{
			Type:               t,
			Status:             st,
			LastUpdateTime:     now,
			LastTransitionTime: now,
			Message:            msg,
		})
		needUpdate = true
	}
	if len(status.ReplicaStatuses) == 0 {
		status.ReplicaStatuses = make(map[commonv1.ReplicaType]*commonv1.ReplicaStatus)
	}
	return needUpdate
}

func modifyGroupLabelsOrAnnotations(mp map[string]string, key string, suffix string) {
	if value, ok := mp[key]; ok {
		mp[key] = fmt.Sprintf("%v-%v", value, suffix)
	}
}
