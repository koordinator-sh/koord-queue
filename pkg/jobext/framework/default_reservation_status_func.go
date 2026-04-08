package framework

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ReservationStatus(ctx context.Context, obj client.Object, qu *v1alpha1.QueueUnit, resvs []koordinatorschedulerv1alpha1.Reservation) (string, string) {
	msg := map[string]string{}
	allScheduled := true
	allSucceed := true
	log := ctrl.LoggerFrom(ctx).WithValues("job", obj.GetName())
	for _, resv := range resvs {
		scheduled := false
		preempted := false
		hasFalse := false
		for _, cond := range resv.Status.Conditions {
			if cond.Type == "Preempted" && cond.Status == koordinatorschedulerv1alpha1.ConditionStatusTrue {
				preempted = true
			}
			if cond.Type != koordinatorschedulerv1alpha1.ReservationConditionScheduled {
				continue
			}
			reason := strings.Split(cond.Reason, "/")
			if len(reason) != 2 {
				if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
					hasFalse = true
					msg[resv.Name] = cond.Message
				}
				scheduled = true
				continue
			}
			revision, err := strconv.Atoi(reason[1])
			if err != nil {
				if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
					hasFalse = true
					msg[resv.Name] = cond.Message
				}
				scheduled = true
				continue
			}
			if int64(revision) != qu.Status.Attempts {
				continue
			}
			if cond.Status == koordinatorschedulerv1alpha1.ConditionStatusFalse {
				hasFalse = true
				msg[resv.Name] = cond.Message
			}
			scheduled = true
		}
		if os.Getenv("TESTENV") == "true" {
			log.Info("detailed reservation status", "resv", resv.Name, "phase", resv.Status.Phase, "scheduled", scheduled, "preeempted", preempted, "failedScheduling", hasFalse, "cond", resv.Status.Conditions)
		}
		if hasFalse && preempted {
			scheduled = false
			hasFalse = false
		}
		if !scheduled {
			allScheduled = false
		} else if hasFalse {
			allSucceed = false
		}
	}
	retMsg := ""
	limit := 1
	count := 0
	for k, v := range msg {
		retMsg += fmt.Sprintf("{%v:%v},", k, v)
		count++
		if count >= limit {
			retMsg += "..."
			break
		}
	}
	if allScheduled && allSucceed {
		return ReservationStatus_Succeed, ""
	} else if allScheduled && !allSucceed {
		return ReservationStatus_Failed, retMsg
	} else {
		return ReservationStatus_Pending, retMsg
	}
}
