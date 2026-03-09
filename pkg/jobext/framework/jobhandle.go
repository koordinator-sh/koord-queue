package framework

import "time"

type JobHandle struct {
	typeName       string
	runningTimeout time.Duration
	backoffTime    time.Duration

	genericJobExtension      GenericJobExtension
	requeueJobExtension      RequeueJobExtension
	reservationJobExtension  GenericReservationJobExtension
	networkawareJobExtension NetworkAwareJobExtension

	enableRunningToPending bool
}

func NewJobHandle(rt, bft time.Duration, ext GenericJobExtension, enableRunningToPending bool) JobHandle {
	jh := JobHandle{
		typeName:               ext.GVK().GroupVersion().String() + "/" + ext.GVK().Kind,
		runningTimeout:         rt,
		backoffTime:            bft,
		genericJobExtension:    ext,
		enableRunningToPending: enableRunningToPending,
	}

	if rext, ok := ext.(RequeueJobExtension); ok {
		jh.requeueJobExtension = rext
	}
	if next, ok := ext.SupportNetworkAware(); ok {
		jh.networkawareJobExtension = next
	}
	if rext, ok := ext.SupportReservation(); ok {
		jh.reservationJobExtension = rext
	}
	return jh
}

func (jh *JobHandle) GetJobKey() string {
	return jh.typeName
}

func (jh *JobHandle) GetHandle() GenericJobExtension {
	return jh.genericJobExtension
}
func (jh *JobHandle) GetResvHandle() GenericReservationJobExtension {
	return jh.reservationJobExtension
}
func (jh *JobHandle) GetNetHandle() NetworkAwareJobExtension {
	return jh.networkawareJobExtension
}
