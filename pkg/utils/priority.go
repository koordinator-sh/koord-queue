package utils

func GetPriority(prio *int32) int32 {
	if prio == nil {
		return 0
	}
	return *prio
}
