package utils

import v1 "k8s.io/api/core/v1"

func ConvertResourceListToString(rl v1.ResourceList) map[string]string {
	res := make(map[string]string, len(rl))
	for k, v := range rl {
		res[string(k)] = v.String()
	}
	return res
}
