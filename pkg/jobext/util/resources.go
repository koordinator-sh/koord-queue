package util

import corev1 "k8s.io/api/core/v1"

// PodRequestsAndLimits returns a dictionary of all defined resources summed up for all
// containers of the pod. If pod overhead is non-nil, the pod overhead is added to the
// total container resource requests and to the total container limits which have a
// non-zero quantity.
func PodRequestsAndLimits(pod *corev1.PodTemplateSpec) (reqs corev1.ResourceList) {
	reqs = corev1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		AddResourceList(reqs, container.Resources.Requests)
	}
	// init containers define the minimum of any resource
	for _, container := range pod.Spec.InitContainers {
		maxResourceList(reqs, container.Resources.Requests)
	}

	// Add overhead for running a pod to the sum of requests and to non-zero limits:
	if pod.Spec.Overhead != nil {
		AddResourceList(reqs, pod.Spec.Overhead)
	}
	return
}

// PodRequestsAndLimits returns a dictionary of all defined resources summed up for all
// containers of the pod. If pod overhead is non-nil, the pod overhead is added to the
// total container resource requests and to the total container limits which have a
// non-zero quantity.
func GetPodRequestsAndLimits(pod *corev1.PodSpec) (reqs corev1.ResourceList) {
	reqs = corev1.ResourceList{}
	for _, container := range pod.Containers {
		if len(container.Resources.Requests) == 0 && len(container.Resources.Limits) != 0 {
			AddResourceList(reqs, container.Resources.Limits)
		} else {
			AddResourceList(reqs, container.Resources.Requests)
		}
	}
	// init containers define the minimum of any resource
	for _, container := range pod.InitContainers {
		if len(container.Resources.Requests) == 0 && len(container.Resources.Limits) != 0 {
			maxResourceList(reqs, container.Resources.Limits)
		} else {
			maxResourceList(reqs, container.Resources.Requests)
		}
	}

	// Add overhead for running a pod to the sum of requests and to non-zero limits:
	if pod.Overhead != nil {
		AddResourceList(reqs, pod.Overhead)
	}
	return
}

// addResourceList adds the resources in newList to list
func AddResourceList(list, new corev1.ResourceList) {
	for name, quantity := range new {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

// maxResourceList sets list to the greater of list/newList for every resource
// either list
func maxResourceList(list, new corev1.ResourceList) {
	for name, quantity := range new {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
			continue
		} else {
			if quantity.Cmp(value) > 0 {
				list[name] = quantity.DeepCopy()
			}
		}
	}
}

func Equal(r1, r2 corev1.ResourceList) bool {
	if len(r1) != len(r2) {
		return false
	}
	for k, v := range r1 {
		if v2, ok := r2[k]; !ok || !v.Equal(v2) {
			return false
		}
	}
	return true
}
