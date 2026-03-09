package utils

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Resource is a collection of compute resource.
type Resource struct {
	Resources map[v1.ResourceName]int64
}

func (r *Resource) Equal(a *Resource) bool {
	if r == nil && a != nil {
		return false
	}
	if r == nil && a == nil {
		return true
	}
	if a == nil {
		return false
	}
	if len(r.Resources) != len(a.Resources) {
		return false
	}

	for k, v := range r.Resources {
		if a.Resources[k] != v {
			return false
		}
	}
	return true
}

func (r *Resource) Zero() bool {
	if r == nil {
		return true
	}
	for _, v := range r.Resources {
		if v != 0 {
			return false
		}
	}
	return true
}

func (r *Resource) ResourceNames() sets.Set[string] {
	result := sets.New[string]()
	for resourceName := range r.Resources {
		result.Insert(string(resourceName))
	}
	return result
}

func (r *Resource) Resource() map[v1.ResourceName]int64 {
	newRes := make(map[v1.ResourceName]int64)
	for i := range r.Resources {
		newRes[i] = r.Resources[i]
	}
	return newRes
}

func scaleUp(res *Resource, f int) {
	for i := range res.Resources {
		res.Resources[i] *= int64(f)
	}
}

// Update res with all resources in r.
func (res *Resource) Update(r *Resource) {
	for i := range r.Resources {
		res.Resources[i] += r.Resources[i]
	}
}

func (res *Resource) Sub(r *Resource) {
	if r.Zero() {
		return
	}
	for i := range res.Resources {
		if _, ok := r.Resources[i]; !ok {
			continue
		}
		res.Resources[i] -= r.Resources[i]
	}
	for i := range r.Resources {
		if _, ok := res.Resources[i]; !ok {
			res.Resources[i] = -r.Resources[i]
		}
	}
}

// Only resource in res will be updated.
func (r *Resource) AddResource(res *Resource) {
	if res.Zero() {
		return
	}
	for i := range res.Resources {
		if len(r.Resources) == 0 {
			r.Resources = map[v1.ResourceName]int64{}
		}
		r.Resources[i] += res.Resources[i]
	}
}

// NewResource creates a Resource from ResourceList
func NewResource(rl v1.ResourceList) *Resource {
	r := &Resource{Resources: map[v1.ResourceName]int64{}}
	r.Add(rl)
	return r
}

func max(rl, rr *Resource) *Resource {
	res := &Resource{Resources: map[v1.ResourceName]int64{}}
	for k, v := range rl.Resources {
		res.Resources[k] = v
	}
	for k, v := range rr.Resources {
		if res.Resources[k] < v {
			res.Resources[k] = v
		}
	}
	return res
}

// Add adds ResourceList into Resource.
func (r *Resource) Add(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuant := range rl {
		if rName == v1.ResourceCPU {
			r.AddScalar(rName, rQuant.MilliValue())
		} else {
			r.AddScalar(rName, rQuant.Value())
		}
	}
}

// IsHugePageResourceName returns true if the resource name has the huge page
// resource prefix.
func IsHugePageResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}

// ResourceList returns a resource list of this resource.
func (r *Resource) ResourceList() v1.ResourceList {
	result := v1.ResourceList{}
	for rName, rQuant := range r.Resources {
		if IsHugePageResourceName(rName) {
			result[rName] = *resource.NewQuantity(rQuant, resource.BinarySI)
		} else if rName == v1.ResourceCPU {
			result[rName] = *resource.NewMilliQuantity(rQuant, resource.DecimalSI)
		} else if rName == v1.ResourceMemory {
			result[rName] = *resource.NewQuantity(rQuant, resource.BinarySI)
		} else {
			result[rName] = *resource.NewQuantity(rQuant, resource.DecimalSI)
		}
	}
	return result
}

// Clone returns a copy of this resource.
func (r *Resource) Clone() *Resource {
	if r == nil {
		return &Resource{Resources: map[v1.ResourceName]int64{}}
	}
	res := &Resource{Resources: make(map[v1.ResourceName]int64)}
	if r.Resources != nil {
		res.Resources = make(map[v1.ResourceName]int64)
		for k, v := range r.Resources {
			res.Resources[k] = v
		}
	}
	return res
}

// AddScalar adds a resource by a scalar value of this resource.
func (r *Resource) AddScalar(name v1.ResourceName, quantity int64) {
	r.SetScalar(name, r.Resources[name]+quantity)
}

// SetScalar sets a resource by a scalar value of this resource.
func (r *Resource) SetScalar(name v1.ResourceName, quantity int64) {
	// Lazily allocate scalar resource map.
	if r.Resources == nil {
		r.Resources = map[v1.ResourceName]int64{}
	}
	r.Resources[name] = quantity
}

// SetMaxResource compares with ResourceList and takes max value for each Resource.
func (r *Resource) SetMaxResource(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuantity := range rl {
		switch rName {
		case v1.ResourceCPU:
			if cpu := rQuantity.MilliValue(); cpu > r.Resources[rName] {
				r.Resources[rName] = cpu
			}
		default:
			value := rQuantity.Value()
			if value > r.Resources[rName] {
				r.SetScalar(rName, value)
			}
		}
	}
}
