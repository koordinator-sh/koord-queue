/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ElasticQuotaTree struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ElasticQuotaTreeSpec defines the Min and Max for Quota.
	// +optional
	Spec ElasticQuotaTreeSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// ElasticQuotaTreeStatus defines the observed use.
	// +optional
	Status ElasticQuotaTreeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ElasticQuotaTreeSpec defines the Min and Max for Quota.
type ElasticQuotaTreeSpec struct {
	Root ElasticQuotaSpec `json:"root,omitempty" protobuf:"bytes,1,opt,name=root"`
}

// ElasticQuotaTreeStatus defines the observed use.
type ElasticQuotaTreeStatus struct {
	Root ElasticQuotaStatus `json:"root,omitempty" protobuf:"bytes,1,rep,name=root"`
}

// ElasticQuotaTreeSpec defines the Min and Max for Quota.
type ElasticQuotaSpec struct {
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	Namespaces []string `json:"namespaces,omitempty" protobuf:"bytes,2,opt,name=namespaces"`

	Attributes map[string]string `json:"attributes,omitempty" protobuf:"bytes,4,rep,name=attributes"`

	Children []ElasticQuotaSpec `json:"children,omitempty" protobuf:"bytes,3,opt,name=children"`

	Min v1.ResourceList `json:"min,omitempty" protobuf:"bytes,4,rep,name=min, casttype=ResourceList,castkey=ResourceName"`

	Max v1.ResourceList `json:"max,omitempty" protobuf:"bytes,5,rep,name=max, casttype=ResourceList,castkey=ResourceName"`
}

// ElasticQuotaTreeSpec defines the Min and Max for Quota.
type ElasticQuotaStatus struct {
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	Namespaces []string `json:"namespaces,omitempty" protobuf:"bytes,2,opt,name=namespaces"`

	Children []ElasticQuotaStatus `json:"children,omitempty" protobuf:"bytes,3,opt,name=children"`

	Used v1.ResourceList `json:"used,omitempty" protobuf:"bytes,4,rep,name=used, casttype=ResourceList,castkey=ResourceName"`

	Request v1.ResourceList `json:"request,omitempty" protobuf:"bytes,5,rep,name=request, casttype=ResourceList,castkey=ResourceName"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ElasticQuotaTreeList is a list of ElasticQuota items.
type ElasticQuotaTreeList struct {
	metav1.TypeMeta `json:",inline"`

	// Standard list metadata.
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is a list of ElasticQuota objects.
	Items []ElasticQuotaTree `json:"items" protobuf:"bytes,2,rep,name=items"`
}
