/*
Copyright 2023 The Kubernetes Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +genclient:nonNamespaced
// +genclient:method=GetQueueUnitsSummary,verb=get,subresource=QueueUnits,result=github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1.QueueUnitsSummary
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Summary QueueUnitsSummary `json:"QueueUnitsSummary"`
}

// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Queue `json:"items"`
}

// +genclient
// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +genclient:nonNamespaced
// +genclient:method=GetQueueUnitsSummary,verb=get,subresource=QueueUnits,result=github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1.QueueUnitsSummary
type ElasticQuota struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Summary QueueUnitsSummary `json:"QueueUnitsSummary"`
}

// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ElasticQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Queue `json:"items"`
}

type PodState struct {
	Running int32 `json:"pending"`
	Pending int32 `json:"running"`
}

// QueueUnit is a user-facing representation of a pending workload that summarizes the relevant information for
// position in the cluster queue.
type QueueUnit struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`

	QuotaName string            `json:"quotaName,omitempty"`
	QueueName string            `json:"queueName,omitempty"`
	Request   map[string]string `json:"request,omitempty"`
	Resources map[string]string `json:"resources,omitempty"`
	Phase     string            `json:"phase,omitempty"`
	PodState  PodState          `json:"podState,omitempty"`
}

// +k8s:openapi-gen=true
// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// QueueUnitsSummary contains a list of pending workloads in the context
// of the query (within LocalQueue or ClusterQueue).
type QueueUnitsSummary struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Items []QueueUnit `json:"items"`
}

// +kubebuilder:object:root=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:defaulter-gen=true
// QueueUnitOptions are query params used in the visibility queries
type QueueUnitOptions struct {
	metav1.TypeMeta `json:",inline"`

	Queue string `json:"queue"`

	Phase string `json:"phase"`

	// Offset indicates position of the first pending workload that should be fetched, starting from 0. 0 by default
	Offset int64 `json:"offset"`

	// Limit indicates max number of pending workloads that should be fetched. 1000 by default
	Limit int64 `json:"limit,omitempty"`
}

func init() {
	SchemeBuilder.Register(
		&QueueUnitsSummary{},
		&QueueUnitOptions{},
		&ElasticQuota{},
		&Queue{},
	)
}
