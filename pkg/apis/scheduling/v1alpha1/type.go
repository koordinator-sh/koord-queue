/*

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +crd
// +kubebuilder:subresource:status
// +groupName=scheduling.x-k8s.io
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,name=metadata"`

	Spec   QueueSpec   `json:"spec,omitempty" protobuf:"bytes,2,name=spec"`
	Status QueueStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// QueueSpec defines the desired state of Queue
type QueueSpec struct {
	QueuePolicy       QueuePolicy                  `json:"queuePolicy,omitempty" protobuf:"bytes,1,opt,name=queuePolicy"`
	Priority          *int32                       `json:"priority,omitempty" protobuf:"varint,2,opt,name=priority"`
	PriorityClassName string                       `json:"priorityClassName,omitempty" protobuf:"bytes,3,opt,name=priorityClassName"`
	AdmissionChecks   []AdmissionCheckWithSelector `json:"admissionChecks,omitempty" protobuf:"bytes,4,opt,name=admissionChecks"`
}

type AdmissionCheckWithSelector struct {
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	// we will check if queue unit match the selector, and we will only add admissionCheckState if match
	Selector *metav1.LabelSelector `json:"labelSelector,omitempty" protobuf:"bytes,2,opt,name=labelSelector"`
}

// QueueStatus defines the observed state of Queue
type QueueStatus struct {
	// TODO
	QueueItemDetails map[string][]QueueItemDetail `json:"queueItemDetails,omitempty" protobuf:"bytes,1,opt,name=queueItemDetails"`
}

type QueueItemDetail struct {
	Name      string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,opt,name=namespace"`
	Priority  int32  `json:"priority,omitempty" protobuf:"varint,3,opt,name=priority"`
	Position  int32  `json:"position,omitempty" protobuf:"varint,4,opt,name=position"`
}

// +k8s:openapi-gen=true
// QueuePolicy defines the queueing policy for the elements in the queue
type QueuePolicy string

const (
	QueuePolicyFIFO     QueuePolicy = "FIFO"
	QueuePolicyPriority QueuePolicy = "Priority"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// QueueUnitList contains a list of QueueUnit
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Queue `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +crd
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={qu,qus}
// +groupName=scheduling.x-k8s.io
// +kubebuilder:printcolumn:JSONPath=.status.phase,name=PHASE,type=string
// +kubebuilder:printcolumn:JSONPath=.spec.priority,name=PRIORITY,type=integer
// +kubebuilder:printcolumn:JSONPath=.status.admissions,name=ADMISSIONS,type=string
// +kubebuilder:printcolumn:JSONPath=.spec.consumerRef.kind,name=JOBTYPE,type=string
type QueueUnit struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,name=metadata"`

	Spec   QueueUnitSpec   `json:"spec,omitempty" protobuf:"bytes,2,name=spec"`
	Status QueueUnitStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// QueueUnitSpec defines the desired state of QueueUnit
type QueueUnitSpec struct {
	ConsumerRef *corev1.ObjectReference `json:"consumerRef,omitempty" protobuf:"bytes,1,opt,name=consumerRef"`
	Priority    *int32                  `json:"priority,omitempty" protobuf:"varint,2,opt,name=priority"`
	Queue       string                  `json:"queue,omitempty" protobuf:"bytes,3,opt,name=queue"`
	Resource    corev1.ResourceList     `json:"resource,omitempty" protobuf:"bytes,4,name=resource"`
	// podSets is a list of sets of homogeneous pods, each described by a Pod spec
	// and a count.
	// There must be at least one element and at most 8.
	// podSets cannot be changed.
	//
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:MinItems=1
	PodSets           []kueue.PodSet `json:"podSet,omitempty" protobuf:"bytes,5,name=podSet"`
	PriorityClassName string         `json:"priorityClassName,omitempty" protobuf:"bytes,6,opt,name=priorityClassName"`

	Request corev1.ResourceList `json:"request,omitempty" protobuf:"bytes,7,opt,name=queue"`
}

type ReclaimState struct {
	Replicas int64 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`
}

type Admission struct {
	Name      string              `json:"name" protobuf:"bytes,1,opt,name=name"`
	Replicas  int64               `json:"replicas" protobuf:"varint,2,opt,name=replicas"`
	Resources corev1.ResourceList `json:"resources,omitempty" protobuf:"bytes,3,name=resources"`

	Running int64 `json:"running" protobuf:"varint,4,opt,name=running"`

	ReclaimState *ReclaimState `json:"reclaimState,omitempty" protobuf:"bytes,5,opt,name=reclaimState"`
}

// QueueUnitStatus defines the observed state of QueueUnit
type QueueUnitStatus struct {
	Phase QueueUnitPhase `json:"phase" protobuf:"bytes,1,name=phase"`
	// +optional
	Attempts int64 `json:"attempts" protobuf:"varint,2,opt,name=attempts"`
	// +optional
	Message        string       `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	LastUpdateTime *metav1.Time `json:"lastUpdateTime" protobuf:"bytes,4,name=lastUpdateTime"`
	// admissionChecks list all the admission checks required by the workload and the current status
	// +optional
	// +listType=map
	// +listMapKey=name
	// +patchStrategy=merge
	// +patchMergeKey=name
	// +kubebuilder:validation:MaxItems=8
	AdmissionChecks []kueue.AdmissionCheckState `json:"admissionChecks,omitempty" patchStrategy:"merge" patchMergeKey:"name"`

	PodState PodState `json:"podState,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=name
	// +patchStrategy=merge
	// +patchMergeKey=name
	// +kubebuilder:validation:MaxItems=32
	Admissions       []Admission  `json:"admissions" patchStrategy:"merge" patchMergeKey:"name"`
	LastAllocateTime *metav1.Time `json:"lastAllocateTime,omitempty"`
}

type PodState struct {
	Running int `json:"running,omitempty"`
	Pending int `json:"pending,omitempty"`
}

type QueueUnitPhase string

const (
	Enqueued     QueueUnitPhase = "Enqueued"
	Reserved     QueueUnitPhase = "Reserved"
	Dequeued     QueueUnitPhase = "Dequeued"
	Running      QueueUnitPhase = "Running"
	Succeed      QueueUnitPhase = "Succeed"
	Failed       QueueUnitPhase = "Failed"
	SchedReady   QueueUnitPhase = "SchedReady"
	SchedSucceed QueueUnitPhase = "SchedSucceed"
	SchedFailed  QueueUnitPhase = "SchedFailed"
	Backoff      QueueUnitPhase = "TimeoutBackoff"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QueueUnitList contains a list of QueueUnit
type QueueUnitList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueueUnit `json:"items"`
}

// Suspend is a flag that instructs the job operator to suspend processing this job
const Suspend = "scheduling.x-k8s.io/suspend"

// Placement is the scheduling result of the scheduler
const Placement = "scheduling.x-k8s.io/placement"

// JobSuspended checks if a Job should be suspended by checking whether its annotation contains key Suspend and its
// value is set "true"
func JobSuspended(job metav1.Object) bool {
	const suspended = "true"
	annotations := job.GetAnnotations()
	if annotations != nil {
		if val, exist := annotations[Suspend]; exist {
			return val == suspended
		}
	}
	return false
}
