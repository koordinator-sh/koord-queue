package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	LabelResourceType                  = "resource-type"
	LabelNotebookUserId                = "user-id"
	LabelDswBilling                    = "dsw-billing"
	LabelNotebookEnableECI             = GroupName + "/enable-eci"
	LabelNotebookEnableECIIndirectMode = GroupName + "/enable-eci-indirect-mode"
	LabelNotebookPrivateZoneName       = GroupName + "/private-zone-name"
)

// NotebookSpec defines the notebook spec
type NotebookSpec struct {
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Template NotebookTemplateSpec `json:"template"`

	// ImageTemplate is use when saving instance
	// +optional
	ImageTemplate ImageSpec `json:"imageTemplate,omitempty"`
}

// NotebookTemplateSpec defines the notebook template including a PodTemplate
type NotebookTemplateSpec struct {
	Spec corev1.PodSpec `json:"spec"`
}

// NotebookStatus defines the observed state of Notebook
type NotebookStatus struct {
	// ReadyReplicas is the number of Pods created by the Deployment controller that have a Ready Condition.
	ReadyReplicas int32 `json:"readyReplicas"`

	// The status of this instance
	Status string `json:"status"`

	// The message of this instance
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=notebooks,shortName=nb
// +kubebuilder:printcolumn:JSONPath=".status.status",name=Status,type=string
// +kubebuilder:printcolumn:JSONPath=".metadata.creationTimestamp",name=Age,type=date

// Notebook defines the CRD
type Notebook struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NotebookSpec   `json:"spec,omitempty"`
	Status NotebookStatus `json:"status,omitempty"`
}

// IsBeingDeleted returns true if a deletion timestamp is set
func (r *Notebook) IsBeingDeleted() bool {
	return !r.ObjectMeta.DeletionTimestamp.IsZero()
}

// HasFinalizer returns true if the item has the specified finalizer
func (r *Notebook) HasFinalizer(finalizerName string) bool {
	return ContainsString(r.ObjectMeta.Finalizers, finalizerName)
}

// AddFinalizer adds the specified finalizer
func (r *Notebook) AddFinalizer(finalizerName string) {
	r.ObjectMeta.Finalizers = append(r.ObjectMeta.Finalizers, finalizerName)
}

// RemoveFinalizer removes the specified finalizer
func (r *Notebook) RemoveFinalizer(finalizerName string) {
	r.ObjectMeta.Finalizers = RemoveString(r.ObjectMeta.Finalizers, finalizerName)
}

// +kubebuilder:object:root=true

// NotebookList defines the NotebookList CRD
type NotebookList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Notebook `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Notebook{}, &NotebookList{})
}
