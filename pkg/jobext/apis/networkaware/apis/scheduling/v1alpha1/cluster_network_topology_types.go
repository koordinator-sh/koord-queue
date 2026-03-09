package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	PoDTopologyLayer     = "PoDTopologyLayer"
	SegmentTopologyLayer = "SegmentTopologyLayer"
	ASWTopologyLayer     = "ASWTopologyLayer"
	NodeTopologyLayer    = "NodeTopologyLayer"
)

type NetworkTopologySpec struct {
	ParentTopologyLayer string   `json:"parentTopologyLayer,omitempty"`
	TopologyLayer       string   `json:"topologyLayer,omitempty"`
	LabelKey            []string `json:"labelKey,omitempty"`
}

type ClusterNetworkTopologySpec struct {
	NetworkTopologySpec []NetworkTopologySpec `json:"networkTopologySpec,omitempty"`
}

type ClusterNetworkTopologyDetailStatus struct {
	TopologyInfo       TopologyInfo `json:"topologyInfo,omitempty"`
	ParentTopologyInfo TopologyInfo `json:"parentTopologyInfo,omitempty"`
	ChildTopologyLayer string       `json:"childTopologyLayer,omitempty"`
	ChildTopologyNames []string     `json:"childTopologyNames,omitempty"`
	NodeNum            int          `json:"nodeNum,omitempty"`
}

type ClusterNetworkTopologyStatus struct {
	DetailStatus []*ClusterNetworkTopologyDetailStatus `json:"detailStatus,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
type ClusterNetworkTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ClusterNetworkTopologySpec   `json:"spec,omitempty"`
	Status            ClusterNetworkTopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterNetworkTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterNetworkTopology `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterNetworkTopology{}, &ClusterNetworkTopologyList{})
}
