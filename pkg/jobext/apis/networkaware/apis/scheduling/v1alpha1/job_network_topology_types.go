package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	MustGather                       = "MustGather"
	PreferGather                     = "PreferGather"
	AllRingReduceMode                = "AllRingReduce"
	JobNetworkTopologyNamespace      = "network-topology-job-namespace"
	JobNetworkTopologyName           = "network-topology-job-name"
	JobNetworkWorkerId               = "job-network-topology-worker-id"
	JobNetworkTopologyPermitWaitTime = "job-network-topology-permit-wait-time"
)

type TopologyStrategy struct {
	Layer    string `json:"layer,omitempty"`
	Strategy string `json:"strategy,omitempty"`
}

type JobNetworkTopologySpec struct {
	WorkerNum          int                `json:"workerNum,omitempty"`
	TopologyStrategy   []TopologyStrategy `json:"topologyStrategy,omitempty"` //{PoD:MustGather, ASW:PreferGather..}
	WorkerAffinityMode string             `json:"workerAffinityMode,omitempty"`
}

type TopologyInfo struct {
	TopologyLayer string `json:"topologyLayer,omitempty"`
	TopologyName  string `json:"topologyName,omitempty"`
}

type TopologyDetailInfo struct {
	TopologyInfo       TopologyInfo `json:"topologyInfo,omitempty"`
	ParentTopologyInfo TopologyInfo `json:"parentTopologyInfo,omitempty"`
	NodeNum            int          `json:"nodeNum,omitempty"`
	WorkerNum          int          `json:"workerNum,omitempty"`
}

type JobNetworkTopologyStatus struct {
	TopologyDetailInfo []*TopologyDetailInfo `json:"topologyDetailInfo,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
type JobNetworkTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              JobNetworkTopologySpec   `json:"spec,omitempty"`
	Status            JobNetworkTopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type JobNetworkTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []JobNetworkTopology `json:"items"`
}

func init() {
	SchemeBuilder.Register(&JobNetworkTopology{}, &JobNetworkTopologyList{})
}
