package framework

import (
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRayClusterOwnerRequest(t *testing.T) {
	tests := []struct {
		name    string
		version string
		owners  []metav1.OwnerReference
		want    bool
	}{
		{"v1 owner", "v1", []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayJob", Name: "job"}}, true},
		{"v1alpha1 owner", "v1alpha1", []metav1.OwnerReference{{APIVersion: "ray.io/v1alpha1", Kind: "RayJob", Name: "job"}}, true},
		{"wrong version", "v1", []metav1.OwnerReference{{APIVersion: "ray.io/v1alpha1", Kind: "RayJob", Name: "job"}}, false},
		{"unrelated owner", "v1", []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayService", Name: "service"}}, false},
		{"no owner", "v1", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", OwnerReferences: tt.owners}}
			req, ok := rayClusterOwnerRequest(cluster, tt.version)
			if ok != tt.want {
				t.Fatalf("got request=%v, want=%v", ok, tt.want)
			}
			if ok && (req.Namespace != "ray.io/"+tt.version+"/RayJob|ns" || req.Name != "job") {
				t.Fatalf("unexpected request: %v", req)
			}
		})
	}
}

func TestRayJobTerminalRequeueAfter(t *testing.T) {
	tests := []struct {
		name string
		job  client.Object
		want time.Duration
	}{
		{"completed v1", &rayv1.RayJob{Status: rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusComplete}}, 5 * time.Minute},
		{"failed v1", &rayv1.RayJob{Status: rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusFailed}}, 5 * time.Minute},
		{"running v1", &rayv1.RayJob{Status: rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusRunning}}, 0},
		{"completed v1alpha1", &rayv1alpha1.RayJob{Status: rayv1alpha1.RayJobStatus{JobDeploymentStatus: rayv1alpha1.JobDeploymentStatusComplete}}, 5 * time.Minute},
		{"other job", &rayv1.RayCluster{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rayJobTerminalRequeueAfter(tt.job); got != tt.want {
				t.Fatalf("requeue after = %s, want %s", got, tt.want)
			}
		})
	}
}
