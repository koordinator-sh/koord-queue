package manageallrayjob

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	queuev1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestManageAllRayJobAdmissions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = env.Stop()
	}()

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, queuev1.AddToScheme, rayv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cli, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	const ns, name, clusterName = "default", "manage-all-rayjob", "manage-all-raycluster"
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Image: "busybox", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}}}}
	job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: rayv1.RayJobSpec{
		Entrypoint: "echo done", Suspend: false, RayClusterSpec: &rayv1.RayClusterSpec{
			HeadGroupSpec:    rayv1.HeadGroupSpec{RayStartParams: map[string]string{}, Template: template},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "cpu", Replicas: ptr.To(int32(1)), RayStartParams: map[string]string{}, Template: template}},
		},
	}}
	if err := cli.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRunning
	job.Status.RayClusterName = clusterName
	job.Status.StartTime = ptr.To(metav1.Now())
	if err := cli.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayJob", Name: name, UID: job.UID}}},
		Spec: *job.Spec.RayClusterSpec.DeepCopy()}
	if err := cli.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, group, node string }{{"head", "headgroup", "test-node"}, {"worker", "cpu", ""}} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: tc.name, Namespace: ns,
			Labels:          map[string]string{"ray.io/group": tc.group},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayCluster", Name: clusterName, UID: cluster.UID}}},
			Spec: *template.Spec.DeepCopy()}
		pod.Spec.NodeName = tc.node
		if err := cli.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, util.PodsByOwnersCacheFields, func(obj client.Object) []string {
		var keys []string
		for _, owner := range obj.GetOwnerReferences() {
			keys = append(keys, fmt.Sprintf("%s/%s", owner.Kind, owner.Name))
		}
		return keys
	}); err != nil {
		t.Fatal(err)
	}
	handle := handles.NewRayJobReconciler(mgr.GetClient(), cfg, scheme, true, "v1", "")
	if err := framework.NewJobReconcilerWithJobExtension(mgr.GetClient(), scheme, handle).SetupWithManager(mgr, 1, 100); err != nil {
		t.Fatal(err)
	}
	if err := framework.NewResourceReporter(mgr.GetClient(), scheme, handle).SetupWithManager(mgr, 1, 100); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mgr.Start(ctx) }()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		qu := &queuev1.QueueUnit{}
		if err := cli.Get(ctx, types.NamespacedName{Namespace: ns, Name: name + "-ray-qu"}, qu); err == nil {
			if qu.Status.Phase == queuev1.Running && len(qu.Status.Admissions) == 2 {
				ads := map[string]queuev1.Admission{}
				for _, ad := range qu.Status.Admissions {
					ads[ad.Name] = ad
				}
				if ads["head"].Running == 1 && ads["head"].Replicas == 1 && ads["cpu"].Running == 0 && ads["cpu"].Replicas == 0 {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	qu := &queuev1.QueueUnit{}
	_ = cli.Get(ctx, types.NamespacedName{Namespace: ns, Name: name + "-ray-qu"}, qu)
	t.Fatalf("admissions not reconstructed from existing Pods: phase=%s admissions=%+v", qu.Status.Phase, qu.Status.Admissions)
}
