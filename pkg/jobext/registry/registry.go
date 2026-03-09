package registry

import (
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func reconcilerWithVersion(
	f func(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, version string, args string) framework.JobHandle,
	version string) func(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	return func(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
		return f(cli, config, scheme, managedAllJobs, version, args)
	}
}

var Registry = map[string]func(cli client.Client, config *rest.Config, scheme *runtime.Scheme, manageAllJobs bool, args string) framework.JobHandle{
	"tfjob":                  handles.NewTfJobReconciler,
	"pytorchjob":             handles.NewPytorchJobReconciler,
	"job":                    handles.NewJobReconciler,
	"mpijob":                 handles.NewMpiV1alpha1JobReconciler,
	"rayjob":                 reconcilerWithVersion(handles.NewRayJobReconciler, "v1"),
	"rayjobv1alpha1":         reconcilerWithVersion(handles.NewRayJobReconciler, "v1alpha1"),
	"sparkapp":               handles.NewSparkAppReconciler,
	"raycluster":             handles.NewRayClusterReconciler,
	"clusterresourcebinding": handles.NewClusterResourceBindingController,
	"workflow":               handles.NewWFReconciler,
}
