package main

/*
Copyright 2024 The Koord-Queue Authors.

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

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	schv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	elasticquotatree "github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota"
	admissioncontroller "github.com/koordinator-sh/koord-queue/pkg/jobext/admission"
	networkv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/jobext/apis/networkaware/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/registry"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/reservation"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"github.com/koordinator-sh/koord-queue/pkg/nscontroller"
	"gopkg.in/yaml.v3"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

// ControllerOptions holds the options for the controllers
type ControllerOptions struct {
	KubeConfig string
	QPS        float64
	Burst      int

	MetricsAddr string
	ProbeAddr   string

	LeaderElect     bool
	LeaderNamespace string

	// Job Extensions options
	EnableJobExtensions bool
	EnabledExtensions   string
	EnableReservation   bool
	ManageAllJobs       bool
	ConfigPath          string
	Workers             int
	WQQPS               int
	EnableNetworkAware  bool
	EnablePodReclaim    bool
}

type Arguments map[string]string

func LoadArguments(path string) Arguments {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("failed to load arguments, path:%v, err:%v", path, err)
	}
	arg := make(map[string]interface{})
	err = yaml.Unmarshal(b, arg)
	if err != nil {
		log.Fatalf("failed to load arguments, path:%v, content:\n%v\n err:%v\n", path, string(b), err)
	}
	log.Printf("load arguments, path:%v, content:\n%v\n", path, string(b))
	res := map[string]string{}
	for k, v := range arg {
		o, _ := yaml.Marshal(v)
		res[k] = string(o)
	}
	log.Printf("loaded arguments, argument:\n%v\n", arg)
	return res
}

func main() {
	opt := &ControllerOptions{}

	flag.Float64Var(&opt.QPS, "qps", 300, "QPS when communicating with apiserver")
	flag.IntVar(&opt.Burst, "burst", 300, "Burst when communicating with apiserver")
	flag.StringVar(&opt.MetricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to")
	flag.StringVar(&opt.ProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to")
	flag.BoolVar(&opt.LeaderElect, "leader-elect", false, "Enable leader election for controller manager")
	flag.StringVar(&opt.LeaderNamespace, "leader-namespace", "koord-queue", "Namespace of the leader election resource")

	// Job Extensions flags
	flag.BoolVar(&opt.EnableJobExtensions, "enable-job-extensions", true, "Enable job extension controllers")
	flag.StringVar(&opt.EnabledExtensions, "enabled-extensions", "job", "Enabled job extensions (comma-separated)")
	flag.BoolVar(&opt.ManageAllJobs, "manage-all-jobs", false, "Manage all jobs, not only those submitted with suspend")
	flag.StringVar(&opt.ConfigPath, "config", "", "Path to the config file")
	flag.IntVar(&opt.Workers, "workers", 20, "Number of workers for each controller")
	flag.IntVar(&opt.WQQPS, "wqqps", 100, "Work queue QPS")
	flag.BoolVar(&opt.EnablePodReclaim, "enable-pod-reclaim", false, "Enable pod reclaim")
	flag.DurationVar(&framework.DefaultRequeuePeriod, "default-requeue-period", time.Minute, "Default requeue period")
	flag.BoolVar(&opt.EnableReservation, "enable-reservation", false, "Enable reservation controller")
	// inner
	flag.BoolVar(&opt.EnableNetworkAware, "enable-network-aware", false, "Enable network aware scheduling")

	klog.InitFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{Development: true})))

	if f := flag.Lookup("kubeconfig"); f != nil {
		opt.KubeConfig = f.Value.String()
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		setupLog.Error(err, "unable to build kubeconfig")
		os.Exit(1)
	}
	cfg.QPS = float32(opt.QPS)
	cfg.Burst = opt.Burst

	if err := run(cfg, opt); err != nil {
		setupLog.Error(err, "problem running controllers")
		os.Exit(1)
	}
}

func run(cfg *rest.Config, opt *ControllerOptions) error {
	var arg Arguments
	if opt.ConfigPath != "" {
		arg = LoadArguments(opt.ConfigPath)
	}

	// Set framework global flags
	framework.EnableReservation = opt.EnableReservation
	framework.EnableNetworkAware = opt.EnableNetworkAware
	framework.EnablePodReclaim = opt.EnablePodReclaim

	leaderElectionID := "koord-queue-controllers"
	if opt.EnableJobExtensions && opt.EnabledExtensions != "" {
		leaderElectionID = "koord-queue-controllers-" + strings.ReplaceAll(opt.EnabledExtensions, ",", "-")
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			SecureServing: false,
			BindAddress:   opt.MetricsAddr,
		},
		HealthProbeBindAddress:  opt.ProbeAddr,
		LeaderElection:          opt.LeaderElect,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: opt.LeaderNamespace,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {Field: fields.AndSelectors(
					fields.OneTermNotEqualSelector("status.phase", "Succeeded"),
					fields.OneTermNotEqualSelector("status.phase", "Failed"))},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("unable to create manager: %w", err)
	}

	// Add schemes
	if opt.EnableReservation {
		koordinatorschedulerv1alpha1.AddToScheme(mgr.GetScheme())
	}
	if opt.EnableNetworkAware {
		networkv1alpha1.AddToScheme(mgr.GetScheme())
	}
	schv1alpha1.AddToScheme(mgr.GetScheme())

	// Setup cache indexes
	if err := setupCacheIndexes(mgr); err != nil {
		return err
	}

	// Setup Namespace Controller
	if err := setupNamespaceController(mgr); err != nil {
		return err
	}

	// Setup Job Extension Controllers
	if opt.EnableJobExtensions && opt.EnabledExtensions != "" {
		if err := setupJobExtensionControllers(mgr, opt, arg); err != nil {
			return err
		}
	}

	// Add health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting controllers manager")
	return mgr.Start(ctrl.SetupSignalHandler())
}

func setupCacheIndexes(mgr ctrl.Manager) error {
	// Namespace index for available quotas
	if err := mgr.GetCache().IndexField(context.Background(), &corev1.Namespace{}, "availableQuotas", func(o client.Object) []string {
		ns, ok := o.(*corev1.Namespace)
		if !ok {
			return nil
		}
		return elasticquotatree.GetAvailableQuotasInNamespace(ns)
	}); err != nil {
		return fmt.Errorf("failed to set namespace index: %w", err)
	}

	// Pod index by owners
	if err := mgr.GetCache().IndexField(context.Background(), &corev1.Pod{}, util.PodsByOwnersCacheFields, func(o client.Object) []string {
		p, ok := o.(*corev1.Pod)
		if !ok {
			return nil
		}
		res := []string{}
		for _, owner := range p.OwnerReferences {
			res = append(res, fmt.Sprintf("%v/%v", owner.Kind, owner.Name))
		}
		return res
	}); err != nil {
		return fmt.Errorf("failed to set pod owners index: %w", err)
	}

	// Pod index by related queue unit
	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &corev1.Pod{}, util.RelatedQueueUnitCacheFields, func(o client.Object) []string {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return []string{}
		}
		quInfo := pod.Annotations[util.RelatedQueueUnitAnnoKey]
		return []string{quInfo}
	}); err != nil {
		return fmt.Errorf("failed to set related queue unit index: %w", err)
	}

	return nil
}

func setupNamespaceController(mgr ctrl.Manager) error {
	if err := (&nscontroller.NamespaceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create namespace controller: %w", err)
	}
	setupLog.Info("namespace controller setup complete")
	return nil
}

func setupJobExtensionControllers(mgr ctrl.Manager, opt *ControllerOptions, arg Arguments) error {
	exts := strings.Split(opt.EnabledExtensions, ",")
	jobHandles := []framework.JobHandle{}
	jhm := map[string]framework.JobHandle{}

	for _, ext := range exts {
		f, ok := registry.Registry[ext]
		if !ok {
			setupLog.Info("skipping unsupported job type", "type", ext)
			continue
		}
		handle := f(mgr.GetClient(), mgr.GetConfig(), mgr.GetScheme(), opt.ManageAllJobs, arg[ext])
		jobHandles = append(jobHandles, handle)
		jhm[handle.GetJobKey()] = handle
		setupLog.Info("registered job extension", "type", ext)
	}

	if len(jobHandles) == 0 {
		setupLog.Info("no job extensions enabled")
		return nil
	}

	// Setup main job reconciler
	reconciler := framework.NewJobReconcilerWithJobExtension(mgr.GetClient(), mgr.GetScheme(), jobHandles...)
	if err := reconciler.SetupWithManager(mgr, opt.Workers, opt.WQQPS); err != nil {
		return fmt.Errorf("unable to create job reconciler: %w", err)
	}

	// Setup resource reporter
	if err := framework.NewResourceReporter(mgr.GetClient(), mgr.GetScheme(), jobHandles...).SetupWithManager(mgr, opt.Workers, opt.WQQPS); err != nil {
		return fmt.Errorf("unable to create resource reporter: %w", err)
	}

	// Setup admission controller
	if err := admissioncontroller.NewAdmissionController(mgr.GetClient(), jhm).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create admission controller: %w", err)
	}

	// Setup reservation controller
	if opt.EnableReservation {
		if err := reservation.NewReservationController(mgr.GetClient(), mgr.GetEventRecorderFor("ResvCtrl"), jobHandles...).SetupWithManager(mgr, opt.Workers, opt.WQQPS); err != nil {
			return fmt.Errorf("unable to create reservation controller: %w", err)
		}
	}

	setupLog.Info("job extension controllers setup complete", "extensions", opt.EnabledExtensions)
	return nil
}
