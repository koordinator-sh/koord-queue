package overadmission_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	listerv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	ctrl "github.com/koordinator-sh/koord-queue/pkg/controller"
	"github.com/koordinator-sh/koord-queue/pkg/controllers"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	eqversioned "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/client/clientset/versioned"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/priority"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	jobextframework "github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	jobextutil "github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"github.com/koordinator-sh/koord-queue/pkg/queue"
	"github.com/koordinator-sh/koord-queue/pkg/queue/multischedulingqueue"
	"github.com/koordinator-sh/koord-queue/pkg/scheduler"
	"github.com/koordinator-sh/koord-queue/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	crctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// This suite verifies that wait-for-pods-running queues cannot over-admit:
// the full kube-queue scheduling stack (queue + scheduler + ElasticQuotaV2 plugin) runs against
// envtest together with the jobext ResourceReporter, which counts scheduled-but-not-running pods
// as Running and releases the queue's assumed slot early. Even with that early release, a second
// unit must stay Enqueued while the first unit still holds the whole quota.

var (
	ctx    context.Context
	cancel context.CancelFunc

	testEnv *envtest.Environment
	cfg     *rest.Config

	fw    framework.Framework
	cli   versioned.Interface
	eqcli eqversioned.Interface

	controller *ctrl.Controller

	crClient client.Client // controller-runtime client backed by the ResourceReporter manager

	queueUnitInformerFactory externalversions.SharedInformerFactory
	queueUnitLister          listerv1alpha1.QueueUnitLister
	queueUnitInformer        cache.SharedIndexInformer
	queueInformer            cache.SharedIndexInformer
)

func TestOverAdmission(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "WaitForPodsRunning Over-Admission Integration Suite")
}

func getFirstFoundEnvTestBinaryDir() string {
	// Project-local envtest binaries first, then the system kubebuilder assets.
	candidates := []string{
		filepath.Join("..", "..", "..", "jobext", "bin", "k8s"),
		filepath.Join("..", "..", "..", "jobext", "test", "bin", "k8s"),
	}
	for _, basePath := range candidates {
		if entries, err := os.ReadDir(basePath); err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					return filepath.Join(basePath, entry.Name())
				}
			}
		}
	}
	home, _ := os.UserHomeDir()
	sysPath := filepath.Join(home, "Library", "Application Support", "io.kubebuilder.envtest", "k8s")
	if entries, err := os.ReadDir(sysPath); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				return filepath.Join(sysPath, entry.Name())
			}
		}
	}
	return ""
}

func addTestIndexer(qif externalversions.SharedInformerFactory) {
	qif.Scheduling().V1alpha1().Queues().Informer().AddIndexers(cache.Indexers{
		utils.AnnotationQuotaFullName: func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to Queue")
			}
			return []string{qu.Annotations[utils.AnnotationQuotaFullName]}, nil
		},
	})
	qif.Scheduling().V1alpha1().Queues().Informer().AddIndexers(cache.Indexers{
		".metadata.uid": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to Queue")
			}
			return []string{string(qu.UID)}, nil
		},
	})
	qif.Scheduling().V1alpha1().QueueUnits().Informer().AddIndexers(cache.Indexers{
		"queueunits.metadata.uid": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.QueueUnit)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to QueueUnit")
			}
			return []string{string(qu.UID)}, nil
		},
	})
}

var _ = BeforeSuite(func() {
	// TestENV (mixed case) makes the controller skip its 1-minute ForgetQueueUnitInfo poll goroutine
	// (eventhandler.go), which otherwise nil-derefs an unset field during a long teardown.
	os.Setenv("TestENV", "true")

	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.Background())

	By("bootstrapping test environment with envtest")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "jobext", "test", "config", "crd"),
			filepath.Join("..", "elasticquotav1alpha1preemption", "crd"),
		},
		ErrorIfCRDPathMissing:   true,
		ControlPlaneStopTimeout: 60 * time.Second,
	}
	testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	if dir := getFirstFoundEnvTestBinaryDir(); dir != "" {
		os.Setenv("KUBEBUILDER_ASSETS", dir)
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	By("initializing kube-queue scheduler stack")
	kubeClient, err := kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())

	_, err = kubeClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "koord-queue"},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	cli, err = versioned.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	eqcli, err = eqversioned.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	queueUnitInformerFactory = externalversions.NewSharedInformerFactory(cli, 0)
	addTestIndexer(queueUnitInformerFactory)

	queueUnitLister = queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Lister()
	queueUnitInformer = queueUnitInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
	queueInformer = queueUnitInformerFactory.Scheduling().V1alpha1().Queues().Informer()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	schemeModified := scheme.Scheme
	v1alpha1.AddToScheme(schemeModified)
	recorder := eventBroadcaster.NewRecorder(schemeModified, corev1.EventSource{Component: utils.ControllerAgentName})

	By("building the framework with the real elasticquotav1alpha1 (ElasticQuotaV2) plugin")
	registry := runtime.Registry{
		priority.Name:  priority.New,
		"ElasticQuota": elasticquotav1alpha1.New, // key "ElasticQuota"; plugin Name() == "ElasticQuotaV2"
	}
	// NewFramework only instantiates plugins listed in pluginconfig.Plugins, so enable
	// every registry entry explicitly (a nil pluginconfig enables nothing and leaves the
	// QueueUnit mapping func nil).
	pluginConfig := &config.KoordQueueConfiguration{
		Plugins: []config.Plugin{{Name: priority.Name}, {Name: "ElasticQuota"}},
	}
	fw, err = runtime.NewFramework(registry, cfg, "", kubeInformerFactory, queueUnitInformerFactory, recorder, cli, 1, pluginConfig)
	Expect(err).NotTo(HaveOccurred())

	var multiQueue queue.MultiSchedulingQueue
	multiQueue, err = multischedulingqueue.NewMultiSchedulingQueue(fw, 1, 10, queueUnitLister, false, nil)
	Expect(err).NotTo(HaveOccurred())

	sched, err := scheduler.NewScheduler(multiQueue, fw, cli, recorder, false, false, false, 10, "")
	Expect(err).NotTo(HaveOccurred())

	quCtrl := controllers.NewQueueUnitController(2, false, cli, queueUnitInformer, queueUnitLister)

	controller = &ctrl.Controller{}
	controller.SetScheduler(sched)
	controller.SetFramework(fw)
	controller.SetMultiSchedulingQueue(multiQueue)
	controller.SetQuController(quCtrl)
	controller.AddAllEventHandlers(queueUnitInformer, queueInformer)

	kubeInformerFactory.Start(ctx.Done())
	queueUnitInformerFactory.Start(ctx.Done())
	kubeInformerFactory.WaitForCacheSync(ctx.Done())
	queueUnitInformerFactory.WaitForCacheSync(ctx.Done())

	go controller.Start(ctx)

	By("starting a controller-runtime manager running the jobext ResourceReporter")
	// The reporter counts scheduled-but-not-running pods (NodeName set) as Running while a
	// QueueUnit is Dequeued, which releases the queue's assumed slot early. args "" disables
	// partialRunningTimeout so replicas are never shrunk during the test.
	crMgr, err := crctrl.NewManager(cfg, crctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"}, // avoid metrics port conflicts with other suites
	})
	Expect(err).NotTo(HaveOccurred())
	crClient = crMgr.GetClient()

	// PodsByOwnersCacheFields index is needed by Job handle's GetRelatedPods.
	Expect(crMgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, jobextutil.PodsByOwnersCacheFields, func(o client.Object) []string {
		p, ok := o.(*corev1.Pod)
		if !ok {
			return nil
		}
		res := []string{}
		for _, owner := range p.OwnerReferences {
			res = append(res, fmt.Sprintf("%v/%v", owner.Kind, owner.Name))
		}
		return res
	})).ToNot(HaveOccurred())

	jobCtrl := handles.NewJobReconciler(crMgr.GetClient(), cfg, crMgr.GetScheme(), true, "")
	rr := jobextframework.NewResourceReporter(crMgr.GetClient(), crMgr.GetScheme(), jobCtrl)
	Expect(rr.SetupWithManager(crMgr, 1, 100)).ToNot(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		Expect(crMgr.Start(ctx)).ToNot(HaveOccurred(), "failed to run the controller-runtime manager")
	}()

	time.Sleep(time.Second)
	_ = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		return queueUnitInformer.HasSynced() && queueInformer.HasSynced(), nil
	})

	klog.Info("All components started successfully")
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	// Let the scheduler/controller/reporter goroutines observe cancellation and stop hitting the
	// apiserver before we shut it down, so envtest's Stop() doesn't race in-flight requests.
	time.Sleep(time.Second)
	if testEnv != nil {
		// A teardown timeout is environmental (a slow apiserver shutdown) and must not fail the
		// suite: the spec result stands on its own. Surface it as a log line instead.
		if err := testEnv.Stop(); err != nil {
			GinkgoWriter.Printf("warning: failed to stop test environment cleanly: %v\n", err)
		}
	}
})
