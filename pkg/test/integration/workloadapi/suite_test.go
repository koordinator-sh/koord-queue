package workloadapi_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/koordinator-sh/koord-queue/cmd/app/options"
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

	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// This suite exercises the QueueUnit API additions that align kube-queue with the upstream
// Kueue Workload API (conditions, spec.active, requeueState, partial
// admission) against a real apiserver, so CRD schema gaps are caught as well as logic bugs.
var (
	ctx    context.Context
	cancel context.CancelFunc

	testEnv *envtest.Environment
	cfg     *rest.Config

	fw    framework.Framework
	cli   versioned.Interface
	eqcli eqversioned.Interface

	controller *ctrl.Controller

	queueUnitInformerFactory externalversions.SharedInformerFactory
	queueUnitLister          listerv1alpha1.QueueUnitLister
	queueUnitInformer        cache.SharedIndexInformer
	queueInformer            cache.SharedIndexInformer
)

func TestWorkloadAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "QueueUnit Workload API Integration Suite")
}

func getFirstFoundEnvTestBinaryDir() string {
	// Project-local envtest binaries (populated by hack/integration-test.sh) first.
	basePath := filepath.Join("..", "..", "..", "jobext", "test", "bin", "k8s")
	if entries, err := os.ReadDir(basePath); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				return filepath.Join(basePath, entry.Name())
			}
		}
	}
	// Fallback to the system kubebuilder envtest assets.
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
	// TestENV (mixed case) makes the controller skip its 1-minute ForgetQueueUnitInfo poll
	// goroutine (eventhandler.go), which otherwise nil-derefs during a long teardown.
	os.Setenv("TestENV", "true")
	options.SetDefaultPreemptibleForTest(true)

	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.Background())

	By("bootstrapping test environment with envtest")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "jobext", "test", "config", "crd"),
			filepath.Join(".", "crd"),
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

	By("building the framework with the real elasticquotav1alpha1 plugin")
	registry := runtime.Registry{
		priority.Name:  priority.New,
		"ElasticQuota": elasticquotav1alpha1.New,
	}
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

	time.Sleep(time.Second)
	_ = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		return queueUnitInformer.HasSynced() && queueInformer.HasSynced(), nil
	})

	klog.Info("All components started successfully")
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	time.Sleep(time.Second)
	if testEnv != nil {
		if err := testEnv.Stop(); err != nil {
			GinkgoWriter.Printf("warning: failed to stop test environment cleanly: %v\n", err)
		}
	}
})
