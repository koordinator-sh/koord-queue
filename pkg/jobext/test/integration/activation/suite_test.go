package activation

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// This suite drives the job-extension side of spec.active and
// spec.maximumExecutionTimeSeconds: the reconciler must suspend the job and hand its resources
// back, and it must do so through the same path preemption already uses.
var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestActivation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "QueueUnit Activation Suite")
}

func getFirstFoundEnvTestBinaryDir() string {
	if assets := os.Getenv("KUBEBUILDER_ASSETS"); assets != "" {
		return assets
	}
	// Layout created by setup-envtest: bin/k8s/k8s/<version>-<os>-<arch>/
	basePath := filepath.Join("..", "..", "..", "..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subPath := filepath.Join(basePath, entry.Name())
		subEntries, err := os.ReadDir(subPath)
		if err != nil {
			continue
		}
		for _, subEntry := range subEntries {
			if subEntry.IsDir() {
				return filepath.Join(subPath, subEntry.Name())
			}
		}
	}
	// Fallback to the system kubebuilder envtest assets.
	home, _ := os.UserHomeDir()
	sysPath := filepath.Join(home, "Library", "Application Support", "io.kubebuilder.envtest", "k8s")
	if sysEntries, err := os.ReadDir(sysPath); err == nil {
		for _, entry := range sysEntries {
			if entry.IsDir() {
				return filepath.Join(sysPath, entry.Name())
			}
		}
	}
	return ""
}

var _ = BeforeSuite(func() {
	os.Setenv("TESTENV", "true")
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("enabling the activation feature gates")
	Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
		string(features.QueueUnitActive):      true,
		string(features.MaximumExecutionTime): true,
	})).To(Succeed())

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	os.Setenv("KUBEBUILDER_ASSETS", getFirstFoundEnvTestBinaryDir())

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Cache:  cache.Options{},
	})
	Expect(err).ToNot(HaveOccurred())
	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).NotTo(BeNil())

	Expect(v1alpha1.AddToScheme(k8sManager.GetScheme())).To(Succeed())
	Expect(koordinatorschedulerv1alpha1.AddToScheme(k8sManager.GetScheme())).To(Succeed())

	// managedAllJobs=true so every batch/v1 Job in the test namespace is queued.
	jobHandle := handles.NewJobReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), true, "")
	jobReconciler := framework.NewJobReconcilerWithJobExtension(k8sManager.GetClient(), k8sManager.GetScheme(), jobHandle)
	Expect(jobReconciler.SetupWithManager(k8sManager, 1, 100)).ToNot(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		Expect(k8sManager.Start(ctx)).ToNot(HaveOccurred(), "failed to run manager")
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
		string(features.QueueUnitActive):      false,
		string(features.MaximumExecutionTime): false,
	})).To(Succeed())
	if testEnv != nil {
		if err := testEnv.Stop(); err != nil {
			GinkgoWriter.Printf("warning: failed to stop test environment cleanly: %v\n", err)
		}
	}
})
