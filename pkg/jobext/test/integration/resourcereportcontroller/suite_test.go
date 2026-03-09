package resourcereportcontroller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kube-queue/kube-queue/pkg/jobext/framework"
	"github.com/kube-queue/kube-queue/pkg/jobext/handles"
	ctrl "sigs.k8s.io/controller-runtime"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.
var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client // You'll be using this client in your tests.
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

// getFirstFoundEnvTestBinaryDir locates the first binary in the specified path.
// ENVTEST-based tests depend on specific binaries, usually located in paths set by
// controller-runtime. When running tests directly (e.g., via an IDE) without using
// Makefile targets, the 'BinaryAssetsDirectory' must be explicitly configured.
//
// This function streamlines the process by finding the required binaries, similar to
// setting the 'KUBEBUILDER_ASSETS' environment variable. To ensure the binaries are
// properly set up, run 'make setup-envtest' beforehand.
func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}

var _ = BeforeSuite(func() {
	os.Setenv("TESTENV", "true")
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	var err error
	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}

	// Retrieve the first found binary directory to allow running tests from IDEs
	testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	os.Setenv("KUBEBUILDER_ASSETS", getFirstFoundEnvTestBinaryDir())
	By(fmt.Sprintf("start testEnv: %v", getFirstFoundEnvTestBinaryDir()))
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())
	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Cache:  cache.Options{},
	})
	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).NotTo(BeNil())
	Expect(err).ToNot(HaveOccurred())
	v1alpha1.AddToScheme(k8sManager.GetScheme())
	koordinatorschedulerv1alpha1.AddToScheme(k8sManager.GetScheme())

	rayClusterCtrl := handles.NewRayClusterReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), false, "")
	rayJobCtrl := handles.NewRayJobReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), false, "v1", "")
	rr := framework.NewResourceReporter(k8sManager.GetClient(), k8sManager.GetScheme(), rayClusterCtrl, rayJobCtrl)
	Expect(rr.SetupWithManager(k8sManager, 1, 100)).ToNot(HaveOccurred())
	framework.EnableReservation = true

	go func() {
		defer GinkgoRecover()
		err = k8sManager.Start(ctx)
		Expect(err).ToNot(HaveOccurred(), "failed to run manager")
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
