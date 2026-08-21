package resourcereportcontroller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	ctrl "sigs.k8s.io/controller-runtime"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
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
	// Check KUBEBUILDER_ASSETS env var first
	if assets := os.Getenv("KUBEBUILDER_ASSETS"); assets != "" {
		return assets
	}
	// Walk two levels of subdirectories under <project-root>/bin/k8s to find the
	// directory containing etcd/kube-apiserver/kubectl binaries.
	// The layout created by setup-envtest is: bin/k8s/k8s/<version>-<os>-<arch>/
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

	// The reporter resolves a queue unit's pods through these indexes, the same way the
	// production controller registers them in cmd/controllers. Without them every List by
	// field selector fails and the reporter never reconciles anything.
	Expect(k8sManager.GetCache().IndexField(ctx, &corev1.Pod{}, util.PodsByOwnersCacheFields, func(o client.Object) []string {
		p, ok := o.(*corev1.Pod)
		if !ok {
			return nil
		}
		res := []string{}
		for _, owner := range p.OwnerReferences {
			res = append(res, fmt.Sprintf("%v/%v", owner.Kind, owner.Name))
		}
		return res
	})).To(Succeed())
	Expect(k8sManager.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, util.RelatedQueueUnitCacheFields, func(o client.Object) []string {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return []string{}
		}
		return []string{pod.Annotations[util.RelatedQueueUnitAnnoKey]}
	})).To(Succeed())

	rayClusterCtrl := handles.NewRayClusterReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), false, "")
	rayJobCtrl := handles.NewRayJobReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), false, "v1", "")
	// Several specs drive a plain batch/v1 Job; without its handle the reporter cannot resolve
	// the job type and silently skips every reconcile for them.
	jobCtrl := handles.NewJobReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), true, "partialRunningTimeout: 5s")
	rr := framework.NewResourceReporter(k8sManager.GetClient(), k8sManager.GetScheme(), rayClusterCtrl, rayJobCtrl, jobCtrl)
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
