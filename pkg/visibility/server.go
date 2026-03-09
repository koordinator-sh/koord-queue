package visibility

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/koordinator-sh/koord-queue/pkg/controller"
	"github.com/koordinator-sh/koord-queue/pkg/visibility/apis"
	generatedopenapi "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/openapi"
	visibilityv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"

	utilversion "k8s.io/apimachinery/pkg/util/version"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/pkg/version"
	"k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/klog/v2"
)

var (
	logger = klog.NewStandardLogger("INFO")
)

// CreateAndStartVisibilityServer creates visibility server injecting KueueManager and starts it
func CreateAndStartVisibilityServer(ctx context.Context, ctrl *controller.Controller, conf *rest.Config, configPath string) {
	config := newVisibilityServerConfig(conf)
	if err := applyVisibilityServerOptions(config, configPath); err != nil {
		logger.Fatalf("Unable to apply VisibilityServerOptions: %v", err)
		os.Exit(1)
	}

	visibilityServer, err := config.Complete().New("visibility-server", genericapiserver.NewEmptyDelegate())
	if err != nil {
		logger.Fatalf("Unable to create visibility server: %v", err)
		os.Exit(1)
	}

	if err := apis.Install(visibilityServer, ctrl); err != nil {
		logger.Fatalf("Unable to install visibility.koord-queue.x-k8s.io API: %v", err)
		os.Exit(1)
	}

	if err := visibilityServer.PrepareRun().RunWithContext(ctx); err != nil {
		logger.Fatalf("Error running visibility server: %v", err)
		os.Exit(1)
	}
}

func applyVisibilityServerOptions(config *genericapiserver.RecommendedConfig, confPath string) error {
	o := genericoptions.NewRecommendedOptions("", apis.Codecs.LegacyCodec(
		visibilityv1alpha1.SchemeGroupVersion,
	))
	if confPath != "" {
		o.Authentication.RemoteKubeConfigFile = confPath
		o.Authorization.RemoteKubeConfigFile = confPath
		o.CoreAPI.CoreAPIKubeconfigPath = confPath
	}
	o.Etcd = nil
	o.SecureServing.BindPort = 8082
	// The directory where TLS certs will be created
	o.SecureServing.ServerCert.CertDirectory = "/tmp"

	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return fmt.Errorf("error creating self-signed certificates: %v", err)
	}
	return o.ApplyTo(config)
}

func newVisibilityServerConfig(conf *rest.Config) *genericapiserver.RecommendedConfig {
	c := genericapiserver.NewRecommendedConfig(apis.Codecs)
	versionInfo := version.Get()
	binaryVersion := utilversion.MustParse(versionInfo.String())
	version := strings.Split(versionInfo.String(), "-")[0]
	// enable OpenAPI schemas
	c.Config.EffectiveVersion = basecompatibility.NewEffectiveVersion(binaryVersion, false, binaryVersion, binaryVersion.SubtractMinor(2))
	c.Config.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(generatedopenapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(apis.Scheme))
	c.Config.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(generatedopenapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(apis.Scheme))
	c.Config.OpenAPIConfig.Info.Title = "Koord-queue visibility-server"
	c.Config.OpenAPIV3Config.Info.Title = "Koord-queue visibility-server"
	c.Config.OpenAPIConfig.Info.Version = version
	c.Config.OpenAPIV3Config.Info.Version = version

	c.EnableMetrics = true

	return c
}
