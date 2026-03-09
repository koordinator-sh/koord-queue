package apis

import (
	"github.com/koordinator-sh/koord-queue/pkg/controller"
	"github.com/koordinator-sh/koord-queue/pkg/visibility/apis/restapi"
	visibilityv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/visibility/apis/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

var (
	Scheme         = runtime.NewScheme()
	Codecs         = serializer.NewCodecFactory(Scheme)
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	utilruntime.Must(visibilityv1alpha1.AddToScheme(Scheme))
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
}

// Install installs API scheme and registers storages
func Install(server *genericapiserver.GenericAPIServer, ctrl *controller.Controller) error {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(visibilityv1alpha1.GroupVersion.Group, Scheme, ParameterCodec, Codecs)
	// TODO: add resource storage map
	apiGroupInfo.VersionedResourcesStorageMap[visibilityv1alpha1.GroupVersion.Version] = restapi.NewStorage(ctrl)
	return server.InstallAPIGroups(&apiGroupInfo)
}
