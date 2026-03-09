package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func SetDefaults_ElasticQuotaTreeArgs(obj *ElasticQuotaArgs) {
	if obj.CheckQuotaOversold == nil {
		obj.CheckQuotaOversold = ptr.To(false)
	}
}

func SetDefaults_KoordQueueConfiguration(obj *KoordQueueConfiguration) {
	scheme := runtime.NewScheme()
	localSchemeBuilder.AddToScheme(scheme)
	if len(obj.Plugins) == 0 {
		obj.Plugins = []Plugin{
			{Name: "Priority"},
			{Name: "ElasticQuotaV2"},
		}
	}
	if len(obj.PluginConfigs) == 0 {
		obj.PluginConfigs = map[string]runtime.RawExtension{}
	}
	for _, plugin := range obj.Plugins {
		pluginName := plugin.Name
		if _, ok := obj.PluginConfigs[pluginName]; !ok {
			gvk := SchemeGroupVersion.WithKind(pluginName + "Args")
			args, err := scheme.New(gvk)
			if err != nil {
				continue
			}
			scheme.Default(args)
			obj.PluginConfigs[pluginName] = runtime.RawExtension{Object: args}
		}
	}
}
