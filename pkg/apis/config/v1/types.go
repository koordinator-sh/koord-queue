package v1

import (
	"bytes"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KubeQueueConfiguration struct {
	metav1.TypeMeta `json:",inline"`

	PluginConfigs map[string]runtime.RawExtension `json:"pluginConfigs,omitempty"`

	Plugins []Plugin `json:"plugins,omitempty"`
}

type Plugin struct {
	Name string `json:"name,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ElasticQuotaArgs struct {
	metav1.TypeMeta `json:",inline"`

	CheckQuotaOversold *bool `json:"checkQuotaOversold,omitempty"`
}

// DecodeNestedObjects decodes plugin args for known types.
func (c *KubeQueueConfiguration) DecodeNestedObjects(d runtime.Decoder) error {
	var strictDecodingErrs []error
	for name, data := range c.PluginConfigs {
		gvk := SchemeGroupVersion.WithKind(name + "Args")
		// dry-run to detect and skip out-of-tree plugin args.
		if _, _, err := d.Decode(nil, &gvk, nil); runtime.IsNotRegisteredError(err) {
			return nil
		}
		obj, parsedGvk, err := d.Decode(data.Raw, &gvk, nil)
		if err != nil {
			decodingArgsErr := fmt.Errorf("decoding args for plugin %s: %w", name, err)
			if obj != nil && runtime.IsStrictDecodingError(err) {
				strictDecodingErrs = append(strictDecodingErrs, runtime.NewStrictDecodingError([]error{decodingArgsErr}))
			} else {
				return decodingArgsErr
			}
		}
		if parsedGvk.GroupKind() != gvk.GroupKind() {
			return fmt.Errorf("args for plugin %s were not of type %s, got %s", name, gvk.GroupKind(), parsedGvk.GroupKind())
		}
		data.Object = obj
		c.PluginConfigs[name] = data
	}
	if len(strictDecodingErrs) > 0 {
		return runtime.NewStrictDecodingError(strictDecodingErrs)
	}
	return nil
}

// EncodeNestedObjects encodes plugin args.
func (c *KubeQueueConfiguration) EncodeNestedObjects(e runtime.Encoder) error {
	for _, data := range c.PluginConfigs {
		if data.Object == nil {
			continue
		}
		var buf bytes.Buffer
		err := e.Encode(data.Object, &buf)
		if err != nil {
			return err
		}
		// The <e> encoder might be a YAML encoder, but the parent encoder expects
		// JSON output, so we convert YAML back to JSON.
		// This is a no-op if <e> produces JSON.
		json, err := yaml.YAMLToJSON(buf.Bytes())
		if err != nil {
			return err
		}
		data.Raw = json
		return nil
	}
	return nil
}
