/*
Copyright 2022 The Koordinator Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KoordQueueConfiguration struct {
	metav1.TypeMeta `json:",inline"`

	PluginConfigs map[string]runtime.Object `json:"pluginConfigs,omitempty"`

	Plugins []Plugin `json:"plugins,omitempty"`
}

type Plugin struct {
	Name string `json:"name,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ElasticQuotaArgs struct {
	metav1.TypeMeta `json:",inline"`
	// default is false
	// this can not be used with NonPreemptible job
	// the reason is:
	// when NonPreemptible job is enabled, total used resources of nonpreemptible job will not
	// be over than Min
	CheckQuotaOversold bool `json:"checkQuotaOversold,omitempty"`
}

const GroupName = "scheduling.k8s.io"
