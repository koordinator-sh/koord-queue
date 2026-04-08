/*
 Copyright 2021 The Koord-Queue Authors.

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

package plugins

import (
	"os"
	"strings"

	apiruntime "k8s.io/apimachinery/pkg/runtime"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/defaultgroup"
	elasticquota "github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/priority"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/resourcequota"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
)

// NewInTreeRegistry builds the registry with all the in-tree plugins.
// A scheduler that runs out of tree plugins can register additional plugins
// through the WithFrameworkOutOfTreeRegistry option.
func NewInTreeRegistry() runtime.Registry {
	return runtime.Registry{
		priority.Name:             priority.New,
		defaultgroup.Name:         defaultgroup.New,
		resourcequota.Name:        resourcequota.New,
		elasticquota.Name:         elasticquota.New,
		elasticquotav1alpha1.Name: elasticquotav1alpha1.New,
	}
}

func pluginproxy(f func(_ apiruntime.Object, handle framework.Handle) (framework.Plugin, error), plugins map[string]framework.Plugin) func(_ apiruntime.Object, handle framework.Handle) (framework.Plugin, error) {
	return func(obj apiruntime.Object, handle framework.Handle) (framework.Plugin, error) {
		plg, err := f(obj, handle)
		plugins[plg.Name()] = plg
		return plg, err
	}
}

func NewFakeRegistry() (runtime.Registry, map[string]framework.Plugin) {
	plugins := map[string]framework.Plugin{}
	queueGroupPlugin := "resourceQuota"
	if os.Getenv("QueueGroupPlugin") != "" {
		queueGroupPlugin = strings.TrimSpace(os.Getenv("QueueGroupPlugin"))
	}
	switch queueGroupPlugin {
	case "resourceQuota":
		return runtime.Registry{
			priority.Name:      pluginproxy(priority.New, plugins),
			defaultgroup.Name:  pluginproxy(defaultgroup.New, plugins),
			resourcequota.Name: pluginproxy(resourcequota.New, plugins),
		}, plugins
	case "elasticquota":
		return runtime.Registry{
			priority.Name:     pluginproxy(priority.New, plugins),
			elasticquota.Name: pluginproxy(elasticquota.FakeNew, plugins),
		}, plugins
	case "elasticquotav2":
		return runtime.Registry{
			priority.Name:     pluginproxy(priority.New, plugins),
			elasticquota.Name: pluginproxy(elasticquotav1alpha1.FakeNew, plugins),
		}, plugins
	default:
		panic("QueueGroupPlugin must be in [resourceQuota|elasticquota]")
	}
}
