package testutils

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins"
	elasticquotaplugin "github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquota"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/priority"
	"github.com/koordinator-sh/koord-queue/pkg/framework/runtime"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func NewFrameworkForTesting() (framework.Framework, map[string]framework.Plugin, versioned.Interface) {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset()
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	_ = versionedInformers.Scheduling().V1alpha1().Queues().Informer().AddIndexers(cache.Indexers{
		utils.AnnotationQuotaFullName: func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to Queue")
			}
			return []string{qu.Annotations[utils.AnnotationQuotaFullName]}, nil
		},
	})
	registry, plugins := plugins.NewFakeRegistry()
	pluginConfig := &config.KoordQueueConfiguration{
		Plugins: []config.Plugin{
			{Name: priority.Name},
			{Name: elasticquotaplugin.Name},
		},
	}
	fwk, err := runtime.NewFramework(
		registry,
		nil,
		"",
		informers,
		versionedInformers,
		record.NewFakeRecorder(100),
		versionedclient,
		1, pluginConfig)

	if err != nil {
		panic(err)
	}
	return fwk, plugins, versionedclient
}

func NewFakeFrameworkHandle() framework.Handle {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset()
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	return &FakeHandle{in: informers, ver: versionedInformers, fakeClient: versionedclient, fakeRecorder: record.NewFakeRecorder(100)}
}

func NewFakeFrameworkHandleWithQueueUnit(objects ...kuberuntime.Object) framework.Handle {
	fakeClient := fake.NewSimpleClientset()
	informers := kubeinformers.NewSharedInformerFactory(fakeClient, time.Second*30)
	versionedclient := versionedfake.NewSimpleClientset(objects...)
	versionedInformers := externalversions.NewSharedInformerFactory(versionedclient, 0)
	versionedInformers.Scheduling().V1alpha1().QueueUnits().Informer().GetIndexer()
	return &FakeHandle{in: informers, ver: versionedInformers, fakeClient: versionedclient, fakeRecorder: record.NewFakeRecorder(100)}
}

type FakeHandle struct {
	ver          externalversions.SharedInformerFactory
	in           kubeinformers.SharedInformerFactory
	fakeClient   *versionedfake.Clientset
	fakeRecorder record.EventRecorderLogger
}

func (f *FakeHandle) EventRecorder() record.EventRecorderLogger {
	return f.fakeRecorder
}

func (f *FakeHandle) GetQueueUnitQuotaName(*v1alpha1.QueueUnit) ([]string, error) {
	return []string{}, nil
}

func (f *FakeHandle) QueueInformerFactory() externalversions.SharedInformerFactory { return f.ver }
func (f *FakeHandle) SharedInformerFactory() kubeinformers.SharedInformerFactory       { return f.in }
func (f *FakeHandle) KubeConfigPath() string                                       { return "" }
func (f *FakeHandle) QueueUnitClient() versioned.Interface                         { return f.fakeClient }
func (f *FakeHandle) OversellRate() float64                                        { return 1 }
func (f *FakeHandle) KubeConfig() *rest.Config                                     { return nil }
func (f *FakeHandle) GetReclaimProtectTime() time.Duration                         { return 0 }
func (f *FakeHandle) UpdateQueueStatus(name string, details map[string][]v1alpha1.QueueItemDetail) error {
	queue, err := f.ver.Scheduling().V1alpha1().Queues().Lister().Queues("koord-queue").Get(name)
	if err != nil {
		return err
	}
	newQueue := queue.DeepCopy()
	newQueue.Status.QueueItemDetails = make(map[string][]v1alpha1.QueueItemDetail, len(details))
	for k, v := range details {
		newQueue.Status.QueueItemDetails[k] = utils.SliceCopy(v)
	}
	if reflect.DeepEqual(newQueue.Status, queue.Status) {
		return nil
	}
	return framework.RetryTooManyRequests(func() error {
		_, err := f.fakeClient.SchedulingV1alpha1().Queues("koord-queue").UpdateStatus(context.Background(), newQueue, metav1.UpdateOptions{})
		return err
	})
}
