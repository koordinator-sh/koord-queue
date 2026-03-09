package fake

import (
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	"github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
)

type FakeHandle struct {
	quToQuotas map[string]string
	cli        *fake.Clientset
}

var _ framework.Handle = &FakeHandle{}

func NewFakeHandle(quToQuotas map[string]string) *FakeHandle {
	return &FakeHandle{quToQuotas: quToQuotas}
}

// 实现 QueueStatusUpdateHandle 接口
func (f *FakeHandle) UpdateQueueStatus(name string, details map[string][]v1alpha1.QueueItemDetail) error {
	return nil
}

// 实现 Handle 接口方法（返回 nil 或默认值）

func (f *FakeHandle) QueueInformerFactory() externalversions.SharedInformerFactory {
	f.cli = fake.NewSimpleClientset()
	return externalversions.NewSharedInformerFactory(f.cli, 0)
}

func (f *FakeHandle) SharedInformerFactory() informers.SharedInformerFactory {
	return nil
}

func (f *FakeHandle) KubeConfigPath() string {
	return "/path/to/kubeconfig"
}

func (f *FakeHandle) QueueUnitClient() versioned.Interface {
	return &versioned.Clientset{}
}

func (f *FakeHandle) OversellRate() float64 {
	return 1.0
}

func (f *FakeHandle) KubeConfig() *rest.Config {
	return &rest.Config{}
}

func (f *FakeHandle) EventRecorder() record.EventRecorderLogger {
	return nil
}

func (f *FakeHandle) GetQueueUnitQuotaName(qu *v1alpha1.QueueUnit) ([]string, error) {
	// 默认返回固定 quota 名称
	return []string{f.quToQuotas[qu.Namespace+"/"+qu.Name]}, nil
}
