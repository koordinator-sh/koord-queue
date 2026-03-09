package controller

import (
	"testing"

	configapi "github.com/koordinator-sh/koord-queue/pkg/apis/config"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/config"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	kueuefake "sigs.k8s.io/kueue/client-go/clientset/versioned/fake"
	kueue "sigs.k8s.io/kueue/client-go/informers/externalversions"
)

func TestWithKubeConfig(t *testing.T) {
	kubeConfig := &rest.Config{
		Host: "https://test-server",
	}

	controllerConfig := &config.ControllerConfig{}
	option := WithKubeConfig(kubeConfig)
	option(controllerConfig)

	assert.Equal(t, kubeConfig, controllerConfig.KubeConfig)
}

func TestWithKubeClient(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()

	controllerConfig := &config.ControllerConfig{}
	option := WithKubeClient(kubeClient)
	option(controllerConfig)

	assert.Equal(t, kubeClient, controllerConfig.KubeClient)
}

func TestWithInformersFactory(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	informersFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	controllerConfig := &config.ControllerConfig{}
	option := WithInformersFactory(informersFactory)
	option(controllerConfig)

	assert.Equal(t, informersFactory, controllerConfig.InformersFactory)
}

func TestWithQueueFactory(t *testing.T) {
	queueClient := versionedfake.NewSimpleClientset()
	queueFactory := externalversions.NewSharedInformerFactory(queueClient, 0)

	controllerConfig := &config.ControllerConfig{}
	option := WithQueueFactory(queueFactory)
	option(controllerConfig)

	assert.Equal(t, queueFactory, controllerConfig.QueueFactory)
}

func TestWithQueueUnitClient(t *testing.T) {
	queueClient := versionedfake.NewSimpleClientset()

	controllerConfig := &config.ControllerConfig{}
	option := WithQueueUnitClient(queueClient)
	option(controllerConfig)

	assert.Equal(t, queueClient, controllerConfig.QueueUnitClient)
}

func TestWithKoordQueueConfig(t *testing.T) {
	kubeQueueConfig := &configapi.KoordQueueConfiguration{
		TypeMeta: metav1.TypeMeta{
			Kind: "TestKind",
		},
	}

	controllerConfig := &config.ControllerConfig{}
	option := WithKoordQueueConfig(kubeQueueConfig)
	option(controllerConfig)

	assert.Equal(t, kubeQueueConfig, controllerConfig.Config)
}

func TestWithKueueInformerFactory(t *testing.T) {
	kueueClient := kueuefake.NewSimpleClientset()
	kueueInformerFactory := kueue.NewSharedInformerFactory(kueueClient, 0)

	controllerConfig := &config.ControllerConfig{}
	option := WithKueueInformerFactory(kueueInformerFactory)
	option(controllerConfig)

	assert.Equal(t, kueueInformerFactory, controllerConfig.KueueInformerFactory)
}
