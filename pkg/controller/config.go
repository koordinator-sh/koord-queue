package controller

import (
	configapi "github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"github.com/koordinator-sh/koord-queue/pkg/config"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kueue "sigs.k8s.io/kueue/client-go/informers/externalversions"
)

type ControllerConfigOpt func(*config.ControllerConfig)

func WithKubeConfig(kubeconfig *rest.Config) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.KubeConfig = kubeconfig
	}
}

func WithKubeClient(kubeclient kubernetes.Interface) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.KubeClient = kubeclient
	}
}

func WithInformersFactory(informersFactory informers.SharedInformerFactory) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.InformersFactory = informersFactory
	}
}

func WithQueueFactory(queueFactory externalversions.SharedInformerFactory) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.QueueFactory = queueFactory
	}
}

func WithQueueUnitClient(queueUnitClient versioned.Interface) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.QueueUnitClient = queueUnitClient
	}
}

func WithKoordQueueConfig(cfg *configapi.KoordQueueConfiguration) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.Config = cfg
	}
}

func WithKueueInformerFactory(kueueInformer kueue.SharedInformerFactory) ControllerConfigOpt {
	return func(c *config.ControllerConfig) {
		c.KueueInformerFactory = kueueInformer
	}
}
