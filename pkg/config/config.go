package config

import (
	"github.com/koordinator-sh/koord-queue/pkg/apis/config"
	"github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned"
	externalversions "github.com/koordinator-sh/koord-queue/pkg/client/informers/externalversions"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kueue "sigs.k8s.io/kueue/client-go/informers/externalversions"
)

type ControllerConfig struct {
	KubeConfig *rest.Config

	KubeClient      kubernetes.Interface
	QueueUnitClient versioned.Interface

	QueueFactory         externalversions.SharedInformerFactory
	InformersFactory     informers.SharedInformerFactory
	KueueInformerFactory kueue.SharedInformerFactory

	Config *config.KoordQueueConfiguration
}
