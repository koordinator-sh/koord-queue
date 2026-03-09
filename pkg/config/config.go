package config

import (
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	externalversions "github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/kube-queue/pkg/apis/config"
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

	Config *config.KubeQueueConfiguration
}
