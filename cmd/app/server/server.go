/*
 Copyright 2021 The Kube-Queue Authors.

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

package app

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	externalversions "github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/kube-queue/cmd/app/options"
	"github.com/kube-queue/kube-queue/pkg/apis/config"
	"github.com/kube-queue/kube-queue/pkg/apis/config/scheme"
	v1 "github.com/kube-queue/kube-queue/pkg/apis/config/v1"
	"github.com/kube-queue/kube-queue/pkg/controller"
	"github.com/kube-queue/kube-queue/pkg/utils"
	"github.com/kube-queue/kube-queue/pkg/visibility"
	"gomodules.xyz/jsonpatch/v2"

	kueueversioned "sigs.k8s.io/kueue/client-go/clientset/versioned"
	kueue "sigs.k8s.io/kueue/client-go/informers/externalversions"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	apiVersion = "v1alpha1"
)

// LoadConfigFromFile loads scheduler config from the specified file path
func LoadConfigFromFile(logger klog.Logger, file string) (*config.KubeQueueConfiguration, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return loadConfig(data)
}

func loadConfig(data []byte) (*config.KubeQueueConfiguration, error) {
	// The UniversalDecoder runs defaulting and returns the internal type by default.
	obj, gvk, err := scheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	if cfgObj, ok := obj.(*config.KubeQueueConfiguration); ok {
		// We don't set this field in pkg/scheduler/apis/config/{version}/conversion.go
		// because the field will be cleared later by API machinery during
		// conversion. See KubeQueueConfiguration internal type definition for
		// more details.
		cfgObj.TypeMeta.APIVersion = gvk.GroupVersion().String()
		return cfgObj, nil
	}
	return nil, fmt.Errorf("couldn't decode as KubeSchedulerConfiguration, got %s: ", gvk)
}

func addIndexer(qif externalversions.SharedInformerFactory) error {
	err := qif.Scheduling().V1alpha1().Queues().Informer().AddIndexers(cache.Indexers{
		utils.AnnotationQuotaFullName: func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to Queue")
			}
			return []string{qu.Annotations[utils.AnnotationQuotaFullName]}, nil
		},
	})
	if err != nil {
		return err
	}
	err = qif.Scheduling().V1alpha1().Queues().Informer().AddIndexers(cache.Indexers{
		".metadata.uid": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.Queue)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to Queue")
			}
			return []string{string(qu.UID)}, nil
		},
	})
	if err != nil {
		return err
	}

	err = qif.Scheduling().V1alpha1().QueueUnits().Informer().AddIndexers(cache.Indexers{
		"queueunits.metadata.uid": func(obj interface{}) ([]string, error) {
			qu, ok := obj.(*v1alpha1.QueueUnit)
			if !ok {
				return []string{}, fmt.Errorf("failed to convert to QueueUnit")
			}
			return []string{string(qu.UID)}, nil
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func Start(ctx context.Context, cfg *rest.Config, kubeClient kubernetes.Interface, opt *options.ServerOption) error {
	var c *config.KubeQueueConfiguration
	var err error
	if opt.Config != "" {
		c, err = LoadConfigFromFile(klog.LoggerWithName(klog.Background(), "Init"), opt.Config)
		if err != nil {
			return err
		}
	} else {
		configv1 := &v1.KubeQueueConfiguration{}
		c = &config.KubeQueueConfiguration{}
		v1.SetDefaults_KubeQueueConfiguration(configv1)
		err = v1.Convert_v1_KubeQueueConfiguration_To_config_KubeQueueConfiguration(configv1, c, nil)
		if err != nil {
			return err
		}
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		return err
	}
	restConfig.QPS = float32(opt.QPS)
	restConfig.Burst = opt.Burst
	queueUnitClient := versioned.NewForConfigOrDie(restConfig)
	kueueClient := kueueversioned.NewForConfigOrDie(restConfig)

	kueueInformerFactory := kueue.NewSharedInformerFactory(kueueClient, 0)
	queueUnitInformerFactory := externalversions.NewSharedInformerFactory(queueUnitClient, 0)

	if err := addIndexer(queueUnitInformerFactory); err != nil {
		return err
	}

	enableStrictConsistency := false
	if os.Getenv("StrictConsistency") != "" {
		enableStrictConsistency = strings.ToLower(strings.TrimSpace(os.Getenv("StrictConsistency"))) == "true"
	}
	controller, err := controller.NewController(
		opt.KubeConfig,
		enableStrictConsistency,
		ctx.Done(),

		controller.WithKubeConfig(restConfig),
		controller.WithKubeClient(kubeClient),
		controller.WithQueueUnitClient(queueUnitClient),
		controller.WithInformersFactory(kubeInformerFactory),
		controller.WithQueueFactory(queueUnitInformerFactory),
		controller.WithKueueInformerFactory(kueueInformerFactory),
		controller.WithKubeQueueConfig(c),
	)

	if err != nil {
		klog.Fatalln("Error building controller")
	}

	kubeInformerFactory.Start(ctx.Done())
	queueUnitInformerFactory.Start(ctx.Done())
	kubeInformerFactory.WaitForCacheSync(ctx.Done())
	queueUnitInformerFactory.WaitForCacheSync(ctx.Done())
	klog.Infof("Informer Start successfully")

	if opt.EnableApiHandler {
		ServeAPIHandlers(ctx, controller)
		klog.Infof("ApiHandler Start successfully")
	}

	if opt.EnableVisibilityServer {
		go visibility.CreateAndStartVisibilityServer(ctx, controller, cfg, opt.KubeConfig)
	}
	controller.Start(ctx)
	klog.Infof("Controller Start successfully")
	return nil
}

func Run(opt *options.ServerOption) error {
	klog.Infof("%+v", apiVersion)

	if len(os.Getenv("KUBECONFIG")) > 0 {
		opt.KubeConfig = os.Getenv("KUBECONFIG")
	}

	if err := utilfeature.DefaultMutableFeatureGate.Set(opt.FeatureGates); err != nil {
		os.Exit(1)
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s\n", err.Error())
	}

	cfg.QPS = float32(opt.QPS)
	cfg.Burst = opt.Burst
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s\n", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if opt.LeaderElection {
		leaseLock := &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      "example-lease",
				Namespace: "default",
			},
			Client: kubeClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: string(uuid.NewUUID()), // 唯一标识这个实例
			},
		}
		leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
			Lock:            leaseLock,
			ReleaseOnCancel: true,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					// 当当前实例成为leader时执行的逻辑
					if opt.EnableVisibilityServer {
						patchStr := fmt.Sprintf("[%v]", ptr.To(jsonpatch.NewOperation("add", "/metadata/labels/kube-queue-leader", "true")).Json())
						_, err := kubeClient.CoreV1().Pods(os.Getenv("POD_NAMESPACE")).Patch(context.Background(),
							os.Getenv("POD_NAME"), types.JSONPatchType,
							[]byte(patchStr),
							metav1.PatchOptions{})
						if err != nil {
							klog.Fatalf("%v: %v", err, patchStr)
						}
					}
					fmt.Println("became leader, start to do job queuing")
					Start(ctx, cfg, kubeClient, opt)
				},
				OnStoppedLeading: func() {
					// 当实例失去leader状态时执行的逻辑
					fmt.Println("stopped leading")
					cancel()
				},
				OnNewLeader: func(identity string) {
					// 当新的leader选举出来时执行的逻辑
					if identity == string(uuid.NewUUID()) {
						// 如果新的leader是当前实例
						return
					}
					fmt.Printf("new leader elected: %s\n", identity)
				},
			},
		})
		return nil
	} else {
		if opt.EnableVisibilityServer {
			patchStr := fmt.Sprintf("[%v]", ptr.To(jsonpatch.NewOperation("add", "/metadata/labels/kube-queue-leader", "true")).Json())
			_, err := kubeClient.CoreV1().Pods(os.Getenv("POD_NAMESPACE")).Patch(context.Background(),
				os.Getenv("POD_NAME"), types.JSONPatchType,
				[]byte(patchStr),
				metav1.PatchOptions{})
			if err != nil {
				klog.Fatalf("%v: %v", err, patchStr)
			}
		}
		return Start(ctx, cfg, kubeClient, opt)
	}
}
