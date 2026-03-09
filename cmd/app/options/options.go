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

package options

import (
	"flag"
	"sync"
)

// ServerOption is the main context object for the queue controller.
type ServerOption struct {
	KubeConfig string
	// QPS indicates the maximum QPS to the master from this client.
	// If it's zero, the created RESTClient will use DefaultQPS: 5
	QPS int
	// Maximum burst for throttle.
	// If it's zero, the created RESTClient will use DefaultBurst: 10.
	Burst int
	// Pod in the backoffQ init duration
	PodInitialBackoffSeconds int
	// Pod in the backoffQ max duration
	PodMaxBackoffSeconds            int
	OversellRate                    float64
	ScheduleSuspendTimeInMillSecond int64
	FeatureGates                    string
	EnableApiHandler                bool
	DefaultPreemptible              bool
	LeaderElection                  bool
	AdmissionCheckControllerWorker  int64
	EnableParentLimit               bool
	EnableVisibilityServer          bool
	Config                          string
	// When strict dequeue mode is enabled, QueueUnits will be SchedReady after dequeue
	// from the queue and waiting scheduler to change the status to SchedSucceed or SchedFailed.
	StrictDequeueMode bool

	QueueList string
}

// Can only be called once
func NewServerOption() *ServerOption {
	once.Do(func() {
		globalOts = &ServerOption{}
	})
	return globalOts
}

var once sync.Once
var globalOts *ServerOption

func (s *ServerOption) Register(fs *flag.FlagSet) {
	if f := fs.Lookup("kubeconfig"); f != nil {
		s.KubeConfig = f.Value.String()
	}
}

func (s *ServerOption) AddFlags(fs *flag.FlagSet) {
	if f := fs.Lookup("kubeconfig"); f == nil {
		fs.StringVar(&s.KubeConfig, "kubeconfig", "", "the path to the kube config")
	}
	fs.IntVar(&s.QPS, "qps", 50, "QPS indicates the maximum QPS to the master from this client.")
	fs.IntVar(&s.Burst, "burst", 50, "Maximum burst for throttle.")
	fs.IntVar(&s.PodInitialBackoffSeconds, "podInitialBackoffSeconds", 1, "Pod in the backoffQ init duration")
	fs.IntVar(&s.PodMaxBackoffSeconds, "podMaxBackoffSeconds", 20, "Pod in the backoffQ max duration")
	fs.Float64Var(&s.OversellRate, "oversellrate", 1, "the rate for oversell")
	fs.Int64Var(&s.ScheduleSuspendTimeInMillSecond, "scheduleSuspendTimeInMillSecond", 10, "the suspend time for scheduling")
	fs.BoolVar(&s.EnableApiHandler, "enableApiHandler", false, "enable api handler")
	fs.BoolVar(&s.DefaultPreemptible, "defaultPreemptible", true, "")
	fs.BoolVar(&s.LeaderElection, "leaderElection", true, "")
	fs.BoolVar(&s.EnableParentLimit, "enableParentLimit", false, "")
	fs.BoolVar(&s.EnableVisibilityServer, "enableVisibilityServer", true, "")
	flag.StringVar(&s.FeatureGates, "feature-gates", "", "A set of key=value pairs that describe feature gates for alpha/experimental features.")
	fs.Int64Var(&s.AdmissionCheckControllerWorker, "admissionCheckControllerWorker", 2, "the number of workers to check admissionCheckState in queue units' status")
	fs.StringVar(&s.Config, "config", "", "the path to the config file")
	fs.BoolVar(&s.StrictDequeueMode, "strictDequeueMode", false, "When strict dequeue mode is enabled, QueueUnits will be SchedReady after dequeue from the queue and waiting scheduler to change the status to SchedSucceed or SchedFailed.")
	fs.StringVar(&s.QueueList, "queue-list", "", "KoordQueue Controller will only enable strictDequeueMode for the queue in the queue list.")
}

func DefaultPreemptible() (bool, bool) {
	if globalOts == nil {
		return false, false
	}
	return true, globalOts.DefaultPreemptible
}

func SetDefaultPreemptibleForTest(opt bool) {
	globalOts = &ServerOption{}
	globalOts.DefaultPreemptible = true
}
