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

package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"

	"github.com/kube-queue/kube-queue/cmd/app/options"
	app "github.com/kube-queue/kube-queue/cmd/app/server"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies"
)

func main() {
	s := options.NewServerOption()
	s.AddFlags(flag.CommandLine)
	queuepolicies.AddCommandLine(flag.CommandLine)
	klog.InitFlags(flag.CommandLine)

	flag.Parse()
	s.Register(flag.CommandLine)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":10259", nil)

	if err := app.Run(s); err != nil {
		log.Fatalln(err)
	}
}
