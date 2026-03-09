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

package framework

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/kube-queue/api/pkg/client/clientset/versioned"
	"github.com/kube-queue/api/pkg/client/informers/externalversions"
	clientv1alpha1 "github.com/kube-queue/api/pkg/client/informers/externalversions/scheduling/v1alpha1"
	apiv1alpha1 "github.com/kube-queue/kube-queue/pkg/visibility/apis/v1alpha1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

// Code is the Status code/type which is returned from plugins.
type Code int

const (
	// Success means that plugin ran correctly and found pod schedulable.
	// NOTE: A nil status is also considered as "Success".
	Success Code = iota
	// Error is used for internal plugin errors, unexpected input, etc.
	Error
	// Unschedulable is used when a plugin finds a pod unschedulable. The scheduler might attempt to
	// preempt other pods to get this pod scheduled. Use UnschedulableAndUnresolvable to make the
	// scheduler skip preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	Unschedulable
	// UnschedulableAndUnresolvable is used when a PreFilter plugin finds a pod unschedulable and
	// preemption would not change anything. Plugins should return Unschedulable if it is possible
	// that the pod can get scheduled with preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	UnschedulableAndUnresolvable
	// Wait is used when a Permit plugin finds a pod scheduling should wait.
	Wait
	// Skip is used when a Bind plugin chooses to skip binding.
	Skip
)

type Framework interface {
	MultiQueueHandle

	RunFilterPlugins(context.Context, *QueueUnitInfo) *Status
	// called in scheduling cycle
	RunReservePluginsReserve(context.Context, *QueueUnitInfo, map[string]Admission) *Status
	// called when a job allocation failed before commit to apiserver
	RunReservePluginsUnreserve(context.Context, *QueueUnitInfo)
	// called when dequeue complete
	RunReservePluginsDequeued(context.Context, *QueueUnitInfo)
	// called when a job is assigned
	RunReservePluginsAddAssignedJob(context.Context, *QueueUnitInfo)
	// called when a job is deleted
	RunReservePluginsRemoveJob(context.Context, *QueueUnitInfo)

	RunReservePluginsResize(context.Context, *QueueUnitInfo, *QueueUnitInfo)
	StartQueueUnitMappingPlugin(ctx context.Context)
	RegisterApiHandler(engine *gin.Engine)
	GetQueueUnitInfoByQuota(quota string) ([]apiv1alpha1.QueueUnit, error)
}

type Admission struct {
	Name     string
	Replicas int64
}

type Status struct {
	message string
	code    Code

	admissions map[string]Admission
}

// NewStatus makes a Status out of the given arguments and returns its pointer.
func NewStatus(code Code, message string, ads map[string]Admission) *Status {
	s := &Status{
		code:    code,
		message: message,

		admissions: map[string]Admission{},
	}
	for k, v := range ads {
		s.admissions[k] = v
	}
	return s
}

func (s *Status) Admissions() map[string]Admission {
	res := map[string]Admission{}
	if s == nil {
		return res
	}
	for k, v := range s.admissions {
		res[k] = v
	}
	return res
}

func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return s.message
}

func (s *Status) AppendMessage(st string) {
	s.message += st
}

type QueueUnitInfoProvider interface {
	// GetQueueUnitQuotaName returns the quotaNames for the given unit.
	GetQueueUnitQuotaName(*v1alpha1.QueueUnit) ([]string, error)
}

// Plugin is the parent type for all the scheduling framework plugins.
type Plugin interface {
	Name() string
}

type MultiQueueSortPlugin interface {
	Plugin
	MultiQueueLess(*v1alpha1.Queue, *v1alpha1.Queue) bool
}

type QueueRequestType string

var (
	CreateQueue QueueRequestType = "create"
	DeleteQueue QueueRequestType = "delete"
	FixQueue    QueueRequestType = "fix"
	Activate    QueueRequestType = "activate"
)

type QueueRequest struct {
	QueueRequestType QueueRequestType
	Queue            *v1alpha1.Queue
}

type QueueUnitMappingPlugin interface {
	Plugin
	Mapping(*v1alpha1.QueueUnit) (string, error)
	AddEventHandler(queueInformer clientv1alpha1.QueueInformer, handle QueueManageHandle)
	Start(ctx context.Context, handle QueueManageHandle)
}

type ApiHandlerPlugin interface {
	Plugin
	RegisterApiHandler(engine *gin.Engine)
}

type MultiQueueLessFunc func(*v1alpha1.Queue, *v1alpha1.Queue) bool

// return queue name for the queue unit, return err if any validation is failed
type QueueUnitMappingFunc func(*v1alpha1.QueueUnit) (string, error)

type QueueSortPlugin interface {
	Plugin
	QueueLess(*QueueUnitInfo, *QueueUnitInfo) bool
}

type QueueLessFunc func(*QueueUnitInfo, *QueueUnitInfo) bool

type FilterPlugin interface {
	Plugin

	Filter(ctx context.Context, queueUnit *QueueUnitInfo, request map[string]Admission) *Status
}

// ScorePlugin is an interface that must be implemented by "Score" plugins to rank
// nodes that passed the filtering phase.
type ScorePlugin interface {
	Plugin
	Score(ctx context.Context, p *v1.Pod) (int64, bool)
}

type ReservePlugin interface {
	Plugin
	Reserve(ctx context.Context, queueUnit *QueueUnitInfo, result map[string]Admission) *Status
	Unreserve(ctx context.Context, queueUnit *QueueUnitInfo)
	DequeueComplete(ctx context.Context, queueUnit *QueueUnitInfo)
	AddAssignedJob(ctx context.Context, queueUnit *QueueUnitInfo)
	Remove(ctx context.Context, queueUnit *QueueUnitInfo)
	Resize(ctx context.Context, old, new *QueueUnitInfo)
}

type Handle interface {
	QueueStatusUpdateHandle
	QueueInformerFactory() externalversions.SharedInformerFactory
	SharedInformerFactory() informers.SharedInformerFactory
	KubeConfigPath() string
	QueueUnitClient() versioned.Interface
	OversellRate() float64
	KubeConfig() *rest.Config
	EventRecorder() record.EventRecorderLogger
	GetQueueUnitQuotaName(*v1alpha1.QueueUnit) ([]string, error)
}

type MultiQueueHandle interface {
	Handle
	MultiQueueSortFunc() MultiQueueLessFunc
	QueueUnitMappingFunc() QueueUnitMappingFunc
}

type QueueStatusUpdateHandle interface {
	UpdateQueueStatus(name string, details map[string][]v1alpha1.QueueItemDetail) error
}

type QueueManageHandle interface {
	QueueStatusUpdateHandle
	CreateQueue(name, generateName, policy string, priority *int32, priorityClassName string, labels, annotations, args map[string]string) error
	DeleteQueue(name string) error
	UpdateQueue(name string, policy *string, priority *int32, priorityClassName string, labels, annotations map[string]string, args ...string) error

	ListQueus() ([]*v1alpha1.Queue, error)
}
