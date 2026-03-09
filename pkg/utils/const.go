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

package utils

const (
	ControllerAgentName     = "koord-queue-controller"
	Default                 = "default"
	QuotaKoordQueueEnable    = "quota-koord-queue-enable"
	ParentQuotaNameLabelKey = "quota.scheduling.koordinator.sh/parent"

	AnnotationParentQuotaName = "koord-queue/parent-quota-fullname"
	AnnotationQuotaFullName   = "koord-queue/quota-fullname"

	AnnotationQuotaOversoldType = "quota.scheduling.koordinator.sh/quota-oversold-type"
	// AnnotationActualQuotaOversoldType for QuotaOversoldTypeAccept queue unit, the value of
	// AnnotationActualQuotaOversoldType depends on the filter plugin.
	AnnotationActualQuotaOversoldType = "quota.scheduling.koordinator.sh/actual-quota-oversold-type"
	QuotaOversoldTypeForbidden        = "ForbiddenQuotaOverSold"
	QuotaOversoldTypeAccept           = "AcceptQuotaOverSold"
	QuotaOversoldTypeForce            = "ForceQuotaOverSold"
	CheckSelfQuotaFailed              = "CheckSelfQuotaFailed"
	CheckParentQuotaFailed            = "CheckParentQuotaFailed"
	QueueSuspend                      = "queue-suspend"

	QueueGuaranteedUsed         = "alibabacloud.com/queue-guaranteed-used"
	QueueSelfGuaranteedUsed     = "alibabacloud.com/queue-self-guaranteed-used"
	QueueChildrenGuaranteedUsed = "alibabacloud.com/queue-children-guaranteed-used"
	QueueUsed                   = "alibabacloud.com/queue-used"
	QueueSelfUsed               = "alibabacloud.com/queue-self-used"
	QueueChildrenUsed           = "alibabacloud.com/queue-children-used"
)
