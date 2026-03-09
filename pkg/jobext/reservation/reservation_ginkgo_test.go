/*
Copyright 2023.

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

package reservation

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	clientgofake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	schedulingv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
)

var _ = Describe("ReservationController", func() {
	var (
		controller *ReservationController
		scheme     *runtime.Scheme
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(v1.AddToScheme(scheme)).To(Succeed())
		Expect(schedulingv1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(koordinatorschedulerv1alpha1.AddToScheme(scheme)).To(Succeed())

		controller = &ReservationController{
			cli: clientgofake.NewClientBuilder().WithScheme(scheme).Build(),
		}
	})

	Describe("GetJobBlackList", func() {
		Context("When ConfigMap has valid data", func() {
			var (
				blacklistConfigMap *v1.ConfigMap
			)

			BeforeEach(func() {
				blacklistConfigMap = &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kubedl-blacklist-data",
						Namespace: "kube-system",
					},
					Data: map[string]string{
						"global_anomalousNodesGlobal_xingyun-aiph": `{"node1": [{"lastUpdateTimestamp": "2025-04-14T07:00:00Z", "errorMsg": "error", "errorCode": 1}]}`,
					},
				}
				Expect(controller.cli.Create(ctx, blacklistConfigMap)).To(Succeed())
			})

			It("should return correct node selector terms", func() {
				nodeSelectorTerms, _ := controller.GetJobBlackList(ctx, klog.Background())
				Expect(nodeSelectorTerms).To(HaveLen(1))

				expectedTerm := v1.NodeSelectorRequirement{
					Key:      "kubernetes.io/hostname",
					Operator: v1.NodeSelectorOpNotIn,
					Values:   []string{"node1"},
				}
				Expect(nodeSelectorTerms[0]).To(Equal(expectedTerm))
			})
		})

		Context("When ConfigMap has expired data", func() {
			BeforeEach(func() {
				expiredConfigMap := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kubedl-blacklist-data",
						Namespace: "kube-system",
					},
					Data: map[string]string{
						"global_anomalousNodesGlobal_xingyun-aiph": `{"node1": [{"lastUpdateTimestamp": "2023-04-14T07:00:00Z", "errorMsg": "error", "errorCode": 1}]}`,
					},
				}
				Expect(controller.cli.Create(ctx, expiredConfigMap)).To(Succeed())
			})

			It("should return correct node selector terms", func() {
				nodeSelectorTerms, _ := controller.GetJobBlackList(ctx, klog.Background())
				Expect(nodeSelectorTerms).To(HaveLen(1))

				expectedTerm := v1.NodeSelectorRequirement{
					Key:      "kubernetes.io/hostname",
					Operator: v1.NodeSelectorOpNotIn,
					Values:   []string{"node1"},
				}
				Expect(nodeSelectorTerms[0]).To(Equal(expectedTerm))
			})
		})
	})

	Describe("CreateReservations", func() {
		var (
			reservations []koordinatorschedulerv1alpha1.Reservation
		)

		BeforeEach(func() {
			reservations = []koordinatorschedulerv1alpha1.Reservation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-reservation-1",
					},
					Spec: koordinatorschedulerv1alpha1.ReservationSpec{
						Template: &v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "test-container",
										Image: "test-image:latest",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("2Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
			}
		})

		Context("When creating reservations", func() {
			It("should create reservations without error", func() {
				err := controller.CreateReservations(ctx, klog.Background(), reservations)
				Expect(err).ToNot(HaveOccurred())

				// 验证 reservation 是否创建成功
			})
		})
	})
})
