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

package admissioncontroller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgofake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("AdmissionController", func() {
	var (
		controller *AdmissionControlelr
		scheme     *runtime.Scheme
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())

		controller = &AdmissionControlelr{
			cli: clientgofake.NewClientBuilder().WithScheme(scheme).Build(),
		}
	})

	Describe("buildXorAdmit", func() {
		var (
			admissions []v1alpha1.Admission
			admitted   map[string][]*corev1.Pod
			unadmitted map[string][]*corev1.Pod
			result     map[string][]*corev1.Pod
		)

		BeforeEach(func() {
			admissions = []v1alpha1.Admission{
				{
					Name:     "podset-1",
					Replicas: 2,
				},
				{
					Name:     "podset-2",
					Replicas: 1,
				},
			}

			admitted = map[string][]*corev1.Pod{
				"podset-1": {
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Labels: map[string]string{"scheduler-admission": "true"}}},
				},
				"podset-2": {
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Labels: map[string]string{"scheduler-admission": "true"}}},
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Labels: map[string]string{"scheduler-admission": "true"}}},
				},
			}

			unadmitted = map[string][]*corev1.Pod{
				"podset-1": {
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-2"}},
				},
			}
		})

		JustBeforeEach(func() {
			result = controller.buildXorAdmit(admissions, admitted, unadmitted)
		})

		Context("When building XOR admit pods", func() {
			It("should correctly combine admitted and unadmitted pods", func() {
				Expect(result).To(HaveLen(2))
				Expect(result["podset-1"]).To(HaveLen(1))
				Expect(result["podset-1"][0].Name).To(Equal("pod-2"))

				Expect(result["podset-2"]).To(HaveLen(1))
				Expect(result["podset-2"][0].Name).To(Equal("pod-3"))
			})
		})
	})
})
