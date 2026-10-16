#!/usr/bin/env bash

# Copyright 2020 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${SCRIPT_ROOT}"

ENVTEST_K8S_VERSION=${ENVTEST_K8S_VERSION:-1.33}
ENVTEST_VERSION=${ENVTEST_VERSION:-71f7db556ca57ce7ea6563f77d739f0d2a54233a}
ENVTEST=${ENVTEST:-${SCRIPT_ROOT}/bin/setup-envtest}

if [[ -z "${KUBEBUILDER_ASSETS:-}" ]]; then
    if [[ ! -x "${ENVTEST}" ]]; then
        GOWORK=off GOBIN="${SCRIPT_ROOT}/bin" go install sigs.k8s.io/controller-runtime/tools/setup-envtest@${ENVTEST_VERSION}
    fi
    if KUBEBUILDER_ASSETS="$(${ENVTEST} use -i "${ENVTEST_K8S_VERSION}" -p path 2>/dev/null)"; then
        export KUBEBUILDER_ASSETS
    else
        KUBEBUILDER_ASSETS="$(${ENVTEST} use "${ENVTEST_K8S_VERSION}" -p path --bin-dir "${SCRIPT_ROOT}/bin/k8s")"
        export KUBEBUILDER_ASSETS
    fi
fi

echo "Using KUBEBUILDER_ASSETS=${KUBEBUILDER_ASSETS}"

packages=$(GOWORK=off go list ./pkg/jobext/test/integration/... ./pkg/test/integration/...)
for package in ${packages}; do
    GOWORK=off go test -mod=readonly -count=1 "${package}" "$@"
done
