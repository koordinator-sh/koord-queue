#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${SCRIPT_ROOT}"

CONTROLLER_GEN_VERSION=${CONTROLLER_GEN_VERSION:-v0.18.0}
GOWORK=off go run sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_GEN_VERSION} \
    crd paths=./pkg/apis/... output:crd:dir=./pkg/crd

cp pkg/crd/scheduling.x-k8s.io_queues.yaml charts/v1.2.0/templates/queue-v1alpha1.yaml
cp pkg/crd/scheduling.x-k8s.io_queueunits.yaml charts/v1.2.0/templates/queueunit-v1alpha1.yaml
cp pkg/crd/scheduling.x-k8s.io_queueunits.yaml pkg/jobext/test/config/crd/queueunit-v1alpha1.yaml