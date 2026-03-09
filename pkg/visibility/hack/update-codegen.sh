#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# run this script in pkg/visibility/hack/
CURRENT_DIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$(realpath "${CURRENT_DIR}/../../..")
PKG="github.com/koordinator-sh/koord-queue"
CODEGEN_PKG=$ROOT/vendor/k8s.io/code-generator

cd "$CURRENT_DIR/.."

# shellcheck source=/dev/null
source "${CODEGEN_PKG}/kube_codegen.sh"

# Generating conversion and defaults functions
kube::codegen::gen_helpers \
  --boilerplate "${ROOT}/hack/boilerplate/boilerplate.generatego.txt" \
  "${ROOT}/pkg/visibility/apis"

# Generating OpenAPI for Kueue API extensions
kube::codegen::gen_openapi \
  --boilerplate "${ROOT}/hack/boilerplate/boilerplate.generatego.txt" \
  --output-dir "${ROOT}/pkg/visibility/apis/openapi" \
  --output-pkg "${PKG}/pkg/visibility/apis/openapi" \
  --update-report \
  "${ROOT}/pkg/visibility/apis"

kube::codegen::gen_client \
  --boilerplate "${ROOT}/hack/boilerplate/boilerplate.generatego.txt" \
  --output-dir "${ROOT}/pkg/visibility/apis/client-go" \
  --output-pkg "${PKG}/pkg/visibility/apis/client-go" \
  --with-watch \
  --with-applyconfig \
  "${ROOT}/pkg/visibility"
