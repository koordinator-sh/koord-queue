#!/usr/bin/env bash


set -o errexit
set -o nounset
set -o pipefail

source "./vendor/k8s.io/code-generator/kube_codegen.sh"

cd pkg/apis/config/hack; kube::codegen::gen_helpers \
    --boilerplate "./boilerplate.go.txt" \
    "../"