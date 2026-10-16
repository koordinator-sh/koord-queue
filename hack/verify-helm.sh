#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${SCRIPT_ROOT}"

chart=charts/v1.2.0
rendered=$(mktemp)
trap 'rm -f "${rendered}"' EXIT

helm lint --strict "${chart}"
helm template koord-queue "${chart}" > "${rendered}"

grep -Fq 'ghcr.io/koordinator-sh/koord-queue:v1.9.0' "${rendered}"
grep -Fq 'ghcr.io/koordinator-sh/koord-queue-controllers:v1.9.0' "${rendered}"
if grep -Eq '^[[:space:]]*image:.*:latest([[:space:]]|$)' "${rendered}"; then
    echo 'rendered chart must not contain mutable latest image tags' >&2
    exit 1
fi

feature_gates='MaximumExecutionTime=false,QueueUnitActive=false,QueueUnitConditions=true,QueueUnitRequeueState=false'
if [[ $(grep -Fc -- "--feature-gates=${feature_gates}" "${rendered}") -ne 2 ]]; then
    echo 'feature gates must be identical in both deployments' >&2
    exit 1
fi

grep -Fq -- '--config=/etc/koord-queue/jobextensions/config.yaml' "${rendered}"
grep -Fq -- '--enable-pod-reclaim=false' "${rendered}"
grep -Fq 'checksum/jobextensions-config:' "${rendered}"
grep -Fq 'partialRunningTimeout: 0s' "${rendered}"

helm template koord-queue "${chart}" \
    --set featureGates.MaximumExecutionTime=true \
    --set featureGates.QueueUnitActive=true >/dev/null

if helm template koord-queue "${chart}" \
    --set featureGates.MaximumExecutionTime=true >/dev/null 2>&1; then
    echo 'MaximumExecutionTime rendered without QueueUnitActive' >&2
    exit 1
fi

if helm template koord-queue "${chart}" \
    --set featureGates.QueueUnitRequeueState=true \
    --set featureGates.QueueUnitConditions=false >/dev/null 2>&1; then
    echo 'QueueUnitRequeueState rendered without QueueUnitConditions' >&2
    exit 1
fi
