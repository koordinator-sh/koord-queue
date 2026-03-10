#!/bin/bash

if [[ "$1" == "cleanup" ]]; then
  kubectl delete -f ./elasticquotas-less-resource.yaml
  kubectl delete -f ./priority.queue.yaml
  kubectl delete -f ./namespace.yaml
  exit 0
fi

kubectl apply -f ./elasticquotas-less-resource.yaml
kubectl apply -f ./priority.queue.yaml
kubectl apply -f ./namespace.yaml

sleep 2

kubectl apply -f jobs1.yaml

sleep 2

phase="$(kubectl get queueunits pi-2 -n test-group -ojson | jq '.status.phase')"
if [[ "$phase" == "\"Dequeued\"" ]]; then
  echo "success"
else
  echo "failed, phase: $phase"
fi