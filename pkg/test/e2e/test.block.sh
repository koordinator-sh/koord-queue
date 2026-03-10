#!/bin/bash

action=$1
if [[ "$action" == "cleanup" ]]; then
  kubectl delete -f ./elasticquotas-less-resource.yaml
  kubectl delete -f ./block.queue.yaml
  kubectl delete -f ./namespace.yaml
  kubectl apply -f jobs2.yaml
  exit 0
fi

kubectl apply -f ./elasticquotas-less-resource.yaml
kubectl apply -f ./block.queue.yaml
kubectl apply -f ./namespace.yaml

sleep 2

kubectl apply -f jobs2.yaml

sleep 2

phase="$(kubectl get queueunits pi-2 -n test-group -ojson | jq '.status.phase')"
if [[ "$phase" == "\"Enqueued\"" ]] || [[ "$phase" == "" ]] || [[ $phase == null ]] ; then
  echo "success"
else
  echo "failed, phase: $phase"
fi