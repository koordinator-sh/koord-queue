#!/bin/bash

action=$1
if [[ "$action" == "cleanup" ]]; then
  kubectl delete -f ./elasticquotas.yaml
  kubectl delete -f ./block.waitforpodsrunning.queue.yaml
  kubectl delete -f ./namespace.yaml
  kubectl apply -f jobs.yaml
  kubectl apply -f high-prio-job.yaml
  exit 0
fi

kubectl apply -f ./elasticquotas.yaml
kubectl apply -f ./block.waitforpodsrunning.queue.yaml
kubectl apply -f ./namespace.yaml

sleep 2

kubectl apply -f jobs.yaml

sleep 2

kubectl apply -f high-prio-job.yaml

sleep 20

phase=$(kubectl get queueunits pi-3  -n test-group -ojson | jq '.status.phase')
if [[ "$phase" == "\"Dequeued\"" ]]; then
  echo "success"
else
  echo "failed"
fi