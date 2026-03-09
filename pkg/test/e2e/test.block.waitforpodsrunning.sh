#!/bin/bash

kubectl apply -f ./elasticquotas.yaml
kubectl apply -f ./block.waitforpodsrunning.queue.yaml
kubectl apply -f ./namespace.yaml

sleep 2

kubectl apply -f jobs.yaml

sleep 2

kubectl apply -f high-prio-job.yaml

sleep 2

phase=$(kubectl get queueunits pi-3  -n test-group -ojson | jq '.status.phase')
if [[ "$phase" == "\"Dequeued\"" ]]; then
  echo "success"
else
  echo "failed"
fi