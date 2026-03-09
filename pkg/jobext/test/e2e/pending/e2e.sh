#!/bin/bash

# this should be run in environment where koord queue has been installed
# will use a fixed namespace koord-queue-e2e-test, make sure the namespace
# is a clean namespace.

failed=true

# 定义在脚本结束时执行的代码
cleanup() {
    kubectl delete eqtree -n kube-system elasticquotatree
    kubectl create -f eqtree-backup.yaml
    rm eqtree-backup.yaml
    # for debug
    if [ "$failed" != true ]; then
        kubectl delete -f test-job.yaml
        kubectl delete ns koord-queue-e2e-test
        echo "success"
    elif [ "$failed" = true ]; then
        echo "failed: qu not Enqueued, status is $status"
    fi
}

# 使用 trap 捕捉 EXIT 信号并调用 cleanup 函数
trap cleanup EXIT

kubectl get eqtree -n kube-system elasticquotatree -o yaml > eqtree-backup.yaml

kubectl create ns koord-queue-e2e-test

kubectl delete eqtree -n kube-system elasticquotatree
kubectl create -f eqtree.yaml

kubectl create -f test-job.yaml

sleep 10

status=$(kubectl -n koord-queue-e2e-test get qu pytorch-e2e-test-py-qu -o jsonpath='{.status.phase}')
if [[ $status == "Enqueued" ]];
then
    failed=false
fi

