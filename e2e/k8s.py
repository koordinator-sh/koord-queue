#!/bin/python3

from kubernetes import config, dynamic
from kubernetes import client as k8s_client
from kubernetes.dynamic.exceptions import ResourceNotFoundError
from kubernetes.client import api_client

class CRDClient:
    def __init__(self, kube_config="") -> None:
        if kube_config != "":
            config.load_kube_config(config_file=kube_config)
        else:
            config.load_kube_config()
        self.api = k8s_client.CustomObjectsApi()
        # print(self.api.list_namespaced_custom_object(group="scheduling.sigs.k8s.io", plural="elasticquotatrees", version="v1beta1", namespace="kube-system"))

    def create_eqtree(self, obj):
        try:
            self.api.create_namespaced_custom_object(namespace="kube-system", group="scheduling.sigs.k8s.io", plural="elasticquotatrees", version="v1beta1", body=obj)
        except Exception as ex:
            print("E: create eqtree failed: {}".format(ex))
            return ex

    def list_queues(self, namespace="default"):
        return self.api.list_namespaced_custom_object(group="scheduling.x-k8s.io", version="v1alpha1", plural="queues", namespace=namespace)

    def list_queueunits(self, namespace="default"):
        return self.api.list_namespaced_custom_object(group="scheduling.x-k8s.io", version="v1alpha1", plural="queueunits", namespace=namespace)

    def create_queueunits(self, obj):
        try:
            self.api.create_namespaced_custom_object(group="scheduling.x-k8s.io", version="v1alpha1", plural="queueunits", namespace=obj["metadata"]["namespace"], body=obj)
        except Exception as ex:
            print("E: create queue unit failed: {}".format(ex))
            return ex

    def delete_queueunits(self, name, namespace="default"):
        self.api.delete_namespaced_custom_object(group="scheduling.x-k8s.io", version="v1alpha1", plural="queueunits", namespace=namespace, name=name)

    