#!/bin/python3

from e2e import TestStep
from k8s import CRDClient
import yaml
import json
from utils import retry_on_failing

class Elasticquota:
    def __init__(self) -> None:
        self.args = {
            "enable": True
        }
        self.client = CRDClient("/Users/yueming/.kube/config")
        self.steps = [
            TestStep("build eqtree", self.build_eqtree_step),
            TestStep("check queue status", self.check_queue_status_step),
            TestStep("submit queueunit", self.submit_queueunit_step),
            TestStep("check queueunits", self.check_queueunit_status_step),
        ]
        self.name = "elasticquota"

    def build_eqtree_step(self, args):
        f = open("templates/eqtree.yaml")
        cfg = f.read()
        tree = yaml.load(cfg,Loader=yaml.FullLoader)
        ex = self.client.create_eqtree(tree)
        if ex is not None:
            return "Err"

    def check_queue_status(self, expect_queues):
        queue_list = self.client.list_queues("koord-queue")
        queues = queue_list["items"]
        queue_set = set()
        for queue in queues:
            queue_set.add(queue["metadata"]["name"])
        if len(expect_queues) != len(queue_set) or len(queue_set.difference(expect_queues)) != 0:
            print("W: expect:{} acutal:{}", expect_queues, queue_set)
            return "Error"
        return None

    def check_queue_status_step(self, args):
        expect_queues = set(["root-root.a", "root-root.b-root.b.1", "root-root.b-root.b.2"])
        res = retry_on_failing(self.check_queue_status, expect_queues)
        if res is not None:
            return "check queue status failed"
        print("I: check queue status succeed")
        return None

    def check_queueunit_status(self, expect_status):
        queueunit_list = self.client.list_queueunits("namespaceb2")
        queueunits = queueunit_list["items"]
        for queueunit in queueunits:
            if "status" not in queueunit:
                print("W: queueunit {} status is None, not {}".format(queueunit["metadata"]["name"], expect_status))
                return "Error"
            if queueunit["status"]["phase"] != expect_status:
                print("W; queueunit {} status is {}, not {}".format(queueunit["metadata"]["name"], queueunit["status"]["phase"], expect_status))
                return "Error"
        return None

    def check_queueunit_status_step(self, args):
        expect_queueunit_status = "Dequeued"
        res = retry_on_failing(self.check_queueunit_status, expect_queueunit_status)
        if res is not None:
            return "check queue unit status failed"
        print("I: check queue unit status succeed")
        return None

    def submit_queueunit_step(self, args):
        f = open("templates/queueunit.yaml")
        cfg = f.read()
        queueunit = yaml.load(cfg,Loader=yaml.FullLoader)
        ex = self.client.create_queueunits(queueunit)
        if ex is not None:
            return "Err"

    def Steps(self):
        return self.steps

    def Name(self):
        return self.name

    def Args(self):
        return self.args