#!/bin/python3

import time

def retry_on_failing(func, args, period=10, times=10):
    i = 0
    while i < times:
        res = func(args)
        if res is not None:
            i = i + 1
            print("--we have tried for {} time".format(i))
            time.sleep(period)
        else:
            print("--succeed")
            break
    return res
