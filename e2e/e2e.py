#!/bin/python3

class TestStep:
    def __init__(self, name, step) -> None:
        self.name = name
        self.step = step

    def Run(self, args: dict):
        return self.step(args)

    def Name(self):
        return self.name

class TestCaseManager:
    def __init__(self) -> None:
        self.testcases = {}
        self.testargs = {}

    def Register(self, name: str, steps: list[TestStep], args: dict):
        self.testcases[name] = steps
        self.testargs[name] = args

    def Run(self):
        for name, steps in self.testcases.items():
            print("test {} start".format(name))
            args = self.testargs[name]
            if not args["enable"]:
                continue
            for step in steps:
                err = step.Run(args)
                if err is not None:
                    print("-step {}/{} failed, err {}".format(name, step.Name(), err))
                    return "Error"
                print("-step {}/{} succeess".format(name, step.Name()))
            print("test {} succeess".format(name))
        return "Test Success"