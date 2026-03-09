from elasticquotatest import Elasticquota
from e2e import TestCaseManager

if __name__ == '__main__':
    manager = TestCaseManager()
    case1 = Elasticquota()
    manager.Register(case1.Name(), case1.Steps(), case1.Args())
    manager.Run()