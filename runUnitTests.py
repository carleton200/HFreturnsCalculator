import sys
import os
from scripts.instantiate_basics import instantiate_basics
instantiate_basics(BASE_DIR= os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))) #prepares values needed for other class functionality and imports
from unitTests.nodeRecursion import nodeRecursion
from unitTests.reportGeneration import pSnap
from unitTests.basicFuncs import dNavSort

allTests = [nodeRecursion,dNavSort, pSnap]
runTests = [pSnap]
ignoreTests = []

#either run everything except for ignored, unless runTests is given, then run only those
if runTests:
    allTests = runTests

results = []
for test in (test for test in allTests if test not in ignoreTests):
    results.append(test())

if all(res for res in results):
    print('All tests passed')
else:
    passRate = float(len([res for res in results if res]) / len(results))
    failed_tests = [allTests[i].__name__ for i, res in enumerate(results) if not res]
    print(f"Failed tests: {failed_tests}")
    print(f"Pass rate: {round(passRate * 100,2)}%")
