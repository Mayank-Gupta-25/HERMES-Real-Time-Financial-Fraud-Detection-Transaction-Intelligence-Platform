"""Unified test runner for HERMES executing all microservice and integration test suites."""

import subprocess
import sys

SUITES = [
    ("tests", ["pytest", "tests/", "-v"]),
    ("services/api-gateway", ["pytest", "services/api-gateway/tests/", "-v"]),
    ("services/fraud-detection", ["pytest", "services/fraud-detection/tests/", "-v"]),
    ("services/notification-service", ["pytest", "services/notification-service/tests/", "-v"]),
    ("services/transaction-processor", ["pytest", "services/transaction-processor/tests/", "-v"]),
    ("services/transaction-simulator", ["pytest", "services/transaction-simulator/tests/", "-v"]),
    ("services/analytics-service", ["pytest", "services/analytics-service/tests/", "-v"]),
    ("ml", ["pytest", "ml/tests/", "-v"]),
]

def main():
    failed = []
    passed = []
    print("=" * 70)
    print("HERMES COMPLETE TEST SUITE VERIFICATION")
    print("=" * 70)
    for name, cmd in SUITES:
        print(f"\n---> Running {name} tests: {' '.join(cmd)}")
        res = subprocess.run(cmd)
        if res.returncode == 0:
            passed.append(name)
        else:
            failed.append(name)
    
    print("\n" + "=" * 70)
    print("TEST SUITE SUMMARY")
    print("=" * 70)
    for p in passed:
        print(f" [PASS] {p}")
    for f in failed:
        print(f" [FAIL] {f}")
    print(f"\nTotal: {len(passed)} passed, {len(failed)} failed")
    sys.exit(0 if not failed else 1)

if __name__ == "__main__":
    main()
