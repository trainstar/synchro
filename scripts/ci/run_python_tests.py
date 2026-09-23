from __future__ import annotations

import argparse
import json
import unittest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("modules", nargs="+")
    arguments = parser.parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromNames(arguments.modules)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    summary = {
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "expected_failures": len(result.expectedFailures),
        "unexpected_successes": len(result.unexpectedSuccesses),
    }
    passed = (
        result.wasSuccessful()
        and result.testsRun > 0
        and not result.skipped
        and not result.expectedFailures
    )
    print(json.dumps({"passed": passed, **summary}, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
