"""Run the bounded offline research regression suite and record its outcome."""
import argparse
import io
import json
import logging
import platform
import unittest
from datetime import datetime, timezone
from pathlib import Path


MODULES = ['tests.test_research_pipeline', 'tests.test_worker_handoff',
           'tests.test_storage', 'tests.test_writer', 'tests.test_data_readiness',
           'tests.test_pit_leakage', 'tests.test_delivery_recovery', 'tests.test_week3_handoff']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='docs/research-validation.json')
    args = parser.parse_args()
    # Capture ordinary logs without disabling assertLogs-based safety tests.
    logging.disable(logging.NOTSET)
    logs = io.StringIO()
    logging.getLogger().handlers = [logging.StreamHandler(logs)]
    logging.getLogger().setLevel(logging.WARNING)
    suite = unittest.defaultTestLoader.loadTestsFromNames(MODULES)
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=1).run(suite)
    evidence = {'generated_at':datetime.now(timezone.utc).isoformat(), 'python':platform.python_version(),
                'modules':MODULES, 'tests_run':result.testsRun, 'failures':len(result.failures),
                'errors':len(result.errors), 'skipped':len(result.skipped), 'passed':result.wasSuccessful(),
                'scope':'Offline temporary-database/unit regressions; no live collection or trained-model results',
                'output':stream.getvalue()}
    Path(args.output).write_text(json.dumps(evidence, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({k:v for k,v in evidence.items() if k != 'output'}, indent=2))
    if not result.wasSuccessful():
        print(stream.getvalue())
        raise SystemExit(1)


if __name__ == '__main__':
    main()
