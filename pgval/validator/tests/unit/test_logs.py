"""Contract tests against the stand's files and malformed/changed logs."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from pgval.validator.main import main, oracle, read, read_records, timestamp, validate

ROOT = Path(__file__).resolve().parents[4]
RUN = ROOT / 'app/checks/sst-gen/results/20261103T155921'
PLAN = ROOT / 'pgval/validator/examples/sst-archive-plan.json'


class LogTests(unittest.TestCase):
    def test_archive(self):
        expected = oracle(read_records(RUN / 'input.tsv'), read(PLAN))
        result = validate(expected, read_records(RUN / 'output.jsonl'), True)
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['correct_results'], 60)
        self.assertEqual(result['missing_contributions'], 0)

    def test_reader_formats_and_key_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'log'
            row = dict(id=1, timestamp='1970-01-01T00:00:00.000', page='/a')
            for value in (json.dumps([row]), json.dumps(row)+'\n', '"/a"\t'+json.dumps(row)):
                path.write_text(value)
                self.assertEqual(read_records(path), [row])
            path.write_text('"/wrong"\t'+json.dumps(row))
            with self.assertRaisesRegex(ValueError, 'key differs'):
                read_records(path)
            path.write_text('not JSON')
            with self.assertRaises(ValueError):
                read_records(path)
            path.write_text('')
            self.assertEqual(read_records(path), [])

    def test_epoch_milliseconds(self):
        self.assertEqual(timestamp(1793700000000), timestamp('2026-11-03T10:00:00.000'))
        for invalid in (True, 1.5, None):
            with self.assertRaises(ValueError):
                timestamp(invalid)

    def test_cli_observed_inputs_and_late_output(self):
        data = Path(__file__).parent / 'data'
        with tempfile.TemporaryDirectory() as directory:
            observed, late, report = [Path(directory) / name for name in ('click.jsonl', 'late.jsonl', 'report.json')]
            rows = read(data / 'inputs.json')
            # Kafka consumption may interleave partitions in a different order.
            observed.write_text(''.join(json.dumps(row)+'\n' for row in reversed(rows)))
            late.write_text('')
            args = ['validator', '--plan', str(data/'plan.json'), '--inputs', str(data/'inputs.json'),
                    '--observed-inputs', str(observed), '--outputs', str(data/'expected.json'),
                    '--late-outputs', str(late), '--complete', '--report', str(report)]
            def run():
                with patch.object(sys, 'argv', args), contextlib.redirect_stdout(io.StringIO()):
                    return main(), read(report)
            self.assertEqual(run()[0], 0)
            late.write_text('{"page":"/a","clickId":1}\n')
            self.assertEqual(run()[1]['status'], 'fail')
            observed.write_text(''.join(json.dumps(row)+'\n' for row in rows[:-1]))
            self.assertEqual(run()[1]['status'], 'invalid_input')


class ActionPlanValidationTests(unittest.TestCase):
    def test_plan_is_source_of_truth(self):
        from pgval.generator.main import generate
        plan, events = generate(generator='sst-uniform', windows=1, pages=1, events_per_window=2,
                                faults=[dict(id='restart', type='fault', scenario='process_restart',
                                             target='taskmanager', duration_ms=1000,
                                             trigger=dict(type='test_start', delay_ms=100))])
        manual = [dict(page='/page-1', windowStart=0, windowEnd=60000, clickIds=[1,2], updateIds=[])]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, data in [('plan',plan),('outputs',manual),('inputs',events)]:
                (root/name).write_text(json.dumps(data))
            args = ['validator', '--plan', str(root/'plan'), '--outputs', str(root/'outputs'),
                    '--complete', '--report', str(root/'report')]
            def run(extra=()):
                with patch.object(sys, 'argv', args+list(extra)), contextlib.redirect_stdout(io.StringIO()):
                    code = main()
                return code, read(root/'report')
            code, report = run()
            self.assertEqual(code, 0)
            self.assertEqual(report['fault_execution'], 'not_checked')
            self.assertEqual(run(['--inputs',str(root/'inputs')])[0],0)
            (root/'inputs').write_text(json.dumps(events[:-1]))
            self.assertEqual(run(['--inputs',str(root/'inputs')])[1]['status'], 'invalid_input')
            manual[0]['clickIds'] = [1,1]
            (root/'outputs').write_text(json.dumps(manual))
            self.assertEqual(run()[1]['status'], 'fail')
