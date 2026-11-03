import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from pgval.generator.main import generate
from pgval.plan import planned_events

FAULT = dict(id='restart', type='fault', scenario='process_restart', target='taskmanager',
             trigger=dict(type='action', action_id='publish-main', phase='started', delay_ms=100),
             duration_ms=1000, parameters={})


class ActionPlanTests(unittest.TestCase):
    def test_embedded_events_and_fault(self):
        plan, events = generate(faults=[FAULT])
        self.assertEqual(plan['schema_version'], 2)
        self.assertEqual(planned_events(plan), events)
        self.assertEqual([a['type'] for a in plan['actions']], ['publish', 'fault', 'publish'])
        self.assertNotIn('file', plan['input'])
        plan['actions'][1]['target'] = 'other'
        self.assertEqual(FAULT['target'], 'taskmanager')

    def test_bad_actions(self):
        base, _ = generate(faults=[FAULT])
        changes = [lambda p: p['actions'][1].update(id='publish-main'),
                   lambda p: p['actions'][1]['trigger'].update(action_id='publish-tail'),
                   lambda p: p['actions'][1].update(type='unknown'),
                   lambda p: p['actions'][1].update(duration_ms=-1),
                   lambda p: p['actions'][1].update(trigger=[]),
                   lambda p: p['actions'][0].update(rate_per_second=0),
                   lambda p: p['actions'][-1].update(trigger={'type':'test_start'})]
        for change in changes:
            with self.subTest(change=change):
                plan = copy.deepcopy(base)
                change(plan)
                with self.assertRaises(ValueError): planned_events(plan)

    def test_signal_trigger(self):
        fault = copy.deepcopy(FAULT)
        fault['trigger'] = dict(type='signal', name='window_closed', timeout_ms=5000)
        plan, events = generate(faults=[fault])
        self.assertEqual(planned_events(plan), events)
        plan['actions'][1]['trigger'].pop('timeout_ms')
        with self.assertRaises(ValueError): planned_events(plan)

    def test_cli_only_writes_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'case'
            faults = Path(directory)/'faults.json'
            faults.write_text(json.dumps([FAULT]))
            subprocess.run(['python3', '-m', 'pgval.generator', '--out-dir', str(output),
                            '--faults-json', str(faults)], check=True, capture_output=True)
            self.assertEqual([p.name for p in output.iterdir()], ['plan.json'])
            self.assertEqual(len(planned_events(json.loads((output/'plan.json').read_text()))), 21)
