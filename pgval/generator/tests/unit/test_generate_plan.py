import unittest
from pgval.generator.main import generate, encode
from pgval.validator.main import oracle, timestamp

class GeneratorTests(unittest.TestCase):
    def test_reproducible(self):
        self.assertEqual(generate(seed=7), generate(seed=7))
        self.assertNotEqual(generate(seed=7)[1], generate(seed=8)[1])

    def test_boundaries_ids_tail_and_plan(self):
        plan, events = generate(seed=3, windows=3, pages=2, events_per_window=4)
        self.assertEqual(len(events), 13)
        self.assertEqual(len({e['id'] for e in events}), 13)
        times = [timestamp(e['timestamp']) for e in events]
        self.assertEqual(times, sorted(times))
        for start in (0,60000,120000):
            self.assertIn(start, times)
            self.assertIn(start+59999,times)
        self.assertEqual(times[-1],180001)
        from pgval.plan import planned_events
        self.assertEqual(planned_events(plan), events)
        expected = oracle(events, plan)
        self.assertEqual(sum(len(r['clickIds']) for r in expected),12)

    def test_known_single_page_case(self):
        plan, events = generate(windows=2,pages=1,events_per_window=2)
        result = oracle(events,plan)
        self.assertEqual([r['clickIds'] for r in result],[[1,2],[3,4]])
        self.assertEqual([r['page'] for r in result],['/page-1','/page-1'])

    def test_one_event_window(self):
        plan, events = generate(windows=1,pages=1,events_per_window=1)
        self.assertEqual(oracle(events,plan)[0]['clickIds'],[1])

    def test_bad_config(self):
        for kwargs in ({'windows':0},{'pages':-1},{'events_per_window':0},{'windows':10001},{'events_per_window':100001}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                generate(**kwargs)


class ModularGeneratorTests(unittest.TestCase):
    def test_uniform_known_distribution(self):
        plan, events = generate(generator='sst-uniform', windows=1, pages=2, events_per_window=3)
        self.assertEqual([timestamp(e['timestamp']) for e in events], [0,29999,59999,60001])
        self.assertEqual([e['page'] for e in events[:-1]], ['/page-1','/page-2','/page-1'])
        self.assertEqual([r['clickIds'] for r in oracle(events, plan)], [[1,3],[2]])

    def test_uniform_seed_independent(self):
        self.assertEqual(generate(generator='sst-uniform',seed=1)[1],generate(generator='sst-uniform',seed=2)[1])

    def test_all_plugins_match_shared_contract(self):
        from pgval.generator.generators import GENERATORS
        for name in GENERATORS:
            for count in (1,2,11):
                with self.subTest(name=name,count=count):
                    plan,events=generate(generator=name,windows=3,events_per_window=count)
                    self.assertEqual(plan['generator']['name'],name)
                    self.assertEqual(sum(len(r['clickIds']) for r in oracle(events,plan)),3*count)

    def test_unknown_generator_and_unsupported_window(self):
        with self.assertRaises(ValueError): generate(generator='missing')
        with self.assertRaises(ValueError): generate(window_ms=10000)

    def test_invalid_plugin_output_rejected(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        plugin=SimpleNamespace(name='bad',version=1,events=lambda config: [(0,'/wrong')])
        with patch('pgval.generator.main.get_generator',return_value=plugin):
            with self.assertRaises(ValueError): generate()

if __name__ == '__main__': unittest.main()
