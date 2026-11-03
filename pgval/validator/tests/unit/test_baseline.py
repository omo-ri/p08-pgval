import copy
from pathlib import Path
import unittest
from pgval.validator.main import oracle, read, validate

DATA = Path(__file__).resolve().parent / 'data'

class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.plan = read(DATA / 'plan.json')
        self.inputs = read(DATA / 'inputs.json')
        self.expected = read(DATA / 'expected.json')
        self.actual = copy.deepcopy(self.expected)

    def test_manual_oracle_and_window_boundary(self):
        self.assertEqual(oracle(self.inputs, self.plan), self.expected)

    def test_correct(self):
        self.assertEqual(validate(self.expected, self.actual, True)['status'], 'pass')

    def test_compensating_loss_and_repeat(self):
        self.actual = read(DATA / 'corrupted.json')
        r = validate(self.expected, self.actual, True)
        self.assertEqual((r['missing_contributions'],r['extra_contributions']), (1,1))

    def test_missing_output(self):
        self.actual.pop(0)
        self.assertEqual(validate(self.expected,self.actual,True)['missing_contributions'],2)

    def test_duplicate_output(self):
        self.actual.append(copy.deepcopy(self.actual[0]))
        self.assertEqual(validate(self.expected,self.actual,True)['extra_contributions'],2)

    def test_wrong_page(self):
        self.actual[0]['page'] = '/wrong'
        self.assertEqual(validate(self.expected,self.actual,True)['status'],'fail')

    def test_wrong_window(self):
        self.actual[0]['windowEnd'] = '1970-01-01T00:01:00.001'
        self.assertEqual(validate(self.expected,self.actual,True)['status'],'fail')

    def test_unexpected_update(self):
        self.actual[0]['updateIds'] = [17]
        self.assertEqual(validate(self.expected,self.actual,True)['status'],'fail')

    def test_incomplete_run(self):
        r=validate(self.expected,[],False)
        self.assertEqual(r['status'],'incomplete')
        self.assertIsNone(r['missing_contributions'])

    def test_no_tail_event(self):
        with self.assertRaises(ValueError): oracle(self.inputs[:-1],self.plan)

    def test_duplicate_input_rejected(self):
        with self.assertRaises(ValueError): oracle([self.inputs[0]] + self.inputs,self.plan)

    def test_late_input_out_of_scope(self):
        with self.assertRaises(ValueError): oracle(list(reversed(self.inputs)),self.plan)

    def test_output_order_not_significant(self):
        self.actual.reverse()
        for row in self.actual: row['clickIds'].reverse()
        self.assertEqual(validate(self.expected,self.actual,True)['status'],'pass')


class GuaranteeTests(unittest.TestCase):
    def setUp(self):
        self.expected = read(DATA / 'expected.json')

    def test_a1_allows_identical_repeated_final_result(self):
        actual = self.expected + [copy.deepcopy(self.expected[0])]
        report = validate(self.expected, actual, True, 'a1')
        self.assertEqual(report['status'], 'pass')
        self.assertFalse(report['strict_output_match'])
        self.assertEqual(report['extra_contributions'], 2)
        self.assertEqual(validate(self.expected, actual, True, 'e1')['status'], 'fail')

    def test_a1_still_rejects_loss_and_wrong_output(self):
        self.assertEqual(validate(self.expected,self.expected[1:],True,'a1')['status'],'fail')
        actual=copy.deepcopy(self.expected)
        actual[0]['clickIds']=[1,1]
        self.assertEqual(validate(self.expected,actual,True,'a1')['status'],'fail')

    def test_flink_needs_no_kstreams_grace(self):
        plan=read(DATA / 'plan.json')
        inputs=read(DATA / 'inputs.json')
        inputs[-1]['timestamp']='1970-01-01T00:02:00.001'
        self.assertEqual(oracle(inputs,plan),self.expected)

if __name__ == '__main__': unittest.main()
