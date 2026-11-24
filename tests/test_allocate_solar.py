import csv
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

from scripts.allocate_solar import (
    Candidate, compare_allocations, experiment_summary, money_cents,
    rank_candidates, read_assumptions, read_candidates, select_with_budget,
)


def home(name, cost, benefit, realized=None, eligible=True):
    return Candidate(name, money_cents(cost, 'cost', positive=True), Decimal(benefit),
                     Decimal(realized) if realized is not None else None, eligible)


class AllocationTest(unittest.TestCase):
    def test_cost_normalized_ranking_and_raw_benefit_option(self):
        homes = [home('A', '10', '10'), home('B', '2', '6'), home('C', '2', '5')]
        self.assertEqual([h.building_id for h in rank_candidates(homes)], ['B', 'C', 'A'])
        self.assertEqual([h.building_id for h in rank_candidates(homes, 'predicted_benefit')], ['A', 'B', 'C'])

    def test_ratio_ranking_retains_precision_beyond_display_precision(self):
        homes = [home('A', '1', '1'), home('Z', '1', '1.0000000000000000000000000001')]
        self.assertEqual(rank_candidates(homes)[0].building_id, 'Z')

    def test_single_random_run_has_undefined_sample_standard_deviation(self):
        homes = [home('A', '1', '1')]
        _, _, results = compare_allocations(homes, 100, random_runs=1)
        summary = experiment_summary(homes, results, 100, 'benefit_per_dollar', 42, {}, 'digest')
        self.assertIsNone(summary['random_baseline']['predicted_avoided_co2_kg']['sample_standard_deviation'])

    def test_ties_are_deterministic_and_exclude_ineligible_homes(self):
        homes = [home('B', '2', '2'), home('A', '1', '1'), home('X', '1', '100', eligible=False)]
        self.assertEqual([h.building_id for h in rank_candidates(homes)], ['A', 'B'])
        _, selections, _ = compare_allocations(homes, 500, random_runs=10)
        self.assertTrue(all('X' not in [h.building_id for h in selected] for _, _, selected in selections))

    def test_skips_unaffordable_house_and_continues(self):
        homes = [home('expensive', '11', '100'), home('medium', '6', '30'), home('small', '4', '10')]
        selected = select_with_budget(rank_candidates(homes), 1000)
        self.assertEqual([h.building_id for h in selected], ['medium', 'small'])
        self.assertEqual(sum(h.cost_cents for h in selected), 1000)

    def test_exact_cent_budget(self):
        homes = [home('A', '0.10', '2'), home('B', '0.20', '1')]
        _, selections, results = compare_allocations(homes, 30, random_runs=10)
        self.assertTrue(all(len(selected) == 2 for _, _, selected in selections))
        self.assertTrue(all(row['total_cost_usd'] == '0.30' for row in results))
        self.assertTrue(all(row['unspent_budget_usd'] == '0.00' for row in results))

    def test_every_strategy_respects_same_cap_for_many_budgets(self):
        homes = [home(str(i), str(i % 5 + 1), str(i * 3)) for i in range(1, 25)]
        for budget in (0, 1, 99, 100, 599, 1500, 100000):
            with self.subTest(budget=budget):
                _, selections, _ = compare_allocations(homes, budget, random_runs=25)
                for _, _, selected in selections:
                    self.assertLessEqual(sum(h.cost_cents for h in selected), budget)
                    self.assertEqual(len(selected), len({h.building_id for h in selected}))

    def test_zero_and_insufficient_budget_do_not_divide_by_zero(self):
        for budget in (0, 99):
            _, selections, results = compare_allocations([home('A', '1', '2')], budget, random_runs=1)
            self.assertTrue(all(not selected for _, _, selected in selections))
            self.assertTrue(all(row['predicted_co2_kg_per_usd'] is None for row in results))

    def test_random_is_reproducible_independent_of_input_order_and_global_rng(self):
        homes = [home(str(i), '1', str(i)) for i in range(8)]
        global_state = random.getstate()
        first = compare_allocations(homes, 300, seed=7, random_runs=8)
        second = compare_allocations(list(reversed(homes)), 300, seed=7, random_runs=8)
        self.assertEqual(first, second)
        self.assertEqual([seed for _, seed, _ in first[1][1:]], list(range(7, 15)))
        self.assertGreater(len({tuple(h.building_id for h in selected) for _, _, selected in first[1][1:]}), 1)
        self.assertEqual(global_state, random.getstate())

    def test_realized_values_cannot_change_selections(self):
        homes = [home('A', '1', '10', '1'), home('B', '1', '1', '100')]
        changed = [replace(h, realized_co2_kg=Decimal('999') - h.realized_co2_kg) for h in homes]
        first = compare_allocations(homes, 100, seed=12, random_runs=10)
        second = compare_allocations(changed, 100, seed=12, random_runs=10)
        ids = lambda output: [[h.building_id for h in selected] for _, _, selected in output[1]]
        self.assertEqual(ids(first), ids(second))
        self.assertEqual(first[2][0]['realized_avoided_co2_kg'], '1')

    def test_partial_realized_outcomes_rejected(self):
        with self.assertRaisesRegex(ValueError, 'every candidate or none'):
            compare_allocations([home('A', '1', '2', '1'), home('B', '1', '3')], 100)

    def test_all_ineligible_and_zero_benefits_produce_valid_summaries(self):
        for homes in ([home('A', '1', '0')], [home('A', '1', '2', eligible=False)]):
            _, _, results = compare_allocations(homes, 100, random_runs=2)
            summary = experiment_summary(homes, results, 100, 'benefit_per_dollar', 42, {}, 'digest')
            self.assertIsNone(summary['comparison']['predicted_avoided_co2_kg']['percent_gain_over_random_mean'])
            self.assertIsNone(summary['random_baseline']['realized_avoided_co2_kg'])
            json.dumps(summary, allow_nan=False)

    def test_greedy_does_not_claim_global_optimality(self):
        homes = [home('A', '6', '12'), home('B', '5', '9'), home('C', '5', '9')]
        _, _, results = compare_allocations(homes, 1000, seed=0, random_runs=100)
        summary = experiment_summary(homes, results, 1000, 'benefit_per_dollar', 0, {}, 'digest')
        self.assertEqual(results[0]['predicted_avoided_co2_kg'], '12')
        self.assertLess(Decimal(summary['comparison']['predicted_avoided_co2_kg']['gain_over_random_mean']), 0)

    def test_invalid_cost_benefit_budget_and_duplicate_ids(self):
        for invalid in ('0', '-1', 'NaN', 'Infinity', '1.001'):
            with self.subTest(cost=invalid), self.assertRaises(ValueError):
                home('A', invalid, '1')
        for invalid in ('-1', 'NaN', 'Infinity'):
            with self.subTest(benefit=invalid), self.assertRaises(ValueError):
                home('A', '1', invalid)
        with self.assertRaises(ValueError):
            compare_allocations([home('A', '1', '1')], -1)
        with self.assertRaises(ValueError):
            compare_allocations([home('A', '1', '1')], 100, random_runs=0)
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            compare_allocations([home('A', '1', '1'), home('A', '2', '2')], 100)


class AllocationCLITest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.candidates = self.root / 'candidates.csv'
        self.candidates.write_text(
            'building_id,installation_cost_usd,predicted_avoided_co2_kg,realized_avoided_co2_kg,eligible\n'
            'A,6,12,10,true\nB,5,9,8,true\nC,4,4,3,true\nX,1,100,100,false\n'
        )
        self.assumptions = self.root / 'assumptions.json'
        self.assumptions.write_text(json.dumps({
            'forecast_run_id': 'synthetic-test', 'pv_profile_id': 'synthetic-test',
            'pv_capacity_kw': 4, 'emissions_source': 'synthetic fixture, not real emissions',
            'evaluation_start': '2018-10-01', 'evaluation_end': '2019-01-01',
        }))
        self.output = self.root / 'experiment'

    def run_cli(self, *extra):
        script = Path(__file__).resolve().parents[1] / 'scripts' / 'allocate_solar.py'
        return subprocess.run([
            sys.executable, str(script), str(self.candidates), str(self.output),
            '--budget-usd', '10', '--assumptions', str(self.assumptions), '--random-runs', '5', *extra,
        ], capture_output=True, text=True)

    def test_cli_saves_ranked_selected_results_and_provenance(self):
        run = self.run_cli()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual({p.name for p in self.output.iterdir()}, {
            'ranked_households.csv', 'selected_households.csv', 'strategy_results.csv', 'summary.json',
        })
        with (self.output / 'selected_households.csv').open() as stream:
            selected = list(csv.DictReader(stream))
        priority = [row for row in selected if row['strategy'] == 'prioritized']
        self.assertEqual([row['building_id'] for row in priority], ['A', 'C'])
        self.assertEqual(sum(Decimal(row['installation_cost_usd']) for row in priority), Decimal('10'))
        with (self.output / 'strategy_results.csv').open() as stream:
            results = list(csv.DictReader(stream))
        self.assertEqual(len(results), 6)
        for result in results:
            rows = [row for row in selected if row['strategy'] == result['strategy'] and row['seed'] == result['seed']]
            self.assertEqual(sum(Decimal(row['installation_cost_usd']) for row in rows), Decimal(result['total_cost_usd']))
            self.assertEqual(sum(Decimal(row['predicted_avoided_co2_kg']) for row in rows), Decimal(result['predicted_avoided_co2_kg']))
        self.assertTrue(all(Decimal(row['total_cost_usd']) <= 10 for row in results))
        summary = json.loads((self.output / 'summary.json').read_text())
        self.assertEqual(summary['eligible_count'], 3)
        self.assertEqual(len(summary['input_sha256']), 64)
        self.assertEqual(summary['prioritized']['realized_avoided_co2_kg'], '13')

    def test_estimated_only_zero_budget_saves_empty_selections(self):
        self.candidates.write_text('building_id,installation_cost_usd,predicted_avoided_co2_kg\nA,6,12\n')
        run = self.run_cli('--budget-usd', '0')
        self.assertEqual(run.returncode, 0, run.stderr)
        with (self.output / 'selected_households.csv').open() as stream:
            self.assertEqual(list(csv.DictReader(stream)), [])
        summary = json.loads((self.output / 'summary.json').read_text())
        self.assertIsNone(summary['prioritized']['realized_avoided_co2_kg'])
        self.assertIn('Estimated benefits only', summary['evaluation'])

    def test_existing_experiment_is_never_overwritten(self):
        self.output.mkdir()
        marker = self.output / 'keep.txt'
        marker.write_text('keep')
        run = self.run_cli()
        self.assertEqual(run.returncode, 2)
        self.assertEqual(marker.read_text(), 'keep')
        self.assertEqual(list(self.output.iterdir()), [marker])

    def test_bad_input_fails_before_creating_outputs(self):
        for value in ('-1', 'NaN', '1.001'):
            with self.subTest(budget=value):
                run = self.run_cli('--budget-usd', value)
                self.assertEqual(run.returncode, 2)
                self.assertFalse(self.output.exists())

    def test_malformed_candidate_tables(self):
        original = self.candidates.read_text()
        for content in (
            original + 'A,2,3,4,true\n',
            original.replace('A,6,12,10,true', 'A,6,12,,true'),
            original.replace('X,1,100,100,false', 'X,1,100,100,maybe'),
            'building_id,installation_cost_usd\nA,1\n',
            'building_id,installation_cost_usd,predicted_avoided_co2_kg\n',
            'building_id,building_id,installation_cost_usd,predicted_avoided_co2_kg\nA,A,1,2\n',
        ):
            with self.subTest(content=content), self.assertRaises(ValueError):
                self.candidates.write_text(content)
                read_candidates(self.candidates)

    def test_missing_and_reversed_assumptions(self):
        self.assumptions.write_text('{}')
        with self.assertRaises(ValueError):
            read_assumptions(self.assumptions)
        self.assumptions.write_text(json.dumps({
            'forecast_run_id': 'test', 'pv_profile_id': 'test', 'pv_capacity_kw': 4,
            'emissions_source': 'synthetic', 'evaluation_start': '2019-01-01', 'evaluation_end': '2018-01-01',
        }))
        with self.assertRaises(ValueError):
            read_assumptions(self.assumptions)


if __name__ == '__main__':
    unittest.main()
