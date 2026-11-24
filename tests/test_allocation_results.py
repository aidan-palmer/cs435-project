import csv
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from scripts.allocate_solar import Candidate, compare_allocations, experiment_summary, save_experiment
from scripts.allocation_results import AllocationResults


class AllocationResultsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.run = self.root / 'run'
        self.homes = [Candidate(str(index), 100, Decimal(index), Decimal(index * 2)) for index in range(1, 8)]
        self.write_run(self.run, self.homes, budget=300)
        self.store = AllocationResults(self.run)

    def write_run(self, path, homes, budget):
        ranked, selections, results = compare_allocations(homes, budget, seed=8, random_runs=3)
        assumptions = dict(forecast_run_id='test', pv_profile_id='test', pv_capacity_kw='4',
                           emissions_source='synthetic tests only', evaluation_start='2018-01-01', evaluation_end='2019-01-01')
        summary = experiment_summary(homes, results, budget, 'benefit_per_dollar', 8, assumptions, 'test')
        save_experiment(path, homes, ranked, selections, results, summary)

    def test_empty_configuration_does_not_present_example_as_real(self):
        self.assertEqual(AllocationResults().view(), {'available': False, 'is_example': False})

    def test_explicit_example_is_labeled_and_generated_with_allocator(self):
        example = AllocationResults().view(example=True)
        self.assertTrue(example['is_example'])
        self.assertEqual(example['summary']['prioritized']['predicted_avoided_co2_kg'], '1600')
        self.assertEqual(len(example['results']), 101)
        self.assertTrue(all(home['building_id'].startswith('example-') for home in example['selection']['homes']))

    def test_saved_results_and_pagination(self):
        first = self.store.view(page_size=2)
        second = self.store.view(page=2, page_size=2)
        self.assertFalse(first['is_example'])
        self.assertEqual(first['selection']['total'], 3)
        self.assertEqual(first['selection']['pages'], 2)
        self.assertEqual([home['building_id'] for home in first['selection']['homes']], ['7', '6'])
        self.assertEqual([home['building_id'] for home in second['selection']['homes']], ['5'])

    def test_seed_filter_matches_corresponding_run(self):
        payload = self.store.view(strategy='random', seed=9)
        selected = payload['selection']['homes']
        result = next(row for row in payload['results'] if row['seed'] == 9)
        self.assertEqual(len(selected), result['selected_count'])
        self.assertEqual(sum(Decimal(row['predicted_avoided_co2_kg']) for row in selected), Decimal(result['predicted_avoided_co2_kg']))

    def test_top_twenty_keep_ranking_and_budget_selection_separate(self):
        output = self.root / 'many'
        homes = [Candidate(str(index), 100, Decimal(index), Decimal(index * 2)) for index in range(1, 31)]
        self.write_run(output, homes, budget=100)
        payload = AllocationResults(output).view()
        ranking = payload['ranking']
        self.assertEqual(ranking['total'], 30)
        self.assertEqual(len(ranking['homes']), 20)
        self.assertEqual([home['rank'] for home in ranking['homes']], list(range(1, 21)))
        self.assertEqual(ranking['homes'][0]['building_id'], '30')
        self.assertEqual(sum(home['selected_prioritized'] for home in ranking['homes']), 1)
        self.assertFalse(ranking['homes'][1]['selected_in_run'])
        random = AllocationResults(output).view(strategy='random', seed=9)
        selected = {home['building_id'] for home in random['selection']['homes']}
        self.assertTrue(all(home['selected_in_run'] == (home['building_id'] in selected) for home in random['ranking']['homes']))

    def test_legacy_run_without_ranking_remains_available(self):
        (self.run / 'ranked_households.csv').unlink()
        payload = self.store.view()
        self.assertFalse(payload['ranking']['available'])
        self.assertEqual(payload['selection']['total'], 3)

    def test_invalid_ranking_order_is_rejected(self):
        path = self.run / 'ranked_households.csv'
        lines = path.read_text().splitlines()
        lines[1], lines[2] = lines[2], lines[1]
        path.write_text('\n'.join(lines) + '\n')
        with self.assertRaisesRegex(ValueError, 'ranking order'):
            self.store.view()

    def test_invalid_queries_are_rejected(self):
        for query in ({'strategy':'unknown'}, {'strategy':'random','seed':999}, {'page':0}, {'page':10},
                      {'strategy':'prioritized','seed':8}, {'page_size':0}, {'page_size':101}):
            with self.subTest(query=query), self.assertRaises(ValueError):
                self.store.view(**query)

    def test_empty_selection_still_has_valid_first_page(self):
        output = self.root / 'zero'
        self.write_run(output, self.homes, budget=0)
        selection = AllocationResults(output).view()['selection']
        self.assertEqual(selection['homes'], [])
        self.assertEqual(selection['pages'], 1)

    def test_missing_realized_data_stays_unavailable(self):
        output = self.root / 'predicted'
        homes = [Candidate('1', 100, Decimal(10))]
        self.write_run(output, homes, budget=100)
        payload = AllocationResults(output).view()
        self.assertTrue(all(row['realized_avoided_co2_kg'] is None for row in payload['results']))
        self.assertIsNone(payload['selection']['homes'][0]['realized_avoided_co2_kg'])

    def test_missing_or_malformed_files_have_useful_errors(self):
        with self.assertRaisesRegex(ValueError, 'Could not load'):
            AllocationResults(self.root / 'missing').view()
        (self.run / 'summary.json').write_text('not json')
        with self.assertRaisesRegex(ValueError, 'Could not load'):
            self.store.view()

    def test_refresh_observes_changed_artifacts(self):
        before = self.store.view()['summary']['assumptions']['forecast_run_id']
        path = self.run / 'summary.json'
        summary = json.loads(path.read_text())
        summary['assumptions']['forecast_run_id'] = 'new-run'
        path.write_text(json.dumps(summary))
        after = self.store.view()['summary']['assumptions']['forecast_run_id']
        self.assertEqual(before, 'test')
        self.assertEqual(after, 'new-run')

    def test_inconsistent_selected_benefit_is_rejected(self):
        path = self.run / 'selected_households.csv'
        with path.open() as stream:
            reader = csv.DictReader(stream)
            fields, rows = reader.fieldnames, list(reader)
        rows[0]['predicted_avoided_co2_kg'] = '999'
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(ValueError, 'benefits do not match'):
            self.store.view()

    def test_budget_violation_is_rejected(self):
        path = self.run / 'summary.json'
        summary = json.loads(path.read_text())
        summary['budget_usd'] = '1.00'
        path.write_text(json.dumps(summary))
        with self.assertRaisesRegex(ValueError, 'budget'):
            self.store.view()

    def test_duplicate_household_within_a_run_is_rejected(self):
        path = self.run / 'selected_households.csv'
        lines = path.read_text().splitlines()
        path.write_text('\n'.join(lines + [lines[1]]) + '\n')
        with self.assertRaisesRegex(ValueError, 'duplicate household'):
            self.store.view()


if __name__ == '__main__':
    unittest.main()
