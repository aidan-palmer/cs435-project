import json
from pathlib import Path
import tempfile
import unittest

from scripts.study_results import StudyResults


class StudyResultsTest(unittest.TestCase):
    def fixture(self,directory,registry=None):
        base=Path(directory)
        (base/'results.json').write_text(json.dumps({'evidence':[{'id':'pilot'}]}))
        (base/'forecast_series.json').write_text(json.dumps({'128':{'2018-11-08':{'actual':[1,2]}}}))
        (base/'experiments.json').write_text(json.dumps(registry or {}))
        (base/'runtime.csv').write_text('run,seconds\nexample,1\n')
        return StudyResults(base,repo=base)

    def test_missing_bundle_and_forecast_selection(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(StudyResults(d).view(),{'available':False})
            result=self.fixture(d)
            self.assertTrue(result.view()['available'])
            self.assertEqual(result.forecast('128','2018-11-08')['actual'],[1,2])
            with self.assertRaises(ValueError):result.forecast('unknown','2018-11-08')

    def test_only_named_downloads_and_registered_paths_are_exposed(self):
        with tempfile.TemporaryDirectory() as d:
            result=self.fixture(d,{'malicious':'../../outside'})
            self.assertEqual(result.download('runtime')[0],'runtime.csv')
            with self.assertRaises(ValueError):result.download('../../secrets')
            with self.assertRaises(ValueError):result.allocation('unknown')
            with self.assertRaisesRegex(ValueError,'outside'):result.allocation('malicious')

    def test_rebuilt_bundle_refreshes_the_cached_snapshot(self):
        with tempfile.TemporaryDirectory() as d:
            result=self.fixture(d)
            self.assertEqual(result.view()['evidence'][0]['id'],'pilot')
            (Path(d)/'results.json').write_text(json.dumps({'evidence':[{'id':'updated-snapshot'}]}))
            self.assertEqual(result.view()['evidence'][0]['id'],'updated-snapshot')

    def test_all_actual_registered_cases_load_in_existing_allocation_view(self):
        study=StudyResults()
        if not study.view().get('available'):self.skipTest('Generated study bundle is absent')
        entries=study.view()['allocations']
        self.assertEqual(len(entries),720 if study.view().get('full_study') else 56)
        for entry in entries:
            with self.subTest(experiment=entry['id']):
                result=study.allocation(entry['id']).view()
                self.assertTrue(result['available'])
                self.assertEqual(int(result['summary']['eligible_count']),10 if entry['cohort']=='pilot' else 382)


if __name__=='__main__':unittest.main()
