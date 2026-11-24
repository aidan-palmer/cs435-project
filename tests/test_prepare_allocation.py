import csv
from datetime import timedelta
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F

from scripts.allocate_solar import read_candidates, read_assumptions
from scripts.prepare_allocation import eligible_ids, save_inputs, score_households, windows


class PreparationHelpersTest(unittest.TestCase):
    def test_clock_change_windows(self):
        _, spring, spring_end = windows('2018-03-11', '2018-03-12', 7)
        _, fall, fall_end = windows('2018-11-04', '2018-11-05', 7)
        self.assertEqual((spring_end-spring).total_seconds()/3600, 23)
        self.assertEqual((fall_end-fall).total_seconds()/3600, 25)
        for days in (0, 6, 8, -7):
            with self.assertRaises(ValueError):
                windows('2018-01-08', '2018-01-10', days)
        with self.assertRaises(ValueError):
            windows('2018-01-10', '2018-01-08')

    def test_detached_eligibility_and_metadata_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'metadata.csv'
            path.write_text('bldg_id,upgrade,in.geometry_building_type_acs\nA,0,Single-Family Detached\nB,0,20 to 49 Unit\n')
            self.assertEqual(eligible_ids(path, {'A','B'}), {'A'})
            with self.assertRaisesRegex(ValueError, 'no baseline metadata'):
                eligible_ids(path, {'A','C'})
            with path.open('a') as stream:
                stream.write('A,0,Single-Family Detached\n')
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                eligible_ids(path, {'A','B'})

    def test_generated_files_are_accepted_by_allocator(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'inputs'
            assumptions = dict(forecast_run_id='test',pv_profile_id='test',pv_capacity_kw='4',
                               emissions_source='Synthetic test',evaluation_start='2018-01-08',evaluation_end='2018-01-10')
            scores = [dict(building_id='A', predicted_grid_reduction_kwh=96,realized_grid_reduction_kwh=144,
                           predicted_avoided_co2_kg=48,realized_avoided_co2_kg=72,scored_hours=48)]
            path = save_inputs(output, scores, 500000, {'A'}, assumptions)
            self.assertEqual(read_candidates(path)[0].predicted_co2_kg, Decimal(48))
            self.assertEqual(read_assumptions(output/'allocation-assumptions.json')['pv_capacity_kw'], '4')
            with self.assertRaises(FileExistsError):
                save_inputs(output, scores, 500000, {'A'}, assumptions)


class PreparationSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('allocation preparation tests').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self, evaluation_demand=100):
        start, cutoff, end = windows('2018-01-08','2018-01-10',7)
        hours = int((end-start).total_seconds()/3600)
        return (self.spark.range(hours)
                .withColumn('building_id',F.lit('A'))
                .withColumn('hour_start_utc',F.timestamp_seconds(F.lit(int(start.timestamp()))+F.col('id')*3600))
                .withColumn('demand_kwh',F.when(F.col('id')>=168,F.lit(float(evaluation_demand))).otherwise(F.lit(2.0)))
                .withColumn('solar_generation_kwh',F.lit(3.0)))

    def test_benefits_use_frozen_history_and_match_hourly_equations(self):
        row = score_households(self.fixture(),'2018-01-08','2018-01-10',training_days=7,factor=.5)[0]
        self.assertEqual(row['scored_hours'],48)
        self.assertEqual(row['predicted_grid_reduction_kwh'],96)
        self.assertEqual(row['predicted_avoided_co2_kg'],48)
        self.assertEqual(row['realized_avoided_co2_kg'],72)
        changed = score_households(self.fixture(evaluation_demand=0),'2018-01-08','2018-01-10',training_days=7,factor=.5)[0]
        self.assertEqual(changed['predicted_avoided_co2_kg'], row['predicted_avoided_co2_kg'])
        self.assertEqual(changed['realized_avoided_co2_kg'],0)

    def test_missing_or_duplicate_hours_are_rejected(self):
        frame = self.fixture()
        for invalid in (frame.where('id != 1'),frame.unionByName(frame.limit(1))):
            with self.assertRaisesRegex(ValueError,'complete, unique'):
                score_households(invalid,'2018-01-08','2018-01-10',training_days=7)

    def test_invalid_energy_and_schema_are_rejected(self):
        frame = self.fixture()
        for value in (-1.0,float('nan'),float('inf')):
            with self.subTest(value=value),self.assertRaises(ValueError):
                score_households(frame.withColumn('demand_kwh',F.lit(value)),'2018-01-08','2018-01-10',training_days=7)
        with self.assertRaisesRegex(ValueError,'Missing joined'):
            score_households(frame.drop('solar_generation_kwh'),'2018-01-08','2018-01-10',training_days=7)
        with self.assertRaisesRegex(ValueError,'UTC timestamp'):
            score_households(frame.withColumn('hour_start_utc',F.col('hour_start_utc').cast('string')),'2018-01-08','2018-01-10',training_days=7)

    def test_decimal_energy_columns_are_supported(self):
        frame = self.fixture().withColumn('demand_kwh', F.col('demand_kwh').cast('decimal(12,3)')).withColumn('solar_generation_kwh', F.col('solar_generation_kwh').cast('decimal(12,3)'))
        row = score_households(frame,'2018-01-08','2018-01-10',training_days=7,factor=.5)[0]
        self.assertEqual(row['predicted_avoided_co2_kg'],48)
        self.assertEqual(row['realized_avoided_co2_kg'],72)

    def test_non_utc_spark_session_is_rejected(self):
        frame = self.fixture()
        self.spark.conf.set('spark.sql.session.timeZone','America/Denver')
        try:
            with self.assertRaisesRegex(ValueError,'timezone UTC'):
                score_households(frame,'2018-01-08','2018-01-10',training_days=7)
        finally:
            self.spark.conf.set('spark.sql.session.timeZone','UTC')

    def test_zero_emissions_factor_is_a_valid_explicit_scenario(self):
        row = score_households(self.fixture(),'2018-01-08','2018-01-10',training_days=7,factor=0)[0]
        self.assertEqual(row['predicted_avoided_co2_kg'],0)
        self.assertEqual(row['realized_avoided_co2_kg'],0)


if __name__=='__main__':
    unittest.main()
