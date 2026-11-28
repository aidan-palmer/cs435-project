import tempfile
from pathlib import Path
import unittest
from pyspark.sql import SparkSession
from scripts.housing_features import join_housing


class HousingFeatureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.master('local[2]').appName('static housing features').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def test_static_feature_allowlist_excludes_energy_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'metadata.csv'
            path.write_text('bldg_id,upgrade,in.sqft,in.heating_fuel,annual_future_kwh\nA,0,1000,Electricity,999999\nB,0,2000,Natural Gas,0\n')
            frame=self.spark.createDataFrame([('A',1.),('B',2.)],['building_id','label'])
            enriched,columns,inventory=join_housing(frame,path)
            self.assertEqual(columns,['housing_floor_area_sqft','housing_fuel_0','housing_fuel_1'])
            self.assertNotIn('annual_future_kwh',enriched.columns)
            rows=enriched.orderBy('building_id').collect()
            self.assertEqual(rows[0].housing_floor_area_sqft,1000.)
            self.assertEqual((rows[0].housing_fuel_0,rows[0].housing_fuel_1),(1.,0.))
            self.assertEqual(inventory['fields'],['in.sqft','in.heating_fuel'])

    def test_missing_static_metadata_fails_without_dropping_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'metadata.csv';path.write_text('bldg_id,upgrade,in.sqft,in.heating_fuel\nA,0,1000,Electricity\n')
            frame=self.spark.createDataFrame([('A',),('B',)],['building_id'])
            with self.assertRaisesRegex(ValueError,'Missing'):join_housing(frame,path)


if __name__=='__main__':unittest.main()
