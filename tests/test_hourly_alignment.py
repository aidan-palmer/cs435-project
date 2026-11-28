import unittest
from pyspark.sql import SparkSession, functions as F
from scripts.hourly_alignment import aggregate_intervals, est_2018_to_utc


class AlignmentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.master('local[2]').appName('hour alignment tests').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def fixture(self):
        return self.spark.sql("""SELECT 'A' building_id,
            timestamp('2018-01-01 00:00:00') + id * INTERVAL 15 MINUTES interval_end,
            cast(id as double) interval_kwh, cast(-id as double) temperature_c
            FROM range(1,9)""")

    def test_interval_ending_on_hour_belongs_to_previous_hour(self):
        hourly=aggregate_intervals(self.fixture()).orderBy('hour_start').select(
            F.date_format('hour_start','yyyy-MM-dd HH:mm').alias('label'),'demand_kwh','temperature_c').collect()
        self.assertEqual([r.label for r in hourly],['2018-01-01 00:00','2018-01-01 01:00'])
        self.assertEqual([r.demand_kwh for r in hourly],[10,26])
        self.assertEqual([r.temperature_c for r in hourly],[-2.5,-6.5])

    def test_missing_and_duplicate_intervals_fail(self):
        frame=self.fixture()
        for bad in [frame.limit(7),frame.unionByName(frame.limit(1))]:
            with self.assertRaisesRegex(ValueError,'Incomplete or duplicate'):aggregate_intervals(bad)

    def test_fixed_est_wrap_and_dst_are_explicit(self):
        df=self.spark.createDataFrame([('2018-01-01 00:00:00',),('2018-01-01 02:00:00',),
            ('2018-11-04 01:00:00',),('2018-11-04 02:00:00',)],['label'])
        rows=df.select(F.date_format(est_2018_to_utc(F.to_timestamp('label')),'yyyy-MM-dd HH:mm').alias('utc')).collect()
        self.assertEqual([r.utc for r in rows],['2019-01-01 05:00','2018-01-01 07:00','2018-11-04 06:00','2018-11-04 07:00'])


if __name__=='__main__':unittest.main()
