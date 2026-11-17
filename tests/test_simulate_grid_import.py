import unittest

from pyspark.sql import SparkSession, functions as F

from scripts.simulate_grid_import import simulate_grid_import


class GridImportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("SolarOpt grid-import tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def hourly_fixture(self):
        return self.spark.sql("""
            SELECT * FROM VALUES
                (1, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 2.0D, 0.0D),
                (2, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 2.0D, 0.5D),
                (3, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 2.0D, 2.0D),
                (4, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 2.0D, 3.5D),
                (5, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 0.0D, 1.5D),
                (6, TIMESTAMP '2018-01-01 07:00:00', '2018-01-01T00:00:00-07:00', 0.0D, 0.0D)
            AS hourly(building_id, hour_start_utc, hour_start_mountain,
                      demand_kwh, solar_generation_kwh)
        """)

    def test_hourly_flows(self):
        expected = [
            (2.0, 2.0, 0.0, 0.0),
            (2.0, 1.5, 0.5, 0.0),
            (2.0, 0.0, 2.0, 0.0),
            (2.0, 0.0, 2.0, 1.5),
            (0.0, 0.0, 0.0, 1.5),
            (0.0, 0.0, 0.0, 0.0),
        ]
        rows = simulate_grid_import(self.hourly_fixture()).orderBy("building_id").collect()
        for row, flows in zip(rows, expected, strict=True):
            with self.subTest(building_id=row.building_id):
                self.assertEqual((
                    row.grid_import_without_solar_kwh,
                    row.grid_import_with_solar_kwh,
                    row.self_consumed_solar_kwh,
                    row.excess_solar_kwh,
                ), flows)
                self.assertEqual(row.grid_reduction_kwh, row.self_consumed_solar_kwh)
                self.assertEqual(row.demand_kwh, row.grid_import_with_solar_kwh + row.self_consumed_solar_kwh)
                self.assertEqual(row.solar_generation_kwh, row.self_consumed_solar_kwh + row.excess_solar_kwh)
                self.assertEqual(row.hour_start_mountain, "2018-01-01T00:00:00-07:00")

    def test_invalid_energy(self):
        hourly = self.hourly_fixture().limit(1)
        for name in ("demand_kwh", "solar_generation_kwh"):
            for value in (None, -1.0, float("nan"), float("inf")):
                with self.subTest(column=name, value=value):
                    invalid = hourly.withColumn(name, F.lit(value).cast("double"))
                    with self.assertRaisesRegex(ValueError, "finite nonnegative"):
                        simulate_grid_import(invalid)

    def test_duplicate_building_hour(self):
        hourly = self.hourly_fixture().limit(1)
        with self.assertRaisesRegex(ValueError, "Duplicate building and UTC hour"):
            simulate_grid_import(hourly.unionByName(hourly))

    def test_unaligned_hour(self):
        hourly = self.hourly_fixture().limit(1).withColumn(
            "hour_start_utc", F.expr("hour_start_utc + INTERVAL 15 MINUTES")
        )
        with self.assertRaisesRegex(ValueError, "aligned UTC hours"):
            simulate_grid_import(hourly)

    def test_missing_column(self):
        with self.assertRaisesRegex(ValueError, "Missing hourly columns: solar_generation_kwh"):
            simulate_grid_import(self.hourly_fixture().drop("solar_generation_kwh"))


if __name__ == "__main__":
    unittest.main()
