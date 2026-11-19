from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.serve_grid_dashboard import (
    BUILDING_FIELDS, MAP_GEOGRAPHY, MEASURES, building_map_area,
    read_building_details, summarize_map_day, summarize_map_hours,
)


class DashboardGeographyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.geography = json.loads(MAP_GEOGRAPHY.read_text())

    def test_city_takes_precedence_over_broader_region(self):
        self.assertEqual(building_map_area({
            "city": "CO, Fort Collins", "puma": "G08000103",
        }, self.geography), "place:0827425")
        self.assertEqual(building_map_area({
            "city": "CO, Loveland", "puma": "G08000102",
        }, self.geography), "place:0846465")

    def test_unspecified_city_uses_only_published_census_area(self):
        for city in ("Not in a census Place", "In another census Place", ""):
            with self.subTest(city=city):
                self.assertEqual(building_map_area({
                    "city": city, "puma": "G08000102",
                }, self.geography), "puma:0800102")

    def test_unknown_location_does_not_use_weather_station(self):
        self.assertIsNone(building_map_area({
            "city": "CO, Windsor", "puma": "unrecognized",
            "weather_file_latitude": "40.44", "weather_file_longitude": "-104.62",
        }, self.geography))
        self.assertIsNone(building_map_area({}, self.geography))

    def test_city_totals_and_unmapped_preserve_all_energy(self):
        records = [
            dict(building_id=1, **dict(zip(MEASURES, [12, 7, 8, 3]))),
            dict(building_id=2, **dict(zip(MEASURES, [20, 14, 8, 2]))),
            dict(building_id=3, **dict(zip(MEASURES, [10, 6, 8, 4]))),
            dict(building_id=4, **dict(zip(MEASURES, [5, 2, 8, 5]))),
        ]
        details = {
            "1": {"map_area_id": "place:0827425"},
            "2": {"map_area_id": "place:0827425"},
            "3": {"map_area_id": "puma:0800102"},
        }
        result = summarize_map_day(records, details)
        self.assertEqual(len(result["areas"]), 1)
        city = result["areas"][0]
        self.assertEqual(city["building_count"], 2)
        self.assertEqual(city["grid_import_without_solar_kwh"], 32)
        self.assertEqual(city["grid_reduction_kwh"], 11)
        self.assertEqual(result["unmapped"]["building_count"], 2)
        for measure in MEASURES:
            self.assertEqual(city[measure] + result["unmapped"][measure],
                             sum(row[measure] for row in records))

    def test_no_metadata_keeps_every_building_unmapped(self):
        result = summarize_map_day([dict(building_id=1, **dict.fromkeys(MEASURES, 0))], {})
        self.assertEqual(result["areas"], [])
        self.assertEqual(result["unmapped"]["building_count"], 1)
        self.assertEqual(result["unmapped"]["grid_reduction_kwh"], 0)

    def test_hourly_groups_preserve_counts_and_unknown_city_energy(self):
        instant = datetime(2018, 1, 1, 19)
        records = [
            dict(hour_start_utc=instant, hour_start_mountain="2018-01-01T12:00:00-07:00",
                 map_area_id=area, building_count=count, **dict.fromkeys(MEASURES, energy))
            for area, count, energy in [
                ("place:0827425", 3, 9), ("puma:0800102", 2, 4), ("", 1, 2),
            ]
        ]
        result = summarize_map_hours(records)[0]
        self.assertEqual(result["total_buildings"], 6)
        self.assertEqual(result["areas"][0]["building_count"], 3)
        self.assertEqual(result["unmapped"]["building_count"], 3)
        self.assertEqual(result["unmapped"][MEASURES[0]], 6)
        self.assertEqual(result["hour_start_utc"], "2018-01-01T19:00:00+00:00")

    def test_repeated_fall_hours_remain_distinct_and_sorted_by_utc(self):
        records = [
            dict(hour_start_utc=datetime(2018, 11, 4, utc_hour),
                 hour_start_mountain="2018-11-04T01:00:00" + offset,
                 map_area_id="place:0827425", building_count=1,
                 **dict.fromkeys(MEASURES, 0))
            for utc_hour, offset in [(8, "-07:00"), (7, "-06:00")]
        ]
        hours = summarize_map_hours(records)
        self.assertEqual(len(hours), 2)
        self.assertTrue(hours[0]["hour_start_mountain"].endswith("-06:00"))
        self.assertTrue(hours[1]["hour_start_mountain"].endswith("-07:00"))
        self.assertEqual(hours[0]["areas"][0]["solar_generation_kwh"], 0)

    def test_formatted_utc_keys_keep_all_25_fall_hours(self):
        start = datetime(2018, 11, 4, 6, tzinfo=timezone.utc)
        instants = [start + timedelta(hours=index) for index in range(25)]
        records = [dict(
            hour_start_utc=instant.isoformat(),
            hour_start_mountain=instant.astimezone(ZoneInfo("America/Denver")).isoformat(),
            map_area_id="place:0827425", building_count=1,
            **dict.fromkeys(MEASURES, 0),
        ) for instant in reversed(instants)]
        result = summarize_map_hours(records)
        self.assertEqual(len(result), 25)
        repeated = [row for row in result if row["hour_start_mountain"][11:13] == "01"]
        self.assertEqual(len(repeated), 2)
        self.assertNotEqual(repeated[0]["hour_start_utc"], repeated[1]["hour_start_utc"])

    def test_location_columns_are_optional_for_older_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.csv"
            with path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["bldg_id", "upgrade", *BUILDING_FIELDS.values()])
                writer.writeheader()
                writer.writerow({"bldg_id": "1", "upgrade": "0"})
            details = read_building_details(path, [1])["1"]
            self.assertEqual(details["city"], "")
            self.assertEqual(details["puma"], "")

    def test_boundaries_have_expected_ids_and_polygon_geometry(self):
        features = self.geography["features"]
        self.assertEqual({f["properties"]["area_id"] for f in features}, {
            "place:0827425", "place:0846465", "puma:0800102", "puma:0800103",
        })
        self.assertTrue(all(f["geometry"]["type"] in ("Polygon", "MultiPolygon") for f in features))
        self.assertTrue(all("tigerWMS_ACS2018" in url for url in self.geography["source_urls"]))


if __name__ == "__main__":
    unittest.main()
