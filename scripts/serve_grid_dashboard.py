"""Serve a local dashboard for the hourly grid-import simulation."""

import argparse
import csv
import json
import logging
from datetime import date
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from pyspark.sql import SparkSession, functions as F


MEASURES = (
    "grid_import_without_solar_kwh",
    "grid_import_with_solar_kwh",
    "solar_generation_kwh",
    "excess_solar_kwh",
)


BUILDING_FIELDS = {
    "building_type": "in.geometry_building_type_acs",
    "floor_area_sqft": "in.sqft",
    "construction_decade": "in.vintage",
    "heating_fuel": "in.heating_fuel",
}
DEFAULT_METADATA = (
    Path(__file__).resolve().parents[2] / "DATA_for_project"
    / "CO_baseline_metadata_and_annual_results.csv"
)


def read_building_details(path, buildings):
    details = {}
    with path.open(encoding="utf-8-sig", newline="") as source:
        rows = csv.DictReader(source)
        required = {"bldg_id", "upgrade", *BUILDING_FIELDS.values()}
        if missing := required - set(rows.fieldnames or ()):
            raise ValueError(f"{path}: missing metadata columns: {', '.join(sorted(missing))}")
        building_ids = {str(building) for building in buildings}
        for row in rows:
            building = row["bldg_id"]
            if row["upgrade"] != "0" or building not in building_ids:
                continue
            if building in details:
                raise ValueError(f"{path}: duplicate baseline metadata for building {building}")
            details[building] = {
                name: (row[column] or "").strip() for name, column in BUILDING_FIELDS.items()
            }
    return details


class GridData:
    def __init__(self, hourly, metadata_path=None):
        self.hourly = hourly.select(
            "building_id", "hour_start_utc", "hour_start_mountain", *MEASURES,
            F.substring("hour_start_mountain", 1, 10).alias("day"),
        )
        inventory = self.hourly.agg(
            F.sort_array(F.collect_set("building_id")).alias("buildings"),
            F.min("day").alias("first_day"),
            F.max("day").alias("last_day"),
        ).first()
        if not inventory.buildings or not inventory.first_day:
            raise ValueError("The simulation contains no buildings with hourly data")
        self.metadata = inventory.asDict()
        self.buildings = set(inventory.buildings)
        self.metadata["building_details"] = (
            read_building_details(metadata_path, self.buildings) if metadata_path else {}
        )

    def select_buildings(self, building):
        if building == "all":
            return self.hourly
        if not building.isascii() or not building.isdecimal():
            raise ValueError("Enter a building ID or select All buildings.")
        building_id = int(building)
        if building_id not in self.buildings:
            raise ValueError("That building ID is absent from the simulation.")
        return self.hourly.where(F.col("building_id") == building_id)

    @lru_cache(maxsize=128)
    def day(self, building, day):
        hourly = self.select_buildings(building)
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError("Enter a date as YYYY-MM-DD.")
        if not self.metadata["first_day"] <= day <= self.metadata["last_day"]:
            raise ValueError("Choose a date inside the simulation period.")
        records = (
            hourly.where(F.col("day") == day)
            .groupBy("hour_start_utc", "hour_start_mountain")
            .agg(*(F.sum(name).alias(name) for name in MEASURES))
            .orderBy("hour_start_utc")
            .drop("hour_start_utc")
            .collect()
        )
        if not records:
            raise ValueError("There are no hourly results for that building and date.")
        return {"hours": [record.asDict() for record in records]}

    @lru_cache(maxsize=128)
    def most_productive_day(self, building):
        result = (
            self.select_buildings(building)
            .groupBy("day")
            .agg(F.sum("solar_generation_kwh").alias("generation"))
            .orderBy(F.desc("generation"), F.asc("day"))
            .first()
        )
        return {"day": result.day}


def dashboard_handler(data, page):
    class DashboardHandler(BaseHTTPRequestHandler):
        def send_content(self, status, content_type, body):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status, payload):
            body = json.dumps(payload, allow_nan=False).encode("utf-8")
            self.send_content(status, "application/json; charset=utf-8", body)

        def do_GET(self):
            target = urlsplit(self.path)
            query = parse_qs(target.query)
            building = query.get("building", ["all"])[0]
            try:
                match target.path:
                    case "/":
                        self.send_content(HTTPStatus.OK, "text/html; charset=utf-8", page)
                        return
                    case "/api/metadata":
                        payload = data.metadata
                    case "/api/day":
                        payload = data.day(building, query.get("date", [""])[0])
                    case "/api/most-productive-day":
                        payload = data.most_productive_day(building)
                    case _:
                        self.send_json(HTTPStatus.NOT_FOUND, {"error": "Page not found."})
                        return
                self.send_json(HTTPStatus.OK, payload)
            except ValueError as error:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            except Exception:
                logging.exception("Dashboard request failed")
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "Could not read simulation data."})

    return DashboardHandler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("hourly", help="Hourly Parquet output from simulate_grid_import.py")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--metadata", type=Path, help="Colorado ResStock baseline metadata CSV for building labels"
    )
    args = parser.parse_args()

    metadata_path = args.metadata
    if metadata_path is None and DEFAULT_METADATA.is_file():
        metadata_path = DEFAULT_METADATA

    page = Path(__file__).with_name("grid_dashboard.html").read_bytes()
    spark = SparkSession.builder.appName("SolarOpt grid dashboard").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        try:
            grid = GridData(spark.read.parquet(args.hourly), metadata_path)
        except (OSError, ValueError) as error:
            parser.error(str(error))
        with HTTPServer(("127.0.0.1", args.port), dashboard_handler(grid, page)) as server:
            print(f"Grid dashboard ready at http://127.0.0.1:{server.server_port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
