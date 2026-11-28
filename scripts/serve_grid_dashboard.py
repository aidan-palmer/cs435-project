"""Serve a local dashboard for the hourly grid-import simulation."""

import argparse
import csv
import json
import logging
from datetime import date, timezone
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from pyspark.sql import SparkSession, functions as F

if __package__:
    from .pvwatts_api import FORT_COLLINS_POINTS
    from .allocation_results import AllocationResults
    from .study_results import StudyResults, DEFAULT_RESULTS
else:
    from pvwatts_api import FORT_COLLINS_POINTS
    from allocation_results import AllocationResults
    from study_results import StudyResults, DEFAULT_RESULTS


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
LOCATION_FIELDS = {"city": "in.city", "county": "in.county_name", "puma": "in.puma"}
MAP_GEOGRAPHY = Path(__file__).with_name("larimer_map_areas.geojson")


def building_map_area(details, geography):
    """Match only published city/PUMA tags; weather coordinates are not home locations."""
    features = geography["features"]
    city = details.get("city", "")
    for feature in features:
        area = feature["properties"]
        if area["kind"] == "place" and city == "CO, " + area["name"].removesuffix(" city"):
            return area["area_id"]
    puma = details.get("puma", "")
    for feature in features:
        area = feature["properties"]
        geoid = area["geoid"]
        if area["kind"] == "puma" and puma == "G" + geoid[:2] + "0" + geoid[2:]:
            return area["area_id"]
    return None


def summarize_map_day(records, details):
    """Sum simulated building kWh by known city, retaining unplaced totals for coverage."""
    areas = {}
    unmapped = {"building_count": 0, **dict.fromkeys(MEASURES, 0.0)}
    for record in records:
        record = record.asDict() if hasattr(record, "asDict") else record
        area_id = record.get("map_area_id") if "map_area_id" in record else (
            details.get(str(record["building_id"]), {}).get("map_area_id")
        )
        if area_id and area_id.startswith("place:"):
            target = areas.setdefault(area_id, {
                "area_id": area_id, "building_count": 0, **dict.fromkeys(MEASURES, 0.0),
            })
        else:
            target = unmapped
        target["building_count"] += record.get("building_count", 1)
        for measure in MEASURES:
            target[measure] += record[measure]
    for target in [*areas.values(), unmapped]:
        target["grid_reduction_kwh"] = (
            target["grid_import_without_solar_kwh"] - target["grid_import_with_solar_kwh"]
        )
    return {"areas": [areas[key] for key in sorted(areas)], "unmapped": unmapped}


def summarize_map_hours(records):
    """Retain UTC ordering and Mountain offsets, including the repeated fall hour."""
    grouped = {}
    for record in records:
        record = record.asDict() if hasattr(record, "asDict") else record
        instant = record["hour_start_utc"]
        grouped.setdefault(instant, []).append(record)
    result = []
    for instant, rows in sorted(grouped.items()):
        summary = summarize_map_day(rows, {})
        result.append({
            "hour_start_utc": instant if isinstance(instant, str) else instant.replace(tzinfo=timezone.utc).isoformat(),
            "hour_start_mountain": rows[0]["hour_start_mountain"],
            "total_buildings": sum(row["building_count"] for row in rows),
            **summary,
        })
    return result


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
            details[building].update({
                name: (row.get(column) or "").strip() for name, column in LOCATION_FIELDS.items()
            })
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
        self.metadata["solar_reference_points"] = [
            {"latitude": latitude, "longitude": longitude}
            for latitude, longitude in FORT_COLLINS_POINTS
        ]
        self.metadata["map_geography"] = json.loads(MAP_GEOGRAPHY.read_text(encoding="utf-8"))
        self.buildings = set(inventory.buildings)
        self.metadata["building_details"] = (
            read_building_details(metadata_path, self.buildings) if metadata_path else {}
        )

        for details in self.metadata["building_details"].values():
            details["map_area_id"] = building_map_area(details, self.metadata["map_geography"])

        # A tiny broadcast join lets Spark aggregate city hours before collecting them.
        locations = self.hourly.sparkSession.createDataFrame([
            (building, self.metadata["building_details"].get(str(building), {}).get("map_area_id") or "")
            for building in sorted(self.buildings)
        ], ["building_id", "map_area_id"])
        self.map_hourly = self.hourly.join(F.broadcast(locations), "building_id", "left")

    def validate_day(self, day):
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError("Enter a date as YYYY-MM-DD.")
        if not self.metadata["first_day"] <= day <= self.metadata["last_day"]:
            raise ValueError("Choose a date inside the simulation period.")

    @lru_cache(maxsize=128)
    def map_day(self, day):
        self.validate_day(day)
        # Collect daily building totals only, rather than the full hourly dataset.
        records = (
            self.hourly.where(F.col("day") == day)
            .groupBy("building_id")
            .agg(*(F.sum(name).alias(name) for name in MEASURES))
            .collect()
        )
        if not records:
            raise ValueError("There are no simulation results for that date.")
        area_hours = (
            self.map_hourly.where(F.col("day") == day)
            .groupBy("map_area_id", "hour_start_utc", "hour_start_mountain")
            .agg(F.count_distinct("building_id").alias("building_count"),
                 *(F.sum(name).alias(name) for name in MEASURES))
            # Format in Spark's UTC session before Python can apply a host-local timezone.
            .withColumn("hour_start_utc", F.date_format("hour_start_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .collect()
        )
        return {"day": day, "total_buildings": len(records), **summarize_map_day(
            records, self.metadata["building_details"]
        ), "hours": summarize_map_hours(area_hours)}

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
        self.validate_day(day)
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


def dashboard_handler(data, page, allocations=None, study=None):
    study = study if study is not None else StudyResults()
    allocations = allocations if allocations is not None else AllocationResults()
    allocation_page = Path(__file__).with_name("allocation_dashboard.html").read_bytes()
    results_page = Path(__file__).with_name("results_dashboard.html").read_bytes()
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
                    case "/allocation" | "/allocation/":
                        self.send_content(HTTPStatus.OK, "text/html; charset=utf-8", allocation_page)
                        return
                    case "/results" | "/results/":
                        self.send_content(HTTPStatus.OK, "text/html; charset=utf-8", results_page)
                        return
                    case "/api/results":
                        payload = study.view()
                    case "/api/results/forecast":
                        payload = study.forecast(query.get("building",[""])[0],query.get("date",[""])[0])
                    case "/api/results/download":
                        filename,body=study.download(query.get("name",[""])[0])
                        self.send_response(HTTPStatus.OK)
                        self.send_header("Content-Type","application/json" if filename.endswith('.json') else "text/csv; charset=utf-8")
                        self.send_header("Content-Disposition",f'attachment; filename="{filename}"')
                        self.send_header("Content-Length",str(len(body)))
                        self.end_headers();self.wfile.write(body)
                        return
                    case "/api/allocation":
                        selected = study.allocation(query["experiment"][0]) if "experiment" in query else allocations
                        payload = selected.view(
                            example=query.get("example", ["0"])[0] == "1",
                            strategy=query.get("strategy", ["prioritized"])[0],
                            seed=query.get("seed", [None])[0],
                            page=int(query.get("page", ["1"])[0]),
                        )
                        if payload["available"]:
                            buildings = {str(building) for building in data.metadata["buildings"]}
                            for home in payload["selection"]["homes"] + payload["ranking"]["homes"]:
                                home["in_simulation"] = home["building_id"] in buildings
                                home["details"] = data.metadata.get("building_details", {}).get(home["building_id"], {})
                    case "/api/metadata":
                        payload = data.metadata
                    case "/api/map-day":
                        payload = data.map_day(query.get("date", [""])[0])
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
    parser.add_argument("--allocation-results", type=Path,
                        help="Saved allocation output directory to show on /allocation")
    parser.add_argument("--study-results",type=Path,default=DEFAULT_RESULTS,help="Generated statistics bundle shown on /results")
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
        with HTTPServer(("127.0.0.1", args.port), dashboard_handler(grid, page, AllocationResults(args.allocation_results), StudyResults(args.study_results))) as server:
            print(f"Grid dashboard ready at http://127.0.0.1:{server.server_port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
