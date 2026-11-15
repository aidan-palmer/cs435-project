"""Average four Fort Collins rooftop PVWatts exports on Colorado's clock."""

import argparse
import csv
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import fmean
from zoneinfo import ZoneInfo


MOUNTAIN_STANDARD = timezone(timedelta(hours=-7))
COLORADO = ZoneInfo("America/Denver")


def read_profile(path, hours):
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        location = None
        settings = []
        for row in reader:
            if row[:3] == ["Month", "Day", "Hour"]:
                break
            if row[:1] == ["Solar Resource Data Location"]:
                location = row[1].strip()
            if settings or row[:1] == ["DC System Size (kW)"]:
                settings.append(tuple(cell.strip() for cell in row))
        else:
            raise ValueError(f"{path}: missing hourly table header")

        if not location or ("Array Type", "Fixed (roof mount)") not in settings:
            raise ValueError(f"{path}: expected a rooftop export with a weather location")
        if ("DC System Size (kW)", "4") not in settings:
            raise ValueError(f"{path}: expected a 4 kW system")
        ac_column = row.index("AC System Output (W)")

        power = []
        for hour, record in zip(hours, reader, strict=True):
            if tuple(map(int, record[:3])) != (hour.month, hour.day, hour.hour):
                raise ValueError(f"{path}: expected a record for {hour:%m-%d %H:00}")
            watts = float(record[ac_column])
            if not math.isfinite(watts) or watts < 0:
                raise ValueError(f"{path}: invalid AC power for {hour:%m-%d %H:00}")
            power.append(watts)

    return location, settings, power


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("exports", nargs=4, type=Path, help="Four hourly rooftop CSVs")
    parser.add_argument("--output", required=True, type=Path, help="Path for the new CSV")
    args = parser.parse_args()

    # Place the typical-year profile on the ResStock pilot calendar.
    start = datetime(2018, 1, 1, tzinfo=MOUNTAIN_STANDARD)
    hours = [start + timedelta(hours=index) for index in range(8760)]

    try:
        profiles = [read_profile(path, hours) for path in args.exports]
        if len({location for location, _, _ in profiles}) != len(profiles):
            raise ValueError("The four exports must use distinct weather locations")
        if any(settings != profiles[0][1] for _, settings, _ in profiles):
            raise ValueError("The PV system settings must match across the four exports")
    except (OSError, ValueError, IndexError) as error:
        parser.error(str(error))

    power_by_location = [power for _, _, power in profiles]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow([
            "hour_start_mountain",
            "hour_start_mst",
            "hour_start_utc",
            "solar_generation_kwh",
        ])
        for hour, watts in zip(hours, zip(*power_by_location, strict=True), strict=True):
            writer.writerow([
                hour.astimezone(COLORADO).isoformat(),
                hour.isoformat(),
                hour.astimezone(timezone.utc).isoformat(),
                f"{fmean(watts) / 1000:.9f}",
            ])


if __name__ == "__main__":
    main()
