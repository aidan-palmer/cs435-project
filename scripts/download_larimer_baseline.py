"""Download individual ResStock baseline profiles listed in Colorado metadata."""

import argparse
import csv
from contextlib import contextmanager
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit


def is_parquet(path):
    if not path.is_file() or path.stat().st_size < 12:
        return False
    with path.open("rb") as file:
        header = file.read(4)
        file.seek(-4, 2)
        return header == file.read(4) == b"PAR1"


def download(url, destination):
    temporary = destination.with_suffix(".parquet.part")
    try:
        subprocess.run(
            ["/usr/bin/curl", "--fail", "--silent", "--show-error", "--location",
             "--retry", "2", "--connect-timeout", "15", "--max-time", "120",
             "--output", str(temporary), url],
            check=True,
        )
        if not is_parquet(temporary):
            raise ValueError("Response did not have a Parquet header and footer")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def metadata_rows(path):
    if path.suffix.lower() == ".xlsx":
        try:
            from openpyxl import load_workbook
        except ImportError as error:
            raise SystemExit("Excel input requires openpyxl.") from error
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            rows = workbook.worksheets[0].values
            columns = next(rows, ())
            yield columns, (
                {key: "" if value is None else str(value) for key, value in zip(columns, row)}
                for row in rows
            )
        finally:
            workbook.close()
    else:
        with path.open(encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            yield reader.fieldnames or (), reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metadata", type=Path, help="Colorado baseline metadata CSV or XLSX")
    parser.add_argument("output", type=Path, help="Folder for downloaded Parquet files")
    parser.add_argument("--sample-url", required=True,
                        help="Working HTTPS download URL for a Colorado baseline profile from the same release as the CSV")
    parser.add_argument("--detached-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    source = urlsplit(args.sample_url)
    if source.scheme != "https" or not source.netloc or not source.path.endswith("-0.parquet") or source.query or source.fragment:
        parser.error("--sample-url must be a direct HTTPS baseline URL ending in -0.parquet")
    base_url = args.sample_url.rsplit("/", 1)[0]

    with metadata_rows(args.metadata) as (columns, reader):
        required = {"bldg_id", "upgrade", "in.state", "in.county"}
        if args.detached_only:
            required.add("in.geometry_building_type_acs")
        missing = required - set(columns)
        if missing:
            parser.error("Missing metadata columns: " + ", ".join(sorted(missing)))
        ids = set()
        for row in reader:
            if (row["upgrade"], row["in.state"], row["in.county"]) != ("0", "CO", "G0800690"):
                continue
            if args.detached_only and row["in.geometry_building_type_acs"] != "Single-Family Detached":
                continue
            building_id = row["bldg_id"]
            if not building_id or not building_id.isascii() or not building_id.isdecimal():
                parser.error(f"Invalid building ID: {building_id!r}")
            ids.add(building_id)

    if not ids:
        parser.error("No matching Larimer County baseline buildings found")
    print(f"{len(ids)} matching profiles from {base_url}")
    if args.dry_run:
        return 0
    args.output.mkdir(parents=True, exist_ok=True)
    downloaded = skipped = 0
    failures = []
    for building_id in sorted(ids, key=int):
        filename = f"{building_id}-0.parquet"
        destination = args.output / filename
        if is_parquet(destination):
            skipped += 1
            continue
        try:
            download(f"{base_url}/{filename}", destination)
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            failures.append(building_id)
            print(f"{filename}: {error}", file=sys.stderr)
        else:
            downloaded += 1
            print(f"Downloaded {filename}", flush=True)
    print(f"Downloaded: {downloaded}. Already present: {skipped}. Failed: {len(failures)}.")
    if failures:
        print("Failed IDs: " + ", ".join(failures), file=sys.stderr)
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(main())
