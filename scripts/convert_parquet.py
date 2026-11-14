import os
import argparse
from pathlib import Path

import pyarrow.parquet as pq


def convert_timestamps_to_microseconds(path):
    """ Spark cannot read parquet files that have nanosecond timestamps. """
    temp_path = path.with_name(path.name + ".tmp")

    try:
        # Read the original Parquet file
        table = pq.read_table(path)

        # Write to a temporary file with microsecond timestamps
        pq.write_table(
            table,
            temp_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )

        # Verify the converted file is readable
        pq.read_metadata(temp_path)

        # Replace the original file
        os.replace(temp_path, path)

        print(f"Converted: {path.name}")

    except Exception as e:
        print(f"Failed: {path.name}: {e}")

        # Remove incomplete temporary file
        if temp_path.exists():
            temp_path.unlink()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "directory",
        help="Directory containing Parquet files"
    )
    args = parser.parse_args()

    directory = Path(args.directory)

    if not directory.is_dir():
        print(f"Invalid directory: {directory}")
        return

    files = sorted(directory.glob("*.parquet"))

    print(f"Found {len(files)} Parquet files")

    for path in files:
        convert_timestamps_to_microseconds(path)

    print("Done!")


if __name__ == "__main__":
    main()
