"""Export saved RF/LSTM test results without Spark or TensorFlow.

This is a descriptive comparison; equal counts do not verify paired target keys.
"""
import argparse
import csv
import json
import math
from pathlib import Path
import re


def summarize(repo):
    rows = []
    for label, filename in [("RF demand only", "random_forest_demand_only.txt"),
                            ("RF with temperature", "random_forest_with_temp.txt")]:
        path = repo / "results/random_forest" / filename
        text = path.read_text()
        match = re.search(
            r"=== RANDOM FOREST TEST RESULTS ===\s+MAE:\s+([\d.]+) kWh"
            r"\s+RMSE:\s+([\d.]+) kWh\s+Samples:\s+(\d+)", text)
        if not match:
            raise ValueError(f"Missing RF test results: {path}")
        mae, rmse, count = match.groups()
        rows.append(dict(model=label, mae_kwh=float(mae), rmse_kwh=float(rmse),
                         test_samples=int(count), source=str(path.relative_to(repo))))
    baselines = None
    for label, filename in [("LSTM demand only", "results_demand_only.txt"),
                            ("LSTM with temperature", "results_with_temp.txt")]:
        path = repo / "results/lstm" / filename
        # A repeated run appends a new block; summarize the final completed result.
        payload = path.read_text().rsplit("=== LSTM TEST RESULTS ===", 1)[-1].lstrip()
        result, _ = json.JSONDecoder().raw_decode(payload)
        expected = "lstm_temperature" if "temperature" in label else "lstm"
        if result["model"] != expected:
            raise ValueError(f"Unexpected model identity: {path}")
        count = result["num_predictions"]
        if count <= 0 or count != 24 * result["num_forecasts"]:
            raise ValueError(f"Inconsistent 24-hour forecast counts: {path}")
        for key in ("mae", "rmse", "seasonal_naive_day_mae", "seasonal_naive_day_rmse",
                    "seasonal_naive_week_mae", "seasonal_naive_week_rmse"):
            if not math.isfinite(result[key]) or result[key] < 0:
                raise ValueError(f"Invalid {key}: {path}")
        current = [("Previous day", result["seasonal_naive_day_mae"],
                    result["seasonal_naive_day_rmse"], count),
                   ("Previous week", result["seasonal_naive_week_mae"],
                    result["seasonal_naive_week_rmse"], count)]
        if baselines is not None and current != baselines:
            raise ValueError("LSTM runs disagree on baseline metrics or target counts")
        baselines = current
        rows.append(dict(model=label, mae_kwh=result["mae"], rmse_kwh=result["rmse"],
                         test_samples=count, source=str(path.relative_to(repo))))
    for label, mae, rmse, count in baselines:
        rows.insert(0, dict(model=label, mae_kwh=mae, rmse_kwh=rmse,
                            test_samples=count, source="results/lstm/results_with_temp.txt"))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = summarize(args.repo)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} model summaries to {args.output}")


if __name__ == "__main__":
    main()
