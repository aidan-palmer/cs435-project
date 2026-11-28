# Aligned forecasting and allocation experiments

## 1. Common hourly preparation

`hourly_alignment.py` is shared by forecasting preparation and the ResStock pilot. A 15-minute reading stamped 01:00 is an interval end: subtract 15 minutes before hourly grouping. Reject incomplete/duplicate quarter-hour bins and invalid energy or temperature. Preserve fixed EST source labels, add five hours for UTC, and restore the two wrapped Colorado-year-end hours. The forecast builder validates all 8,760 hours per building and exports UTC `timestamp`, demand, and temperature.

The new aligned dataset covers all 573 buildings / 5,019,480 hours. Its UTC keys and demand values match the existing solar simulation exactly (maximum difference 0 kWh). On raw profile 128, the old direct floor yields 8,761 bins, with three readings in the first and one in the last; corrected bins all have four. The executed full preparation took 22.61 seconds on Spark local[2] with two execution threads. This excludes Spark startup and is a single-run measurement.

```bash
python scripts/build_model_features.py ../DATA_for_project/larimer_baseline \
  data/processed/aligned-hourly-2018-new \
  --runtime-output results/performance/aligned-hourly-new.json

python scripts/audit_time_alignment.py \
  --aligned data/processed/aligned-hourly-2018-new \
  --joined data/processed/resstock-pvwatts-2018 \
  --raw-example ../DATA_for_project/larimer_baseline/128-0.parquet \
  --output results/performance/time-alignment-new.json
```

Use Java 17 and a UTC Spark session. Existing legacy model logs remain historical evidence; their original input provenance is not established by this correction. Retraining on the aligned dataset creates a separate run.

## 2. Export and connect model forecasts

The teammate's `train_lstm.py` already forecasts and exports `predictions.parquet`. `train_random_forest.py` exports a `predictions/` Parquet folder. `allocation_experiments.py` accepts both formats, verifies exact building/hour coverage and actual demand against the aligned solar dataset, and adds previous-day/week baselines on those same targets. Missing, duplicated, shifted, or conflicting records cause a failure rather than a silent inner join.

Both trainers label `forecast_issued_at` with the **start of the last observed hour**. Its demand is available one hour later; new exports also include `forecast_available_at`. Horizon 1 targets the hour that starts at that availability instant. Do not interpret the issue label as an earlier decision instant.

```bash
python scripts/allocation_experiments.py \
  data/processed/resstock-pvwatts-2018 outputs/model-allocation-new \
  --predictions rf=outputs/rf-new/predictions lstm=outputs/lstm-new/predictions.parquet \
  --prediction-clock UTC --retrospective-ranking \
  --evaluation-start 2018-11-08T00:00:00Z --evaluation-end 2019-01-01T00:00:00Z \
  --metadata ../DATA_for_project/CO_baseline_metadata_and_annual_results.csv \
  --budgets-usd 54000 --sensitivity
```

This command explicitly evaluates **retrospective ranking from daily forecasts**. It does not show a one-time installation decision made before the test period: later-issued forecasts were unavailable then. For a prospective policy use `--decision-time-utc` instead; the adapter rejects any unavailable forecast. A valid prospective multi-month study needs forecasts for the entire decision horizon available at that cutoff. Neither model's test actuals may be used for selection.

The adapter requires full coverage for every building in the supplied joined dataset. A pilot must use its explicitly saved cohort subset as the joined input. `--prediction-clock resstock-est-label` can convert legacy timestamp labels, but the actual-demand match still rejects old aggregation errors. Do not relabel legacy timestamps as UTC without verification.

Negative model demand is kept in accuracy calculations and clipped to zero only for physical solar-benefit scoring. Solar benefit is `sum(min(max(prediction, 0), scaled_solar))`; realized scoring substitutes held-out demand.

## 3. Matched RF/LSTM runs

Use separate new output directories. Install `requirements-forecasting.txt` in a Python 3.12 environment; Java 17 is needed for Spark. The pinned TensorFlow/PyArrow versions avoid the Mac ARM import conflict encountered with TensorFlow 2.20 / PyArrow 21. Compatibility reference: https://github.com/tensorflow/tensorflow/issues/99464 .

```bash
python scripts/train_random_forest.py data/processed/aligned-hourly-2018-new outputs/rf-new \
  --val-start 2018-09-15 --test-start 2018-11-08 \
  --runtime-output results/performance/rf-new.json

python scripts/prepare_lstm_data.py data/processed/aligned-hourly-2018-new \
  data/processed/lstm-sequences-new --val-start 2018-09-15 --test-start 2018-11-08

python scripts/train_lstm.py data/processed/lstm-sequences-new outputs/lstm-new \
  --runtime-output results/performance/lstm-new.json
```

Add `--use-temperature` to each training command for the historical-temperature variants and choose new output paths. Spark sequence preparation now saves `_sequence_metadata.json` with exact cutoffs and input clock; future LSTM runtime records include it. Existing saved aggregate metrics alone cannot establish paired target alignment.

## 4. Sensitivity scenarios

The executed Q4 sensitivity run is under `outputs/priority-experiments/historical-sensitivity-v1/`, with a fixed $54,000 budget, 382 eligible profiles, and 100 random seeds per case. It uses the historical baseline so its results do not imply an RF/LSTM advantage. Scenarios vary one assumption at a time: cost $8,640 / $10,800 / $12,960; solar 2 / 4 / 6 / 8 kW; constant emissions 0.55976 / 0.6997 / 0.83964 kg/kWh.

Solar capacity actually rescales the hourly profile **before** self-consumption scoring, so overlap and rankings can change. This is a linear fixed-shape sensitivity, not a new PVWatts physical simulation of inverter clipping. PV cases hold installation cost fixed to isolate capacity. Cost cases keep the budget fixed so affordable installation counts change. Uniform positive emissions scaling changes totals but should not change ranking or percentage gain over random. These are illustrative assumptions, not current local quotes or measured hourly marginal emissions.

```bash
python scripts/allocation_experiments.py data/processed/resstock-pvwatts-2018 \
  outputs/historical-sensitivity-new --historical-baseline \
  --evaluation-start 2018-10-01T06:00:00Z --evaluation-end 2019-01-01T07:00:00Z \
  --metadata ../DATA_for_project/CO_baseline_metadata_and_annual_results.csv \
  --budgets-usd 54000 --sensitivity
```

## 5. Measured phases and remaining evidence

The forecast builder records materialized preparation time. RF records materialized preprocessing, each training configuration, total tuning plus validation, selected training time, materialized prediction plus test metrics, and prediction export. LSTM records preprocessing, fit, and combined prediction/metrics/Parquet export. The latter is not pure inference time; do not compare unlike phase totals as a speed contest. Runtime records include dataset counts, settings, software/platform details, commit, and (for new records) script hashes.

The deterministic 10-building pilot selects the smallest numeric IDs among detached profiles. It verifies integration and measures local runtime; it is not a representative performance estimate or replacement for the existing 573-profile experiments. For controlled scaling, repeat fixed settings on nested 50 / 150 / 300 / 573 cohorts, record partition counts and resource limits, and report medians and spread. Local Spark results do not establish multi-machine speedup.

## Executed pilot outputs

`outputs/priority-experiments/matched-pilot-v1/` contains six models on 12,960 verified identical targets: RF MAE/RMSE 0.4939/1.1655, RF temperature 0.4487/1.0140, LSTM 0.4762/0.9905, LSTM temperature 0.5229/1.1725, previous-day 0.5329/1.2366, previous-week 0.5822/1.3689 kWh. There are 48 model/scenario combinations with 100 random controls each.

At the base $54,000 budget, all four trained models select five homes yielding 1,550.91 kg modeled realized benefit; the seasonal baselines yield 1,558.92 kg, compared with random mean 1,256.13 kg. These results do not establish model superiority across the wider cohort.

`predict_lstm.py` also supports recovering a prediction export from saved weights without fitting again:

```bash
python scripts/predict_lstm.py data/processed/lstm-sequences-new outputs/lstm-new \
  outputs/lstm-predictions-recovered-new
```

The executed recovery reproduces all 12,960 demand-only pilot predictions. The new pilot retains model files and per-run histories; the original large-run weights and prediction files are still absent locally.

## Showing the evidence in the web application

The local dashboard now includes `/results`, linked from Simulation and Allocation results. It separates the matched ten-home pilot from the original aggregate logs, shows MAE/RMSE comparisons, lets viewers select a pilot household/date/model to inspect hourly demand against forecasts and solar, and exposes all 56 saved allocation scenarios. Each scenario links to the existing selected-household/ranking view. The page also includes measured phase runtimes and a proposal-completion checklist, plus CSV downloads.

Generate or refresh the snapshot in the compatible forecasting environment, then run the existing Spark dashboard:

```bash
python scripts/build_results_dashboard.py
python scripts/serve_grid_dashboard.py data/processed/grid-import-2018 \
  --allocation-results data/processed/allocation-ready/budget-15000
```

Open `http://127.0.0.1:8765/results` with the server's default port. The current preview uses port 8766. `--study-results` selects an alternate snapshot directory. The generator reads the explicitly named completed runs in `outputs/priority-experiments/`; to present a new experiment cohort, update those inputs and its scope/period labels together. Generated dashboard JSON/CSV is ignored by Git; reproducible source code is retained. Source hashes are saved with the bundle. Model training and experiment computation do not run when a visitor changes a selector.

Against proposal pages 2–4, the remaining gaps are the full-cohort matched model/placement experiment, a prospective decision protocol if that claim is made, comparable repeated scaling runs (including honest local/cluster scope), and either a housing-feature ablation or an explicit explanation of omitted housing inputs. The new hourly browser supplies the actual-versus-forecast and demand/solar overlap figures promised on page 4. Before submission, freeze actual source choices and feature definitions, document changes from planned datasets/geography, and verify a clean reproduction of the final outputs. Batteries, detailed roofs, and advanced allocation optimizers remain secondary under proposal page 6.
