# LSTM forecasting workflow

## Implemented pipeline

`prepare_lstm_data.py` constructs per-building daily examples: 168 historical demand values, 168 historical temperature values, and 24 future hourly demand targets. Issues occur at hour 23 in the Spark session clock. Output is Parquet partitioned into `split=train`, `split=validation`, and `split=test`.

`train_lstm.py` streams local Parquet batches through PyArrow and TensorFlow. Normalization means and standard deviations are calculated only from training histories; the same demand transform scales targets and is inverted before evaluation in kWh. Historical windows overlap, so the saved normalization describes history slots, not unique building-hours.

The model is LSTM(64), Dropout(0.2), Dense(64, ReLU), Dense(24). Script defaults are Adam at 0.001, MSE loss, batch size 128, at most 30 epochs, and early stopping on validation loss with patience 5 and restoration of the best weights. The random seed is 42. These are code defaults, not a recovered command for the saved runs.

## Prepare and train

Requires PySpark for preparation; TensorFlow, NumPy, and PyArrow for local training. Use the team's environment versions and record them with the run.

```bash
spark-submit scripts/prepare_lstm_data.py \
  hdfs:///solaropt/hourly_features hdfs:///solaropt/lstm_sequences_new_run \
  --val-start 2018-09-15 --test-start 2018-11-08

hdfs dfs -get hdfs:///solaropt/lstm_sequences_new_run ./lstm_sequences_new_run

python scripts/train_lstm.py ./lstm_sequences_new_run outputs/lstm/demand_only_new_run
python scripts/train_lstm.py ./lstm_sequences_new_run outputs/lstm/temperature_new_run \
  --use-temperature
```

Replace the example hourly input with the verified dataset containing `building_id`, `timestamp`, `demand_kwh`, and `temperature_c`. The split dates above match saved RF logs; the original LSTM command and cutoffs were not retained. Confirm the Spark timezone and source interval convention before a new run. Preparation overwrites its destination; use a fresh sequence path. Use a separate output directory per model/run because model, prediction, metric, and history filenames are shared.

## Saved evidence

`results/lstm/results_demand_only.txt` and `results_with_temp.txt` preserve both completed evaluations. Each reports 139,239 training, 30,942 validation, and 30,369 test sequences. Test evaluation covers 728,856 hourly predictions. The training and validation counts differ from RF when multiplied by 24; exact paired keys remain unverified.

| Run | MAE kWh | RMSE kWh |
|---|---:|---:|
| Demand only | 0.4168226162 | 0.8765170023 |
| Historical temperature | 0.4114688823 | 0.8836325630 |

Temperature lowers MAE by 1.28% and raises RMSE by 0.81% versus demand-only LSTM. Against RF with temperature, the temperature LSTM has 3.86% lower MAE and 9.79% lower RMSE, using rounded saved RF metrics. No repeated-run uncertainty or significance claim is supported.

`metrics.json` identifies the temperature run and agrees with its log. `history.json` contains eight epochs of normalized losses/MAE; minimum validation loss occurs at epoch 3. `normalization.json` records the scaling statistics. The shared folder does not retain separate histories/normalizers for both runs, so it cannot support a demand-only learning curve or a controlled attribution to temperature. Training/inference timing, actual CLI settings, environment versions, model weights, and hourly prediction files are not present in this checkout.

The trainer is implemented to save `model.keras`, `history.json`, `normalization.json`, `metrics.json`, and `predictions.parquet`. Prediction columns are building ID, issue timestamp, target timestamp, horizon 1–24, actual kWh, predicted kWh, and model label. Timestamps are saved without timezone metadata; establish their UTC interpretation before joining solar. Outputs are not clipped to zero by the trainer; any downstream physical clipping rule must be explicit.

## Remaining work

Recover the original run commands, split dates, time convention, saved models and per-target predictions. Verify matching RF/LSTM building-hour keys and source aggregation. Preserve separate run folders, configuration, environment versions, training/inference time, and run IDs. Connect model predictions to the common solar/allocation adapter with explicit decision timing; existing allocation benefits remain based on the frozen historical forecast.

## New aligned integration pilot

The original saved evaluations above are preserved. A new ten-building aligned pilot now retains separate trained models, per-target predictions, histories, exact cutoffs, and measured runtime records; see [PRIORITY_EXPERIMENTS.md](PRIORITY_EXPERIMENTS.md). Use `scripts/predict_lstm.py` to export forecasts from a saved model without retraining. The main comparison utility still summarizes the original saved full-result logs, not these new pilot runs.
