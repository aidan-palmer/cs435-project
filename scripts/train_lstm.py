
import argparse
import json
from pathlib import Path
import math
import time

if __package__:
    from .runtime_evidence import save_runtime
else:
    from runtime_evidence import save_runtime

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import tensorflow as tf


INPUT_HOURS = 168
OUTPUT_HOURS = 24


def read_records(directory, include_metadata=False):
    """Stream prepared Parquet records without loading all data."""
    columns = [
        "demand_history",
        "temperature_history",
        "targets"
    ]

    if include_metadata:
        columns += ["building_id", "forecast_issued_at"]

    dataset = ds.dataset(str(directory), format="parquet")

    for batch in dataset.to_batches(
        columns=columns,
        batch_size=256
    ):
        for row in batch.to_pylist():
            yield row


def calculate_stats(train_dir):
    """Compute normalization statistics from training history."""

    demand_sum = 0.0
    demand_sq_sum = 0.0
    temp_sum = 0.0
    temp_sq_sum = 0.0
    count = 0

    for row in read_records(train_dir):
        demand = np.asarray(
            row["demand_history"], dtype=np.float64
        )
        temp = np.asarray(
            row["temperature_history"], dtype=np.float64
        )

        demand_sum += demand.sum()
        demand_sq_sum += np.square(demand).sum()
        temp_sum += temp.sum()
        temp_sq_sum += np.square(temp).sum()
        count += len(demand)

    if count == 0:
        raise ValueError("Training dataset is empty")

    d_mean = demand_sum / count
    t_mean = temp_sum / count

    d_std = np.sqrt(
        max(demand_sq_sum / count - d_mean ** 2, 1e-12)
    )
    t_std = np.sqrt(
        max(temp_sq_sum / count - t_mean ** 2, 1e-12)
    )

    return {
        "demand_mean": float(d_mean),
        "demand_std": float(d_std),
        "temperature_mean": float(t_mean),
        "temperature_std": float(t_std)
    }


def encode_record(row, stats, use_temperature):
    demand = np.asarray(
        row["demand_history"], dtype=np.float32
    )
    temperature = np.asarray(
        row["temperature_history"], dtype=np.float32
    )
    targets = np.asarray(
        row["targets"], dtype=np.float32
    )

    if (
        demand.shape != (INPUT_HOURS,)
        or temperature.shape != (INPUT_HOURS,)
        or targets.shape != (OUTPUT_HOURS,)
    ):
        raise ValueError("Unexpected LSTM sequence length")

    demand_scaled = (
        demand - stats["demand_mean"]
    ) / stats["demand_std"]

    if use_temperature:
        temp_scaled = (
            temperature - stats["temperature_mean"]
        ) / stats["temperature_std"]

        x = np.stack(
            [demand_scaled, temp_scaled],
            axis=-1
        )
    else:
        x = demand_scaled[:, np.newaxis]

    y = (
        targets - stats["demand_mean"]
    ) / stats["demand_std"]

    return x.astype(np.float32), y.astype(np.float32)


def count_sequences(directory):
    dataset = ds.dataset(
        str(directory),
        format="parquet"
    )
    return dataset.count_rows()



def make_dataset(
    directory,
    stats,
    use_temperature,
    batch_size,
    shuffle=False,
    repeat=False
):
    n_features = 2 if use_temperature else 1

    def generator():
        for row in read_records(directory):
            yield encode_record(
                row, stats, use_temperature
            )

    dataset = tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            tf.TensorSpec(
                shape=(INPUT_HOURS, n_features),
                dtype=tf.float32
            ),
            tf.TensorSpec(
                shape=(OUTPUT_HOURS,),
                dtype=tf.float32
            )
        )
    )

    if shuffle:
        dataset = dataset.shuffle(
            buffer_size=2048,
            reshuffle_each_iteration=True
        )

    dataset = dataset.batch(batch_size)

    # Repeat complete batches across epochs.
    if repeat:
        dataset = dataset.repeat()

    return dataset.prefetch(tf.data.AUTOTUNE)



def build_model(n_features, learning_rate):
    model = tf.keras.Sequential([
        tf.keras.Input(
            shape=(INPUT_HOURS, n_features)
        ),
        tf.keras.layers.LSTM(64),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(
            64, activation="relu"
        ),
        tf.keras.layers.Dense(OUTPUT_HOURS)
    ])

    model.compile(
        optimizer=tf.keras.optimizers.Adam(
            learning_rate=learning_rate
        ),
        loss="mse",
        metrics=["mae"]
    )

    return model


def evaluate_and_save(
    model,
    test_dir,
    stats,
    use_temperature,
    output_dir,
    batch_size,
    filename="predictions.parquet"
):
    """Evaluate in kWh and write hourly predictions to Parquet."""

    output_file = output_dir / filename

    schema = pa.schema([
        ("building_id", pa.string()),
        ("forecast_issued_at", pa.timestamp("us")),
        ("target_timestamp", pa.timestamp("us")),
        ("forecast_available_at", pa.timestamp("us")),
        ("horizon", pa.int32()),
        ("actual_kwh", pa.float32()),
        ("predicted_kwh", pa.float32()),
        ("model", pa.string())
    ])

    writer = pq.ParquetWriter(
        str(output_file), schema=schema
    )

    abs_sum = 0.0
    sq_sum = 0.0
    day_abs_sum = 0.0
    day_sq_sum = 0.0
    week_abs_sum = 0.0
    week_sq_sum = 0.0
    count = 0
    num_forecasts = 0
    prediction_model_calls_seconds = 0.0

    # Accumulate one manageable batch of complete forecasts.
    batch_records = []

    def process_batch(records):
        nonlocal abs_sum, sq_sum
        nonlocal day_abs_sum, day_sq_sum
        nonlocal week_abs_sum, week_sq_sum
        nonlocal count, num_forecasts, prediction_model_calls_seconds

        encoded = [
            encode_record(r, stats, use_temperature)[0]
            for r in records
        ]

        X = np.stack(encoded, axis=0)

        # Model outputs normalized electricity demand.
        model_started=time.perf_counter()
        pred_scaled = np.asarray(model.predict_on_batch(X))
        prediction_model_calls_seconds += time.perf_counter()-model_started

        predicted = (
            pred_scaled * stats["demand_std"]
            + stats["demand_mean"]
        )

        output_rows = []

        for i, row in enumerate(records):
            actual = np.asarray(
                row["targets"], dtype=np.float64
            )
            forecast = np.asarray(
                predicted[i], dtype=np.float64
            )

            history = np.asarray(
                row["demand_history"], dtype=np.float64
            )

            # Seasonal naive baselines on the identical test rows.
            naive_day = history[-24:]
            naive_week = history[:24]

            error = actual - forecast
            day_error = actual - naive_day
            week_error = actual - naive_week

            abs_sum += np.abs(error).sum()
            sq_sum += np.square(error).sum()

            day_abs_sum += np.abs(day_error).sum()
            day_sq_sum += np.square(day_error).sum()

            week_abs_sum += np.abs(week_error).sum()
            week_sq_sum += np.square(week_error).sum()

            count += OUTPUT_HOURS
            num_forecasts += 1

            issued_at = row["forecast_issued_at"]

            from datetime import timedelta

            for h in range(OUTPUT_HOURS):
                output_rows.append({
                    "building_id": row["building_id"],
                    "forecast_issued_at": issued_at,
                    "target_timestamp": (
                        issued_at + timedelta(hours=h + 1)
                    ),
                    "forecast_available_at": issued_at + timedelta(hours=1),
                    "horizon": h + 1,
                    "actual_kwh": float(actual[h]),
                    "predicted_kwh": float(forecast[h]),
                    "model": (
                        "lstm_temperature"
                        if use_temperature else "lstm"
                    )
                })

        writer.write_table(
            pa.Table.from_pylist(output_rows, schema=schema)
        )

    try:
        for row in read_records(
            test_dir, include_metadata=True
        ):
            batch_records.append(row)

            if len(batch_records) >= batch_size:
                process_batch(batch_records)
                batch_records.clear()

        if batch_records:
            process_batch(batch_records)

    finally:
        writer.close()

    if count == 0:
        raise ValueError("Test dataset is empty")

    results = {
        "model": (
            "lstm_temperature" if use_temperature else "lstm"
        ),
        "mae": abs_sum / count,
        "rmse": np.sqrt(sq_sum / count),
        "num_predictions": count,
        "num_forecasts": num_forecasts,
        "prediction_model_calls_seconds": prediction_model_calls_seconds,
        "seasonal_naive_day_mae": day_abs_sum / count,
        "seasonal_naive_day_rmse": np.sqrt(
            day_sq_sum / count
        ),
        "seasonal_naive_week_mae": week_abs_sum / count,
        "seasonal_naive_week_rmse": np.sqrt(
            week_sq_sum / count
        )
    }
    return results

def printf(filename: str, message: str):
    with open(filename, 'a') as f:
        f.write(message + '\n')
    

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("input", help="Local sequence directory")
    parser.add_argument("output", help="Local model output directory")

    parser.add_argument(
        "--use-temperature",
        action="store_true",
        help="Include historical temperature in LSTM inputs"
    )

    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument(
        "--learning-rate", type=float, default=0.001
    )
    parser.add_argument(
        "--patience", type=int, default=5
    )

    parser.add_argument('--runtime-output', help='New local runtime/configuration JSON')
    args = parser.parse_args()
    if min(args.epochs,args.batch_size,args.patience)<1 or args.learning_rate<=0:
        parser.error('Epochs, batch size, patience, and learning rate must be positive')
    processing_start=time.perf_counter()
    phases={}

    tf.keras.utils.set_random_seed(42)

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=False)

    train_dir = input_dir / "split=train"
    val_dir = input_dir / "split=validation"
    test_dir = input_dir / "split=test"

    # Normalize using training observations only.
    stats = calculate_stats(train_dir)

    with open(output_dir / "normalization.json", "w") as f:
        json.dump(stats, f, indent=2)

    # Count examples in each partition.
    n_train = count_sequences(train_dir)
    n_val = count_sequences(val_dir)
    n_test = count_sequences(test_dir)
    
    log_file = (
        output_dir / "results_with_temp.txt" 
        if args.use_temperature
        else output_dir / "results_demand_only.txt"
    )

    printf(log_file, f"Training sequences: {n_train}")
    printf(log_file, f"Validation sequences: {n_val}")
    printf(log_file, f"Testing sequences: {n_test}")

    if min(n_train, n_val, n_test) == 0:
        raise ValueError(
            "One or more LSTM dataset partitions are empty"
        )

    # Number of batches per epoch.
    train_steps = math.ceil(n_train / args.batch_size)
    val_steps = math.ceil(n_val / args.batch_size)

    printf(log_file, f"Training steps per epoch: {train_steps}")
    printf(log_file, f"Validation steps: {val_steps}")

    train_ds = make_dataset(
        train_dir,
        stats,
        args.use_temperature,
        args.batch_size,
        shuffle=True,
        repeat=True
    )

    val_ds = make_dataset(
        val_dir,
        stats,
        args.use_temperature,
        args.batch_size,
        shuffle=False,
        repeat=True
    )

    n_features = 2 if args.use_temperature else 1

    model = build_model(
        n_features,
        args.learning_rate
    )

    model.summary()

    callbacks = [
        tf.keras.callbacks.EarlyStopping(
            monitor="val_loss",
            patience=args.patience,
            restore_best_weights=True
        )
    ]

    phases['processing_seconds']=time.perf_counter()-processing_start
    training_start=time.perf_counter()
    # Train on chronological train data.
    # Validate only on the held-out validation dates.
    history = model.fit(
        train_ds,
        validation_data=val_ds,
        epochs=args.epochs,
        steps_per_epoch=train_steps,
        validation_steps=val_steps,
        shuffle=False,
        callbacks=callbacks
    )

    phases['training_seconds']=time.perf_counter()-training_start
    model.save(str(output_dir / 'model.keras'))

    with open(output_dir / "history.json", "w") as f:
        json.dump(
            {
                key: [float(v) for v in values]
                for key, values in history.history.items()
            },
            f,
            indent=2
        )

    # Evaluate on the untouched test period.
    inference_start=time.perf_counter()
    metrics = evaluate_and_save(
        model,
        test_dir,
        stats,
        args.use_temperature,
        output_dir,
        args.batch_size
    )

    phases['prediction_metrics_and_export_seconds']=time.perf_counter()-inference_start
    phases['prediction_model_calls_seconds']=metrics['prediction_model_calls_seconds']
    validation_started=time.perf_counter()
    validation_metrics=evaluate_and_save(model,val_dir,stats,args.use_temperature,output_dir,args.batch_size,filename='validation_predictions.parquet')
    (output_dir/'validation_metrics.json').write_text(json.dumps(validation_metrics,indent=2))
    phases['validation_prediction_export_seconds']=time.perf_counter()-validation_started
    save_runtime(args.runtime_output or output_dir/'runtime.json',phases,
        dict(input=args.input,output=args.output,use_temperature=args.use_temperature,
             epochs_requested=args.epochs,epochs_completed=len(history.history['loss']),
             batch_size=args.batch_size,learning_rate=args.learning_rate,patience=args.patience,
             seed=42,timestamp_clock='UTC',train_sequences=n_train,validation_sequences=n_val,
             test_sequences=n_test,tensorflow=tf.__version__,
             sequence_metadata=json.loads((input_dir/'_sequence_metadata.json').read_text())
                 if (input_dir/'_sequence_metadata.json').exists() else None))
    with open(output_dir / 'metrics.json', 'w') as f:
        json.dump(
            {
                key: (
                    float(value)
                    if isinstance(value, np.generic)
                    else value
                )
                for key, value in metrics.items()
            },
            f,
            indent=2
        )

    printf(log_file, "\n=== LSTM TEST RESULTS ===")
    printf(log_file, json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
