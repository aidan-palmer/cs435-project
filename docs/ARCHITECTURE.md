# System Architecture

## Goal

Build a reproducible pipeline that connects distributed residential-demand preprocessing to forecasting, solar simulation, emissions estimation, and fixed-budget allocation.

## Components

### 1. Data ingestion

**Technology:** PySpark / Spark DataFrames

Responsibilities:

- read raw source files;
- apply geography and building-type filters;
- retain required metadata;
- standardize column names and data types;
- create an auditable data inventory.

Suggested module:

```text
src/data/ingest.py
```

### 2. Data cleaning and time alignment

Responsibilities:

- detect duplicates and missing intervals;
- normalize electricity units;
- align timestamps;
- document daylight-saving and leap-year behavior;
- reject or flag incomplete demand profiles.

Suggested module:

```text
src/data/clean.py
```

### 3. Feature engineering

Responsibilities:

- demand lags;
- rolling historical summaries;
- hour/day/week/season features;
- available weather features;
- selected static building characteristics.

All features must be computable from information available when the forecast is issued.

Suggested module:

```text
src/features/build_features.py
```

### 4. Forecasting

All forecasting methods target the same next-day hourly period.

#### Seasonal naive baseline

Possible initial forms:

- same hour previous day (`t - 24h`);
- same hour previous week (`t - 168h`).

Freeze the exact baseline before model comparison.

#### Random Forest

**Technology:** Spark MLlib

Inputs: engineered tabular features.

Output: hourly demand prediction.

#### LSTM

**Technology:** TensorFlow / Keras

Inputs: historical demand sequences plus any frozen contextual features.

Output: same forecast horizon as baseline and Random Forest.

Suggested layout:

```text
src/models/baseline.py
src/models/random_forest.py
src/models/lstm.py
```

### 5. Solar generation

Generate or load an hourly production profile for a standardized residential PV system.

The simulator must record assumptions including:

- capacity;
- losses;
- tilt;
- azimuth;
- source/API version;
- production unit.

Suggested module:

```text
src/simulation/solar.py
```

### 6. Grid-import simulation

For each simulated building and UTC hour, all energy values are in kWh:

| Output column | Calculation |
|---|---|
| `grid_import_without_solar_kwh` | Hourly demand |
| `self_consumed_solar_kwh` | Smaller of demand and solar generation |
| `grid_import_with_solar_kwh` | Demand minus self-consumed solar |
| `excess_solar_kwh` | Solar generation minus self-consumed solar |
| `grid_reduction_kwh` | Import without solar minus import with solar |

Surplus is recorded in its own column. It represents energy available for export or curtailment under a later policy. The hourly model uses solar within the hour it is generated, with zero export credit and no battery storage. Each row represents an installation scenario for one building. Apply the placement selection before aggregating solar benefit across buildings.

Run the [simulation script](../scripts/simulate_grid_import.py) from the repository root after creating the joined Parquet described in [DATA.md](DATA.md#join-2018-demand-and-rooftop-solar):

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/simulate_grid_import.py \
  data/processed/resstock-pvwatts-2018 \
  data/processed/grid-import-2018
```

The output retains the input building ID, UTC timestamp, and offset-aware Mountain Time label. Choose a new output directory for each run. Run the unit tests with `SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python -m unittest discover -s tests`.

#### Explore the hourly simulation

Run the local dashboard after generating the grid-import Parquet:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/serve_grid_dashboard.py data/processed/grid-import-2018
```

Open `http://127.0.0.1:8765` while the process is running. In an IDE, use the repository root as the working directory, pass `data/processed/grid-import-2018` as the script parameter, and set the same two environment variables. Pass `--port 8766` if the default port is in use.

#### Find a simulated building

The dashboard reads `../DATA_for_project/CO_baseline_metadata_and_annual_results.csv` by default. If you keep the Colorado baseline metadata elsewhere, add `--metadata "path/to/CO_baseline_metadata_and_annual_results.csv"` to the dashboard command or the IDE Parameters field. The [ResStock data instructions](DATA.md#1-resstock-hourly-demand) link to the original CSV. The dashboard matches its baseline `bldg_id` to the simulation's `building_id`. Buildings without a matching metadata record appear by ID.

Each result reads like **Single-Family Detached · 1,698 sq ft · 2000s · Natural Gas · ID 128**. The labels describe simulated dwellings.

| Label | Meaning | Example search |
|---|---|---|
| Building type | Modeled housing structure | `detached` |
| Square footage | Modeled floor area in square feet | `1698` or `1,698` |
| Construction decade | Modeled construction vintage | `2000s` |
| Heating fuel | Modeled heating fuel | `natural gas` |
| ID | ResStock building identifier | `id 128` |

Type one or more terms into **Search buildings**. For example, `detached 2000s natural gas` matches labels containing all those terms. Then choose a result from **Building**. To return to the aggregate view, clear the search and choose **All buildings**.

Set the Mountain Time date and click **Show day**. **Most productive day** jumps to the date with the greatest total solar generation for the selected scope. Ties use the earliest date. The chart maintains the separate repeated hour on the fall clock change.

### 7. Avoided-CO2 estimation

Basic form:

```text
avoided_co2 = grid_reduction_kwh * emissions_factor
```

The emissions-factor source, spatial resolution, temporal resolution, and unit conversions must be explicit.

Suggested module:

```text
src/simulation/emissions.py
```

### 8. Fixed-budget allocation

At minimum compare:

1. random allocation;
2. predicted-benefit prioritization.

Potential score:

```text
predicted_benefit_per_dollar = predicted_avoided_co2 / installation_cost
```

A key evaluation requirement is that households selected using predictions are scored on **held-out realized demand**, not only on their predicted benefit.

Suggested module:

```text
src/allocation/strategies.py
```

### 9. Evaluation and reporting

Forecast metrics:

- MAE;
- RMSE.

Decision metrics:

- grid reduction;
- avoided CO2;
- benefit per dollar;
- allocation regret or difference relative to an oracle strategy, if added.

System metrics:

- preprocessing runtime;
- training runtime;
- inference runtime;
- scaling with data size.

Suggested modules:

```text
src/evaluation/forecast_metrics.py
src/evaluation/allocation_metrics.py
src/evaluation/runtime.py
```

## Leakage Prevention

The pipeline should make chronological ordering a first-class design constraint.

```text
TRAIN -------- VALIDATION -------- TEST
past                                  future
```

Rules:

- choose time boundaries before fitting transformations;
- rolling features must use only prior observations;
- do not calculate normalizers/statistics from future/test data;
- tune models using validation data only;
- perform final decision evaluation on held-out test demand.

## Suggested End-to-End CLI

A future interface could look like:

```bash
python -m src.data.prepare --config configs/data.yaml
python -m src.models.train --model rf --config configs/rf.yaml
python -m src.models.train --model lstm --config configs/lstm.yaml
python -m src.simulation.run --forecast outputs/... --config configs/simulation.yaml
python -m src.allocation.run --config configs/allocation.yaml
python -m src.evaluation.report --run-id <run-id>
```

This is a target structure, not a requirement to implement immediately.
